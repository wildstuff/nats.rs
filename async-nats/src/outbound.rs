// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The outbound step: one process-global filter every message passes on
//! its way out of this client.
//!
//! The client calls [`outbound`] at every site that hands a message to the
//! connection task (each publish, each request, a JetStream publish),
//! before the size check, and checks the size again on what the filter
//! answered. It runs on the caller's task, never on the connection task,
//! so a slow filter delays one publisher and never the connection.
//!
//! The filter holds every rule; this module holds none. It is set once per
//! process with [`set_outbound_filter`]. Unset, every message goes out
//! exactly as given.

use std::sync::OnceLock;

use bytes::Bytes;

use crate::header::HeaderMap;
use crate::subject::Subject;

/// What a filter answers: the headers and the body to send in place of the
/// ones it was given.
pub type Outgoing = (Option<HeaderMap>, Bytes);

/// The filter: given the subject, headers and body of an outgoing message,
/// answers the headers and body to send.
///
/// It runs synchronously. A body of [`BLOCKING_THRESHOLD`] bytes or more is
/// passed to it on Tokio's blocking pool when a runtime is current.
pub type OutboundFilter = dyn Fn(&str, Option<HeaderMap>, Bytes) -> Outgoing + Send + Sync;

/// A body this size or larger is filtered on the blocking pool when a
/// runtime is current, so a long scan does not stall an async worker.
pub const BLOCKING_THRESHOLD: usize = 64 * 1024;

static FILTER: OnceLock<Box<OutboundFilter>> = OnceLock::new();

/// Returned by [`set_outbound_filter`] when a filter is already set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundFilterAlreadySet;

impl std::fmt::Display for OutboundFilterAlreadySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the outbound filter is already set")
    }
}

impl std::error::Error for OutboundFilterAlreadySet {}

/// Sets the process-global outbound filter. It can be set once; a second
/// call leaves the first filter in place and returns an error.
///
/// Every client in the process passes every message it sends through the
/// filter from then on.
///
/// # Examples
/// ```
/// // Set once, early in `main`, before the first client sends.
/// async_nats::outbound::set_outbound_filter(|_subject, headers, payload| {
///     // Answer what to send: here, the message as given.
///     (headers, payload)
/// })
/// .expect("set once");
/// assert!(async_nats::outbound::outbound_filter_is_set());
/// ```
pub fn set_outbound_filter<F>(filter: F) -> Result<(), OutboundFilterAlreadySet>
where
    F: Fn(&str, Option<HeaderMap>, Bytes) -> Outgoing + Send + Sync + 'static,
{
    FILTER
        .set(Box::new(filter))
        .map_err(|_| OutboundFilterAlreadySet)
}

/// Whether an outbound filter is set in this process.
pub fn outbound_filter_is_set() -> bool {
    FILTER.get().is_some()
}

/// The outbound step, for an async send site. Unset: the message as given.
pub(crate) async fn outbound(
    subject: &Subject,
    headers: Option<HeaderMap>,
    payload: Bytes,
) -> Result<Outgoing, tokio::task::JoinError> {
    outbound_with(FILTER.get().map(|f| &**f), subject, headers, payload).await
}

/// The outbound step, for a send site that cannot await (`Sink`): always
/// inline.
pub(crate) fn outbound_sync(
    subject: &Subject,
    headers: Option<HeaderMap>,
    payload: Bytes,
) -> Outgoing {
    match FILTER.get() {
        None => (headers, payload),
        Some(filter) => filter(subject.as_str(), headers, payload),
    }
}

pub(crate) async fn outbound_with(
    filter: Option<&'static OutboundFilter>,
    subject: &Subject,
    headers: Option<HeaderMap>,
    payload: Bytes,
) -> Result<Outgoing, tokio::task::JoinError> {
    let Some(filter) = filter else {
        return Ok((headers, payload));
    };
    if payload.len() >= BLOCKING_THRESHOLD {
        // No runtime: scanned inline. `block_in_place` is never used — it
        // panics on a current-thread runtime.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let subject = subject.clone();
            return handle
                .spawn_blocking(move || filter(subject.as_str(), headers, payload))
                .await;
        }
    }
    Ok(filter(subject.as_str(), headers, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    fn leak(
        f: impl Fn(&str, Option<HeaderMap>, Bytes) -> Outgoing + Send + Sync + 'static,
    ) -> &'static OutboundFilter {
        Box::leak(Box::new(f))
    }

    /// Answers the id of the thread it ran on, as the body.
    fn thread_reporter() -> &'static OutboundFilter {
        leak(|_, headers, _| {
            (
                headers,
                Bytes::from(format!("{:?}", std::thread::current().id())),
            )
        })
    }

    fn here() -> String {
        format!("{:?}", std::thread::current().id())
    }

    #[test]
    fn unset_returns_the_message_unchanged() {
        let subject = Subject::from_static("a.b");
        let payload = Bytes::from_static(b"body");
        let (headers, body) = outbound_with(None, &subject, None, payload.clone())
            .now_or_never()
            .expect("no await point when unset")
            .unwrap();
        assert!(headers.is_none());
        assert_eq!(body, payload);
    }

    #[test]
    fn large_body_without_a_runtime_is_filtered_inline() {
        let subject = Subject::from_static("a.b");
        let payload = Bytes::from(vec![b'x'; BLOCKING_THRESHOLD]);
        let (_, body) = outbound_with(Some(thread_reporter()), &subject, None, payload)
            .now_or_never()
            .expect("inline without a runtime")
            .unwrap();
        assert_eq!(body, here());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn small_body_on_a_runtime_is_filtered_inline() {
        let subject = Subject::from_static("a.b");
        let payload = Bytes::from(vec![b'x'; BLOCKING_THRESHOLD - 1]);
        let (_, body) = outbound_with(Some(thread_reporter()), &subject, None, payload)
            .await
            .unwrap();
        assert_eq!(body, here());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn large_body_on_a_runtime_goes_to_the_blocking_pool() {
        let subject = Subject::from_static("a.b");
        let payload = Bytes::from(vec![b'x'; BLOCKING_THRESHOLD]);
        let (_, body) = outbound_with(Some(thread_reporter()), &subject, None, payload)
            .await
            .unwrap();
        assert_ne!(body, here());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_filter_that_panics_on_the_blocking_pool_is_an_error() {
        let subject = Subject::from_static("a.b");
        let filter = leak(|_, _, _| panic!("filter failed"));
        let payload = Bytes::from(vec![b'x'; BLOCKING_THRESHOLD]);
        let err = outbound_with(Some(filter), &subject, None, payload)
            .await
            .unwrap_err();
        assert!(err.is_panic());
    }
}
