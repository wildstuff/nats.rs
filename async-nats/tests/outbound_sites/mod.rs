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

//! One check per send site of the client, shared by the armed and the
//! unarmed test binary. The filter is process-global and set once, so each
//! state needs a process of its own: `outbound_armed.rs` arms it with
//! [`test_filter`], `outbound_unarmed.rs` never does.
//!
//! [`test_filter`] is a fixture, not a rule: it swaps a token so a test can
//! see whether a message passed the filter.

use async_nats::client::traits::Publisher;
use async_nats::client::Request;
use async_nats::message::OutboundMessage;
use async_nats::{Client, HeaderMap, Message, Subject};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;

/// What the fixture filter swaps out of a body …
pub const RAW: &str = "RAW-TOKEN";
/// … and what it puts in its place.
pub const FILTERED: &str = "FILTERED";
/// The header the fixture filter adds to every message it rewrites.
pub const MARK_HEADER: &str = "X-Test-Filtered";
/// A body holding this is answered with one a byte over `max_payload`.
pub const GROW: &str = "GROW-PAST-MAX";
/// A body holding this is answered with a short one.
pub const SHRINK: &str = "SHRINK-BELOW-MAX";

/// The fixture filter armed by `outbound_armed.rs`.
#[allow(dead_code)] // the unarmed binary never arms it
pub fn test_filter(
    _subject: &str,
    headers: Option<HeaderMap>,
    payload: Bytes,
) -> (Option<HeaderMap>, Bytes) {
    let text = String::from_utf8_lossy(&payload);
    let body = if text.contains(GROW) {
        // Server default max_payload (1 MiB) plus one.
        Bytes::from(vec![b'g'; 1024 * 1024 + 1])
    } else if text.contains(SHRINK) {
        Bytes::from_static(b"shrunk")
    } else if text.contains(RAW) {
        Bytes::from(text.replace(RAW, FILTERED))
    } else {
        return (headers, payload);
    };
    let mut headers = headers.unwrap_or_default();
    headers.insert(MARK_HEADER, "yes");
    (Some(headers), body)
}

fn body() -> Bytes {
    Bytes::from(format!("before {RAW} after"))
}

fn assert_delivered(msg: &Message, armed: bool) {
    let text = String::from_utf8_lossy(&msg.payload);
    let marked = msg
        .headers
        .as_ref()
        .and_then(|h| h.get(MARK_HEADER))
        .is_some();
    if armed {
        assert!(text.contains(FILTERED) && !text.contains(RAW), "{text}");
        assert!(marked, "the filter's header arrives");
    } else {
        assert_eq!(msg.payload, body(), "sent unchanged");
        assert!(!marked);
    }
}

async fn next(sub: &mut async_nats::Subscriber) -> Message {
    tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("message within 5s")
        .expect("subscription open")
}

async fn deliver_one<F, Fut>(client: &Client, subject: &str, send: F) -> Message
where
    F: FnOnce(Client, Subject) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut sub = client.subscribe(subject.to_string()).await.unwrap();
    client.flush().await.unwrap();
    send(client.clone(), Subject::from(subject)).await;
    next(&mut sub).await
}

pub async fn publish(client: &Client, armed: bool) {
    let msg = deliver_one(client, "site.publish", |c, s| async move {
        c.publish(s, body()).await.unwrap()
    })
    .await;
    assert_delivered(&msg, armed);
}

pub async fn publish_with_headers(client: &Client, armed: bool) {
    let msg = deliver_one(client, "site.publish_with_headers", |c, s| async move {
        let mut headers = HeaderMap::new();
        headers.insert("Kept", "1");
        c.publish_with_headers(s, headers, body()).await.unwrap()
    })
    .await;
    assert_delivered(&msg, armed);
    assert_eq!(
        msg.headers.unwrap().get("Kept").map(|v| v.as_str()),
        Some("1"),
        "the caller's headers pass through"
    );
}

pub async fn publish_with_reply(client: &Client, armed: bool) {
    let msg = deliver_one(client, "site.publish_with_reply", |c, s| async move {
        c.publish_with_reply(s, "site.reply", body()).await.unwrap()
    })
    .await;
    assert_delivered(&msg, armed);
    assert_eq!(msg.reply.as_deref(), Some("site.reply"));
}

pub async fn publish_with_reply_and_headers(client: &Client, armed: bool) {
    let msg = deliver_one(
        client,
        "site.publish_with_reply_and_headers",
        |c, s| async move {
            c.publish_with_reply_and_headers(s, "site.reply", HeaderMap::new(), body())
                .await
                .unwrap()
        },
    )
    .await;
    assert_delivered(&msg, armed);
    assert_eq!(msg.reply.as_deref(), Some("site.reply"));
}

pub async fn publish_message(client: &Client, armed: bool) {
    let msg = deliver_one(client, "site.publish_message", |c, s| async move {
        Publisher::publish_message(
            &c,
            OutboundMessage {
                subject: s,
                reply: None,
                payload: body(),
                headers: None,
            },
        )
        .await
        .unwrap()
    })
    .await;
    assert_delivered(&msg, armed);
}

pub async fn sink(client: &Client, armed: bool) {
    let msg = deliver_one(client, "site.sink", |mut c, s| async move {
        c.send(OutboundMessage {
            subject: s,
            reply: None,
            payload: body(),
            headers: None,
        })
        .await
        .unwrap()
    })
    .await;
    assert_delivered(&msg, armed);
}

/// Answers every request with an empty body; hands back what it received.
async fn responder(client: &Client, subject: &str) -> tokio::sync::oneshot::Receiver<Message> {
    let mut sub = client.subscribe(subject.to_string()).await.unwrap();
    client.flush().await.unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let responder = client.clone();
    tokio::spawn(async move {
        let msg = next(&mut sub).await;
        responder
            .publish(msg.reply.clone().unwrap(), Bytes::new())
            .await
            .unwrap();
        let _ = tx.send(msg);
    });
    rx
}

/// `Client::request`: the multiplexed `Command::Request` site.
pub async fn request(client: &Client, armed: bool) {
    let received = responder(client, "site.request").await;
    client.request("site.request", body()).await.unwrap();
    assert_delivered(&received.await.unwrap(), armed);
}

/// `Client::send_request` with a caller's inbox: its own publish site.
pub async fn request_with_inbox(client: &Client, armed: bool) {
    let received = responder(client, "site.request_with_inbox").await;
    let request = Request::new().payload(body()).inbox(client.new_inbox());
    client
        .send_request("site.request_with_inbox", request)
        .await
        .unwrap();
    assert_delivered(&received.await.unwrap(), armed);
}

/// `jetstream::Context::send_publish`, the one JetStream send site.
pub async fn jetstream_publish(client: &Client, armed: bool) {
    let js = async_nats::jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "SITES".to_string(),
        subjects: vec!["site.js".to_string()],
        ..Default::default()
    })
    .await
    .unwrap();
    let mut sub = client.subscribe("site.js").await.unwrap();
    client.flush().await.unwrap();
    js.publish("site.js", body()).await.unwrap().await.unwrap();
    assert_delivered(&next(&mut sub).await, armed);
}

/// A body of 64 KiB or more crosses the blocking pool when armed.
pub async fn large_body(client: &Client, armed: bool) {
    let mut big = format!("{RAW} ").into_bytes();
    big.resize(async_nats::outbound::BLOCKING_THRESHOLD * 2, b'x');
    let big = Bytes::from(big);
    let mut sub = client.subscribe("site.large").await.unwrap();
    client.flush().await.unwrap();
    client.publish("site.large", big.clone()).await.unwrap();
    let msg = next(&mut sub).await;
    if armed {
        assert!(msg.payload.starts_with(FILTERED.as_bytes()));
    } else {
        assert_eq!(msg.payload, big);
    }
}

/// Sends `payload` through each of the eight send sites and answers, per
/// site, whether it was refused as over `max_payload`.
async fn max_payload_refusals(client: &Client, payload: Bytes) -> Vec<(&'static str, bool)> {
    use async_nats::jetstream::context::PublishErrorKind as JsKind;
    use async_nats::{PublishErrorKind as Kind, RequestErrorKind as ReqKind};

    let is_max = |r: Result<(), async_nats::PublishError>| matches!(r, Err(e) if e.kind() == Kind::MaxPayloadExceeded);
    let req_max = |r: Result<Message, async_nats::RequestError>| matches!(r, Err(e) if e.kind() == ReqKind::MaxPayloadExceeded);
    let msg = |payload: Bytes| OutboundMessage {
        subject: "size.sink".into(),
        reply: None,
        payload,
        headers: None,
    };
    let js = async_nats::jetstream::new(client.clone());
    js.get_or_create_stream(async_nats::jetstream::stream::Config {
        name: "SIZE".to_string(),
        subjects: vec!["size.js".to_string()],
        ..Default::default()
    })
    .await
    .unwrap();
    let p = || payload.clone();
    let quick = || Request::new().timeout(Some(Duration::from_millis(200)));

    vec![
        ("publish", is_max(client.publish("size.a", p()).await)),
        (
            "publish_with_headers",
            is_max(
                client
                    .publish_with_headers("size.a", HeaderMap::new(), p())
                    .await,
            ),
        ),
        (
            "publish_with_reply",
            is_max(client.publish_with_reply("size.a", "size.r", p()).await),
        ),
        (
            "publish_with_reply_and_headers",
            is_max(
                client
                    .publish_with_reply_and_headers("size.a", "size.r", HeaderMap::new(), p())
                    .await,
            ),
        ),
        (
            "publish_message",
            is_max(Publisher::publish_message(client, msg(p())).await),
        ),
        ("sink", is_max(client.clone().send(msg(p())).await)),
        (
            "request",
            req_max(client.send_request("size.q", quick().payload(p())).await),
        ),
        (
            "request_with_inbox",
            req_max(
                client
                    .send_request("size.q", quick().payload(p()).inbox(client.new_inbox()))
                    .await,
            ),
        ),
        (
            "jetstream_publish",
            matches!(
                js.publish("size.js", p()).await,
                Err(e) if e.kind() == JsKind::MaxPayloadExceeded
            ),
        ),
    ]
}

/// The size check measures what is sent: the filter's answer, not the input.
pub async fn size_is_checked_after_the_filter(client: &Client, armed: bool) {
    let grows = Bytes::from(GROW);
    let mut shrinks = SHRINK.as_bytes().to_vec();
    shrinks.resize(1024 * 1024 + 1, b's');
    let shrinks = Bytes::from(shrinks);

    for (site, refused) in max_payload_refusals(client, grows).await {
        assert_eq!(
            refused, armed,
            "{site}: a body the filter grows past max_payload"
        );
    }
    for (site, refused) in max_payload_refusals(client, shrinks).await {
        assert_eq!(
            refused, !armed,
            "{site}: a body over max_payload the filter shrinks"
        );
    }
}
