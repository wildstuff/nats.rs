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

//! The filter ARMED: every send site passes its message through it.
//! A process of its own: the filter is process-global and set once.

mod outbound_sites;

use std::sync::Once;

static ARM: Once = Once::new();

async fn client() -> (nats_server::Server, async_nats::Client) {
    ARM.call_once(|| {
        async_nats::outbound::set_outbound_filter(outbound_sites::test_filter)
            .expect("this binary arms the filter once");
    });
    let server = nats_server::run_server("tests/configs/jetstream.conf");
    let client = async_nats::connect(server.client_url()).await.unwrap();
    (server, client)
}

#[tokio::test]
async fn publish() {
    let (_server, client) = client().await;
    outbound_sites::publish(&client, true).await;
}

#[tokio::test]
async fn publish_with_headers() {
    let (_server, client) = client().await;
    outbound_sites::publish_with_headers(&client, true).await;
}

#[tokio::test]
async fn publish_with_reply() {
    let (_server, client) = client().await;
    outbound_sites::publish_with_reply(&client, true).await;
}

#[tokio::test]
async fn publish_with_reply_and_headers() {
    let (_server, client) = client().await;
    outbound_sites::publish_with_reply_and_headers(&client, true).await;
}

#[tokio::test]
async fn publish_message() {
    let (_server, client) = client().await;
    outbound_sites::publish_message(&client, true).await;
}

#[tokio::test]
async fn sink() {
    let (_server, client) = client().await;
    outbound_sites::sink(&client, true).await;
}

#[tokio::test]
async fn request() {
    let (_server, client) = client().await;
    outbound_sites::request(&client, true).await;
}

#[tokio::test]
async fn request_with_inbox() {
    let (_server, client) = client().await;
    outbound_sites::request_with_inbox(&client, true).await;
}

#[tokio::test]
async fn jetstream_publish() {
    let (_server, client) = client().await;
    outbound_sites::jetstream_publish(&client, true).await;
}

#[tokio::test]
async fn large_body() {
    let (_server, client) = client().await;
    outbound_sites::large_body(&client, true).await;
}

#[tokio::test]
async fn size_is_checked_after_the_filter() {
    let (_server, client) = client().await;
    outbound_sites::size_is_checked_after_the_filter(&client, true).await;
}
