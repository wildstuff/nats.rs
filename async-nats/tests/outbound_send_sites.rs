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

//! Every send site runs the outbound step — read from the source, so a new
//! site that skips it fails here, not in production.
//!
//! A message reaches the wire only as `ClientOp::Publish`, which only the
//! connection task builds, and only from a `Command::Publish` or a
//! `Command::Request`. So the send sites are the places that BUILD one of
//! those two commands and hand it to the connection task's channel. Each
//! must call `outbound(` or `outbound_sync(` earlier in the same function.

use std::path::{Path, PathBuf};

/// The send sites at this revision. A new one fails the count: wire it
/// through the outbound step, then raise this number.
const SEND_SITES: usize = 9;

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The file's text up to its first `#[cfg(test)]`: the code that ships.
fn shipped(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap();
    match text.find("#[cfg(test)]") {
        Some(at) => text[..at].to_string(),
        None => text,
    }
}

fn sources() -> Vec<(String, String)> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    assert!(files.len() > 20, "read the crate's sources");
    files
        .into_iter()
        .map(|path| {
            let name = path.strip_prefix(&src).unwrap().display().to_string();
            (name, shipped(&path))
        })
        .collect()
}

/// A line that hands a newly built `Command::Publish`/`Command::Request` to
/// the connection task's channel.
fn is_send_site(line: &str) -> bool {
    let builds = line.contains("Command::Publish(") || line.contains("Command::Request {");
    let sends = line.contains("send(") || line.contains("start_send_unpin(");
    builds && sends
}

/// The function body text from the nearest `fn` above `line` to `line`.
fn enclosing_fn(lines: &[&str], line: usize) -> String {
    let start = (0..=line)
        .rev()
        .find(|&i| {
            let l = lines[i].trim_start();
            l.starts_with("fn ")
                || l.starts_with("pub fn ")
                || l.starts_with("async fn ")
                || l.starts_with("pub async fn ")
                || l.starts_with("pub(crate) fn ")
                || l.starts_with("pub(crate) async fn ")
        })
        .expect("a send site sits inside a function");
    lines[start..=line].join("\n")
}

#[test]
fn every_send_site_runs_the_outbound_step() {
    let mut sites = Vec::new();
    for (file, text) in sources() {
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if is_send_site(line) {
                let body = enclosing_fn(&lines, i);
                let filtered = body.contains("outbound(") || body.contains("outbound_sync(");
                sites.push((format!("{file}:{}", i + 1), filtered));
            }
        }
    }
    let unfiltered: Vec<_> = sites.iter().filter(|(_, f)| !f).collect();
    assert!(
        unfiltered.is_empty(),
        "send sites without the outbound step: {unfiltered:?}"
    );
    assert_eq!(sites.len(), SEND_SITES, "the send sites changed: {sites:?}");
}

/// Only the connection task builds a wire publish, from the two commands
/// above — so those commands are every road to the wire.
#[test]
fn only_the_connection_task_builds_a_wire_publish() {
    let builders: Vec<String> = sources()
        .into_iter()
        .flat_map(|(file, text)| {
            text.lines()
                .enumerate()
                .filter(|(_, l)| l.contains("ClientOp::Publish {"))
                .map(|(i, _)| format!("{file}:{}", i + 1))
                .collect::<Vec<_>>()
        })
        .collect();
    // lib.rs: the connection task's `Command::Request` and `Command::Publish`
    // arms. connection.rs: the writer's match arm that serialises it.
    let (lib, rest): (Vec<_>, Vec<_>) = builders.iter().partition(|s| s.starts_with("lib.rs:"));
    assert_eq!(lib.len(), 2, "{builders:?}");
    assert_eq!(rest.len(), 1, "{builders:?}");
    assert!(rest[0].starts_with("connection.rs:"), "{builders:?}");
}
