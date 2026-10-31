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

//! Every send site runs the outbound step — read from the source, a
//! backstop behind the type.
//!
//! The proof is the type: `Command::Publish` and `Command::Request` carry an
//! `outbound::Filtered`, which only the outbound step can build, so a send
//! site that skips the filter, or sends a body other than the one the filter
//! answered, does not compile. This scan keeps the list of sites visible:
//! a new one fails the count here and gets a reviewer's look.
//!
//! A message reaches the wire only as `ClientOp::Publish`, which only the
//! connection task builds, and only from a `Command::Publish` or a
//! `Command::Request`. So the send sites are the places that BUILD one of
//! those two commands, wherever the value is then sent: on the same line or
//! a later statement. Each must call `outbound(` or `outbound_sync(` earlier
//! in the same function.

use std::path::{Path, PathBuf};

/// The send sites at this revision. A new one fails the count: wire it
/// through the outbound step, then raise this number.
const SEND_SITES: usize = 9;

/// The connection task's two match arms that take the commands apart.
const PATTERNS: usize = 2;

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

#[derive(Debug, PartialEq)]
enum Use {
    /// The command is built here: a send site.
    Built,
    /// A pattern takes the command apart (`=>`, `if let … =`).
    Pattern,
}

/// Every mention of `Command::Publish`/`Command::Request` in `text`, as
/// (byte offset, what it is). Mentions in `//` comments are skipped.
fn command_uses(text: &str) -> Vec<(usize, Use)> {
    let mut out = Vec::new();
    for token in ["Command::Publish", "Command::Request"] {
        for (at, _) in text.match_indices(token) {
            let after = &text[at + token.len()..];
            if after
                .chars()
                .next()
                .is_some_and(|c| c.is_alphanumeric() || c == '_')
            {
                continue; // a longer name, e.g. `Command::PublishX`
            }
            let line_start = text[..at].rfind('\n').map_or(0, |i| i + 1);
            if text[line_start..at].contains("//") {
                continue;
            }
            out.push((at, classify(after)));
        }
    }
    out.sort_by_key(|(at, _)| *at);
    out
}

/// What follows the variant name: its `{ … }` or `( … )`, balanced, then
/// `=>` or a lone `=` for a pattern; anything else builds a value.
fn classify(after: &str) -> Use {
    let rest = after.trim_start();
    let rest = match rest.chars().next() {
        Some(open @ ('{' | '(')) => {
            let close = if open == '{' { '}' } else { ')' };
            let mut depth = 0usize;
            let mut end = rest.len();
            for (i, c) in rest.char_indices() {
                if c == open {
                    depth += 1;
                } else if c == close {
                    depth -= 1;
                    if depth == 0 {
                        end = i + 1;
                        break;
                    }
                }
            }
            &rest[end..]
        }
        _ => rest,
    };
    let rest = rest.trim_start();
    if rest.starts_with("=>") || (rest.starts_with('=') && !rest.starts_with("==")) {
        Use::Pattern
    } else {
        Use::Built
    }
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

/// The scanner's own control, in the same run: the shapes the review found
/// it missing are counted as sites, and patterns are not.
#[test]
fn the_scan_sees_a_site_in_any_shape() {
    let two_statements = "let cmd = Command::Publish { message, reply: None };\n\
                          self.sender.send(cmd).await?;";
    assert_eq!(command_uses(two_statements), vec![(10, Use::Built)]);

    let tuple_form = "let cmd = Command::Publish(msg);";
    assert_eq!(command_uses(tuple_form), vec![(10, Use::Built)]);

    let request = "tx.send(Command::Request {\n message,\n respond,\n sender,\n})";
    assert_eq!(command_uses(request), vec![(8, Use::Built)]);

    let arm = "Command::Publish {\n message,\n reply: respond,\n} => {";
    assert_eq!(command_uses(arm), vec![(0, Use::Pattern)]);

    let if_let = "if let Command::Request { message, .. } = command {";
    assert_eq!(command_uses(if_let), vec![(7, Use::Pattern)]);

    let comment = "// a Command::Publish is sealed";
    assert!(command_uses(comment).is_empty());
}

#[test]
fn every_send_site_runs_the_outbound_step() {
    let mut sites = Vec::new();
    let mut patterns = Vec::new();
    for (file, text) in sources() {
        let lines: Vec<&str> = text.lines().collect();
        for (at, kind) in command_uses(&text) {
            let line = text[..at].matches('\n').count();
            let place = format!("{file}:{}", line + 1);
            match kind {
                Use::Built => {
                    let body = enclosing_fn(&lines, line);
                    let filtered = body.contains("outbound(") || body.contains("outbound_sync(");
                    sites.push((place, filtered));
                }
                Use::Pattern => patterns.push(place),
            }
        }
    }
    let unfiltered: Vec<_> = sites.iter().filter(|(_, f)| !f).collect();
    assert!(
        unfiltered.is_empty(),
        "send sites without the outbound step: {unfiltered:?}"
    );
    assert_eq!(sites.len(), SEND_SITES, "the send sites changed: {sites:?}");
    assert_eq!(patterns.len(), PATTERNS, "{patterns:?}");
    assert!(
        patterns.iter().all(|p| p.starts_with("lib.rs:")),
        "{patterns:?}"
    );
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
