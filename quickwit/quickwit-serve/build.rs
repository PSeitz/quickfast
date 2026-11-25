// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::path::Path;
use std::process::Command;

use time::OffsetDateTime;
use time::macros::format_description;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=QW_COMMIT_DATE");
    println!("cargo:rerun-if-env-changed=QW_COMMIT_HASH");
    println!("cargo:rerun-if-env-changed=QW_COMMIT_TAGS");
    emit_git_rerun_hints();

    println!(
        "cargo:rustc-env=BUILD_DATE={}",
        OffsetDateTime::now_utc()
            .format(format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second]Z"
            ))
            .unwrap()
    );
    println!(
        "cargo:rustc-env=BUILD_PROFILE={}",
        env::var("PROFILE").unwrap()
    );
    println!(
        "cargo:rustc-env=BUILD_TARGET={}",
        env::var("TARGET").unwrap()
    );
    commit_info();
}

/// Emits `rerun-if-changed` directives for the current Git `HEAD` (and the
/// ref it points to, if symbolic) so edits to Rust sources do not re-run this
/// build script and invalidate downstream crates via a fresh `BUILD_DATE`.
fn emit_git_rerun_hints() {
    let Ok(output) = Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .output()
    else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let git_dir_str = String::from_utf8_lossy(&output.stdout);
    let git_dir = Path::new(git_dir_str.trim());
    let head_path = git_dir.join("HEAD");
    let Ok(head_contents) = std::fs::read_to_string(&head_path) else {
        return;
    };
    println!("cargo:rerun-if-changed={}", head_path.display());
    if let Some(ref_rel) = head_contents.trim().strip_prefix("ref: ") {
        let ref_path = git_dir.join(ref_rel);
        if matches!(ref_path.try_exists(), Ok(true)) {
            println!("cargo:rerun-if-changed={}", ref_path.display());
        }
    }
}

/// Extracts commit date, hash, and tags
fn commit_info() {
    // Extract commit date and hash.
    let output_bytes = match Command::new("git")
        .arg("log")
        .arg("-1")
        .arg("--format=%cd %H")
        .arg("--date=format-local:%Y-%m-%dT%H:%M:%SZ")
        .env("TZ", "UTC0")
        .output()
    {
        Ok(output) if output.status.success() => output.stdout,
        _ => Vec::new(),
    };
    let output = String::from_utf8(output_bytes).unwrap();
    let mut parts = output.split_whitespace();

    if let Some(commit_date) = parts.next() {
        println!("cargo:rustc-env=QW_COMMIT_DATE={commit_date}");
    }
    if let Some(commit_hash) = parts.next() {
        println!("cargo:rustc-env=QW_COMMIT_HASH={commit_hash}");
    }

    // Extract commit tags.
    let output_bytes = match Command::new("git")
        .arg("tag")
        .arg("--points-at")
        .arg("HEAD")
        .output()
    {
        Ok(output) if output.status.success() => output.stdout,
        _ => Vec::new(),
    };
    let output = String::from_utf8(output_bytes).unwrap();
    let tags = output.lines().collect::<Vec<_>>();
    if !tags.is_empty() {
        println!("cargo:rustc-env=QW_COMMIT_TAGS={}", tags.join(","));
    }
}
