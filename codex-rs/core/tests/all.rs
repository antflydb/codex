#![allow(clippy::expect_used)]
// TEMPORARY, pre-existing and unrelated to thread/state persistence: see
// `codex-core`'s `lib.rs` for the same `recursion_limit` workaround and why.
#![recursion_limit = "256"]

// Single integration test binary that aggregates all test modules.
// The submodules live in `tests/all/`.
pub use codex_protocol::error;

mod suite;
