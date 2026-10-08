//! Antfly-backed implementations of `StateRuntime`'s feature groups.
//!
//! Each submodule mirrors one SQLite-backed module in `state/src/runtime/`
//! (goals, memories, queue, guardian feedback, remote control, external
//! agent config imports, and a small thread-metadata adapter), reimplemented
//! as free functions over a shared [`codex_antfly::Antfly`] handle instead of
//! a `sqlx::SqlitePool`.
//!
//! ## Key layout
//!
//! Keys are namespaced under `st:` (StateRuntime) so they never collide with
//! the thread store's `ts:` keys in the same Antfly table:
//!
//! ```text
//! st:goal:{thread}                       GoalStore: ThreadGoal, JSON
//! st:goaldef:{thread}                    GoalStore: continuation-deferral marker
//! st:mem:{version}:s1:{thread}           MemoryStore: Stage1Output + job state
//! st:mem:{version}:p2                    MemoryStore: the single global phase2 job
//! st:mem:{version}:progress              MemoryStore: consolidation_progress (max_thread_count)
//! st:mem:polluted:{thread}               thread_adapter: polluted-memory-mode marker
//! ts:t:{thread}                          thread_adapter: read-only view of AntflyThreadStore's
//!                                        thread record (and `patch.preview` write-back for
//!                                        `set_thread_preview_if_empty`)
//! st:edgep:{parent}:{child}              thread_adapter: spawn edge, by parent
//! st:edgec:{child}                       thread_adapter: spawn edge, by child (one parent each)
//! st:queue:{thread}:{ordinal}            queue item, in insertion/reorder order
//! st:queuerev:global                     queue: global monotonic change counter
//! st:queuerev:t:{thread}                 queue: last-touched revision per thread
//! st:guardian:{id}                       guardian review record (id is UUIDv7, globally ordered)
//! st:remote:{url}:{account}:{client}     remote control enrollment
//! st:extimport:{import_id}               external agent config import record
//! st:extimportx:{desc completed_ms}:{id} external agent config import, completion order
//! ```
//!
//! Every read-modify-write sequence holds [`codex_antfly::Antfly::lock`] for
//! as little time as possible: read, decide, write, release. That lock is
//! process-wide and shared by every store built on the same `Antfly` handle,
//! so it must never be held across an `.await` that is not itself part of
//! the read-modify-write (for example, never across a search call).

pub(crate) mod external_agent_config_imports;
pub(crate) mod goals;
pub(crate) mod guardian_feedback;
pub(crate) mod memories;
pub(crate) mod queue;
pub(crate) mod remote_control;
pub(crate) mod thread_adapter;

/// Maps an [`codex_antfly::AntflyError`] to `anyhow::Error` with a module tag,
/// matching the style of the thread-store's `internal()` helper.
pub(crate) fn internal(err: codex_antfly::AntflyError) -> anyhow::Error {
    anyhow::anyhow!("antfly: {err}")
}
