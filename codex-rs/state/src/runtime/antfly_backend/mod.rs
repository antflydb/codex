//! Antfly-backed implementations of `StateRuntime`'s feature groups.
//!
//! Each submodule mirrors one SQLite-backed module in `state/src/runtime/`
//! (goals, memories, queue, guardian feedback, remote control, external
//! agent config imports), reimplemented as free functions over a shared
//! [`codex_antfly::Antfly`] handle instead of a `sqlx::SqlitePool`.
//!
//! `threads`, `projects`, `thread_sections`, and `thread_attachments` are
//! SQL tables (`codex_threads`, `codex_thread_spawn_edges`,
//! `codex_projects`, `codex_thread_sections`, `codex_thread_attachments`,
//! ...; see `codex_antfly::schema`) shared with `codex-thread-store`'s
//! `AntflyThreadStore` (`thread-store/src/antfly/`): one source of truth for
//! thread state, not a `StateRuntime`-only copy. `thread_adapter` is the
//! thin remainder of what used to bridge `AntflyThreadStore`'s old
//! key-value thread record into `StateRuntime`'s memory pipeline
//! (`antfly_backend::memories`); it now reads the same `codex_threads` rows
//! by delegating into `threads`.
//!
//! ## Key layout
//!
//! Everything else here still uses `st:`-prefixed keys in the legacy
//! key-value table, namespaced so they never collide with the thread
//! store's `ts:` keys in the same Antfly table:
//!
//! ```text
//! st:goal:{thread}                       GoalStore: ThreadGoal, JSON
//! st:goaldef:{thread}                    GoalStore: continuation-deferral marker
//! st:mem:{version}:s1:{thread}           MemoryStore: Stage1Output + job state
//! st:mem:{version}:p2                    MemoryStore: the single global phase2 job
//! st:mem:{version}:progress              MemoryStore: consolidation_progress (max_thread_count)
//! st:queue:{thread}:{ordinal}            queue item, in insertion/reorder order
//! st:queuerev:global                     queue: global monotonic change counter
//! st:queuerev:t:{thread}                 queue: last-touched revision per thread
//! st:guardian:{id}                       guardian review record (id is UUIDv7, globally ordered)
//! st:remote:{url}:{account}:{client}     remote control enrollment
//! st:extimport:{import_id}               external agent config import record
//! st:extimportx:{desc completed_ms}:{id} external agent config import, completion order
//! ```
//!
//! Every read-modify-write sequence over those key-value tables holds
//! [`codex_antfly::Antfly::lock`] for as little time as possible: read,
//! decide, write, release. That lock is process-wide and shared by every
//! store built on the same `Antfly` handle, so it must never be held across
//! an `.await` that is not itself part of the read-modify-write (for
//! example, never across a search call). SQL tables use a transaction
//! instead (see `codex_antfly::sql::SqlTx`).

pub(crate) mod external_agent_config_imports;
pub(crate) mod goals;
pub(crate) mod guardian_feedback;
pub(crate) mod memories;
pub(crate) mod projects;
pub(crate) mod queue;
pub(crate) mod remote_control;
pub(crate) mod thread_adapter;
pub(crate) mod thread_attachments;
pub(crate) mod thread_sections;
pub(crate) mod threads;

/// Maps an [`codex_antfly::AntflyError`] to `anyhow::Error` with a module tag,
/// matching the style of the thread-store's `internal()` helper.
pub(crate) fn internal(err: codex_antfly::AntflyError) -> anyhow::Error {
    anyhow::anyhow!("antfly: {err}")
}
