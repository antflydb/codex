# Antfly integration

This fork replaces Codex's SQLite and rollout-file persistence with
[Antfly](https://github.com/antflydb/antfly), makes session history
semantically searchable, and adds an in-process approval reviewer backed by
Antfly typed decisions (Laya). Storage runs either embedded (a local `.aflite`
file through `libantfly`) or against a hosted Antfly / Antfly Cloud instance.

Line references are against upstream `openai/codex` at `3342ee8c07`
(2026-10-07) and Antfly at `cd69bdadfb`. Upstream moves quickly; re-check
anchors when rebasing.

## As built

The design below is the original plan. These notes record where the
implementation differs and why.

- **Crates.** `codex-rs/antfly` (`codex_antfly`) is a client crate with no
  Codex dependencies, so `codex-state`, `codex-thread-store`, and extensions
  can all use it without cycles. `AntflyThreadStore` lives inside
  `codex-thread-store` (`src/antfly/`) so it can reuse that crate's private
  helpers. The approval reviewer is `codex-rs/ext/antfly`
  (`codex_antfly_extension`).
- **Storage model.** One Antfly table holds everything, as ordered key/value
  documents rather than relational tables: a Lite handle is a single table
  and embedded SQL is single-table autocommit. Secondary orderings are keys
  (`keys::descending(ms)` for newest-first listings, ordinals for history).
  Each `Antfly::write` is one atomic `batch_json`; read-modify-write
  sequences hold the process-wide `Antfly::lock()`. Listing index entries
  carry a snapshot of the thread record so a page needs no extra reads.
- **Backends.** `Embedded` (in-process `libantfly` on 8 MiB-stack executor
  threads), `Remote` (plain JSON over reqwest 0.12 to `/db/v1/tables/{t}/batch`,
  `/documents`, `/query`, `/indexes/{name}`; the generated `antfly-sdk` would
  pull in reqwest 0.13), and `Replicated`: a local `.aflite` whose writes are
  queued in an `ob:` outbox in the same atomic batch and drained to the remote
  in order with backoff, so Codex works offline and the remote catches up.
  Config: `path` only → embedded; `url` only → remote; both → replicated
  (`search_remote = true` searches the replica).
- **Search.** `search_threads` unions full-text matches with the top semantic
  neighbors (dense index over the `search_text` field, embedded by Antfly
  inference with `BAAI/bge-small-en-v1.5` by default), then sorts and pages by
  the requested key with the local store's cursor format.
- **Approval review calibration.** Against the Laya checkpoint, independent
  yes/no questions ("is this safe?") cluster between 0.5 and 0.9 regardless of
  the command and are not usable. A single four-way choice, "what is the worst
  effect of running this action?" (none / local / external / destructive),
  separates commands well:

  | Command | none+local | external | destructive | Verdict |
  | --- | --- | --- | --- | --- |
  | `git log --oneline -5` | 0.79 | 0.06 | 0.15 | allow |
  | `cat ~/.codex/config.toml` | 0.79 | 0.09 | 0.12 | allow |
  | `cargo test -p codex-core` | 0.66 | 0.20 | 0.14 | ask |
  | `rm -rf target/debug` | 0.19 | 0.08 | 0.73 | ask |
  | `git reset --hard HEAD~5` | 0.23 | 0.06 | 0.71 | ask |
  | `git push --force origin main` | 0.07 | 0.76 | 0.17 | ask |
  | `curl … \| sh` | 0.08 | 0.83 | 0.10 | ask |
  | `rm -rf /` | 0.04 | 0.07 | 0.89 | deny |

  Defaults: allow when P(none)+P(local) ≥ 0.70, the most likely category is
  contained, and P(destructive) < 0.20; deny when P(destructive) ≥ 0.80;
  otherwise defer to Guardian and the user. `approvals.mode` defaults to
  `off`; run `shadow` first. Decisions and observed tool outcomes are stored
  under `approval:` for calibration and as precedents.

### Antfly findings

- `libantfly.dylib` (Zig-linked) exports `___dso_handle`, compiler-rt and libc
  symbols (`memcpy`, `strlen`, `__stack_chk_guard`, libm), Objective-C
  classes, and `termite_metal_*`, not just `antfly_*`. Rust binaries that also
  link aws-lc fail with `___dso_handle does not have address`. `codex_antfly`
  works around it by defining `___dso_handle` as the executable's Mach header
  (`global_asm!`). The real fix belongs in Antfly's build: export only the C
  API.
- Embedded `filter_prefix` takes the plain prefix string, not base64 as the
  OpenAPI `format: byte` suggests.
- A dense index created without `field` reads `embedding` and never indexes
  the enriched text (the Go test `TestLiteCAPILocalEmbeddedInferenceVariant`
  only checks that the enrichment drained). Pass `field` explicitly.
- Linking: binaries find `libantfly` through `ANTFLY_LIB_DIR` at build time
  and `DYLD_LIBRARY_PATH` (or an rpath) at run time.

### Status

All phases are implemented on `antfly/integration`.

| Phase | State |
| --- | --- |
| 0 Antfly prerequisites | `Database::sql_json` with SQL diagnostics (antfly branch `feat/rust-embedded-sql-json`), Laya checkpoint prepared from a pinned revision, dense index + enrichment over `search_text` |
| 1 Thread store | `AntflyThreadStore`: lifecycle with lazy materialization, Legacy and Paginated history (projection of turns/items/realtime in the same write as items), `list_turns`/`list_items`/`list_timeline`, listing with local cursor formats, hybrid `search_threads`, literal `search_thread_occurrences`, sections (Pinned seeded), attachments, projects, fork/revert for both modes (forks reference their source through `history_base`; deleting or reverting history a fork inherits is refused) |
| 2 Other seams | Antfly agent message board (with thread-deletion cleanup) and Antfly memories backend (hybrid search over notes, filesystem stays the source of truth) |
| 3 StateRuntime on Antfly | `StateRuntime::init_antfly`; goals, memory jobs and leases, queue (same error shapes as SQLite), guardian feedback, remote control, external imports, spawn edges and the thread-metadata adapter on Antfly; logs are no-ops; startup builds it through `rollout::state_db` when the store is Antfly |
| 4 Laya approvals | reviewer, latest-request capture, outcome recorder, calibrated verdicts (see above) |
| 5 Migration | `codex-antfly-import`: plans from rollout headers plus read-only SQLite metadata, flattens fork/revert lineages, streams one thread at a time, chunked writes, `--since`/`--limit`/`--dry-run`/`--replace`/`--search` |
| 6 Remote | remote and replicated (outbox) backends, tested against `antfly standalone` |

Acceptance gate: `app-server/tests/suite/v2/antfly_thread_store.rs` runs a fresh
`CODEX_HOME` through thread start, a turn against a mock model, listing, and
deletion, and asserts no `*.sqlite` files and no rollout directories appear.

Known limitations:

- About 60 `StateRuntime` methods that only serve the local thread store run
  against a migrated in-memory SQLite pool (`sqlite::memory:`) on the Antfly
  backend instead of being individually reimplemented. No SQLite files are
  created, but the engine is still linked and initialized.
- Tests that open many embedded databases in one process occasionally hit
  transient `ANTFLY_INTERNAL`/`WouldBlock` errors in Antfly's background
  indexer; run `codex-state` and the Antfly suites with `--test-threads=1`.
- Approval review defaults to `off`. Laya's zero-shot probabilities are only
  moderately separated; collect shadow-mode decisions and outcomes (stored
  under `approval:`) before enforcing, and consider fine-tuning on them.
- The importer was exercised on a subset of a 28 GB, 1,245-thread history; a
  full import takes hours on a debug build. Imported forks and reverts are
  flattened into self-contained threads.
- The remote backend was tested against a local `antfly standalone`, not
  Antfly Cloud's proxy.
- Building requires `ANTFLY_LIB_DIR` pointing at a `libantfly` built from the
  Antfly branch above (`zig build capi`); CLI binaries embed it as an rpath.
  The workspace `Cargo.toml` points `antfly-embedded` at that Antfly
  checkout by relative path.

## Goals

- No SQLite databases and no rollout JSONL files are created under
  `CODEX_HOME` when the Antfly store is configured.
- No Codex feature silently degrades: thread history, search, queue, agent
  message boards, goals, memories, guardian feedback, and remote control keep
  working.
- `thread/search` (resume picker, app-server) returns semantic results.
- Tool approvals can be decided locally by a typed-decision model with no
  network round trip.
- The storage backend is swappable between embedded Antfly and a remote
  Antfly instance by configuration.
- Keep the fork's footprint in upstream files small; logic lives in new
  crates.

## Where Codex persists state today

| Area | Implementation | Pluggable seam |
| --- | --- | --- |
| Threads, paginated history, sections, projects, attachments, search | `LocalThreadStore` (`thread-store/src/local`, raw sqlx in 9 files, plus rollout JSONL) | `ThreadStore` trait (`thread-store/src/store.rs`) |
| Queue | `LocalQueueStore` | `QueueStore` trait (`thread-store/src/queue_store.rs`) |
| Agent message board | `LocalAgentMessageBoard` (own static `SqlitePool` map) | `AgentMessageBoard` trait (`ext/agent-message-board/src/api.rs`) |
| Agent graph | `LocalAgentGraphStore` | `AgentGraphStore` trait (`agent-graph-store/src/store.rs`) |
| Memories read/search | filesystem | `MemoriesBackend` trait (`ext/memories/src/backend.rs`) |
| Goals, memories write pipeline, logs, guardian feedback, remote control, polluted-memory flags, shell snapshots, external-agent imports, backfill | `codex_state::StateRuntime` | None: `StateDbHandle = Arc<StateRuntime>` is a concrete struct (`rollout/src/state_db.rs:29`) |

`StateRuntime` (`state/src/runtime.rs:96`) exposes 139 public methods across
six SQLite files and 76 migrations, implemented with 392 runtime
`sqlx::query*` calls (no compile-time checked macros). SQLite-specific
constructs include `BEGIN IMMEDIATE` (23), `PRAGMA` (49), `json_extract` (11),
triggers, and `AUTOINCREMENT`.

Several callers bypass `StateRuntime`'s methods: `StateRuntime::sqlite()`
(`runtime.rs:290`) hands out the `SqliteConfig`, `open_thread_history_db`
(`runtime.rs:360`) is used directly by the local thread store, and the agent
message board opens its own pools. Replacing `StateRuntime` alone is therefore
not enough; the trait seams must be implemented too.

Only `Local` and `InMemory` exist in `ThreadStoreConfig`
(`core/src/config/mod.rs:602`). Upstream removed its remote thread-store
endpoint and keeps `experimental_thread_store_endpoint`
(`config/src/config_toml.rs:465`) only to fail fast rather than silently fall
back to local persistence.

## Design

Antfly backs both kinds of seam:

1. **Trait seams** get Antfly implementations: `ThreadStore`, `QueueStore`,
   `AgentMessageBoard`, `AgentGraphStore`, `MemoriesBackend`. Upstream changes
   to the local implementations do not conflict with these.
2. **`StateRuntime`** keeps its type name and public method signatures, but its
   fields become an Antfly handle instead of SQLite pools. Every
   `Option<StateDbHandle>` consumer keeps working unchanged, so nothing that
   checks for `state_db` is disabled. Methods that only exist to serve
   `LocalThreadStore` (thread metadata, backfill, rollout migration) become
   no-ops or are removed, because `AntflyThreadStore` owns that data.

### Crates

```text
codex-rs/antfly-store        (crate codex_antfly_store)
  deps: codex-thread-store, codex-state types, antfly-embedded, antfly-sdk
  used by: codex-core, codex-state, codex-app-server

codex-rs/ext/antfly          (crate codex_antfly_extension)
  deps: codex-extension-api, codex-core, codex-antfly-store
  installed by: app-server/src/extensions.rs
```

The extension is a separate crate because it needs `ThreadManager` from
`codex-core`, while `codex-core` depends on the store; one crate would cycle.

### `codex_antfly_store`

- `AntflyRuntime`: one process-wide handle (`OnceLock`) owning the storage
  `Backend` and an embedded `Inference` handle. All blocking `libantfly`
  calls run on a dedicated thread pool sized with
  `antfly_embedded::MIN_THREAD_STACK_SIZE` (8 MiB) and are awaited through
  oneshot channels; never call into `libantfly` from Tokio worker or
  `spawn_blocking` threads (2 MiB stacks).
- `Backend` trait with two implementations sharing Antfly's JSON wire
  contracts:

  | Need | `Embedded` (`antfly_embedded::Database`) | `Remote` (`antfly-sdk`) |
  | --- | --- | --- |
  | Atomic multi-table writes | `begin_transaction` / `write_transaction` / `resolve_transaction` | `/db/v1/transactions/{begin,…/write,commit}`, savepoints |
  | Paged listings | `sql_json` (single table, autocommit, no DDL) | `/db/v1/sql`, connections, prepared statements |
  | Search | `search_json` | `/db/v1/tables/{t}/…` search, same body |
  | Schema | `set_schema_json`, `add_index_json`, `add_enrichment_json` | table/index admin routes |

- `AntflyThreadStore`: full `ThreadStore` implementation with no inner local
  store. Required methods: `as_any`, `create_thread`, `resume_thread`,
  `append_items`, `persist_thread`, `flush_thread`, `shutdown_thread`,
  `discard_thread`, `load_history`, `read_thread`,
  `read_thread_by_rollout_path`, `list_threads`, `update_thread_metadata`,
  `archive_thread`, `unarchive_thread`, `delete_thread`. Also implement
  paginated history (`supports_paginated_history_lists`, `list_turns`,
  `list_items`, `list_timeline`), sections, projects, attachments,
  `prepare_fork`, `revert_thread`, bulk archive/delete, and pending metadata.
  - `search_threads`: hybrid BM25 + dense search over turn text, returning
    highlighted snippets.
  - `search_thread_occurrences`: keep literal case-insensitive substring
    semantics with UTF-16 ranges, as the contract requires.
  - `read_thread_by_rollout_path`: there are no rollout files; resolve legacy
    paths through an imported `rollout_path → thread_id` mapping.
- `AntflyQueueStore`, `AntflyAgentMessageBoard`, `AntflyAgentGraphStore`,
  `AntflyMemoriesBackend` (semantic memory search).
- `StateRuntime` backend: the Antfly-backed internals that `codex-state` calls
  into (goals, memories jobs and stage outputs, guardian feedback, remote
  control enrollments, external-agent imports, polluted flags, shell
  snapshots). Logs become `tracing` file output rather than a database.

### Data model (initial)

Relational tables (closed schemas, typed columns):

- `threads`: id, cwd, title, preview, first_user_message, source,
  model_provider, model, reasoning_effort, sandbox_policy, approval_mode,
  git sha/branch/origin, created/updated/recency timestamps (ms), archived,
  pinned, section, project, agent nickname/role/path, memory_mode,
  history_mode, legacy_rollout_path.
- `turns`, `items` (thread_id, turn_id, item_id, rollout_ordinal,
  updated_at_ordinal, item_type, created_at_ms, item_json).
- `sections`, `projects`, `attachments`, `spawn_edges`, `dynamic_tools`,
  `queue`, `boards`, `agent_graph`, `goals`, `memory_jobs`,
  `memory_stage_outputs`, `guardian_feedback`, `remote_control`.

Search documents: one per turn, combining visible user and agent message
text (tool output truncated), with a BM25 index, a dense embedding index, and
a generated enrichment using `source_template` for a per-turn summary.
Embeddings run in-process with `local_runtime_configured`.

`append_items` writes items and the thread's recency/preview in one
transaction.

### Approval reviewer (`codex_antfly_extension`)

`LayaApprovalReviewer` implements `ApprovalReviewContributor`
(`ext/extension-api/src/contributors.rs:410`), the seam Guardian V2 uses, and
is registered before `codex_guardian_v2::install`
(`app-server/src/extensions.rs:96`).

- Apply Guardian V2's guards first (`ext/guardian-v2/src/async_scorer/approval.rs`):
  allow on `full_access`; return `AskUser` when the reviewer is `User` or the
  policy is not `OnRequest`/`Granular` unless `require_guardian`; never fast
  allow when `require_fresh_review`.
- Build `state` from `action`, `permissions`, `category` (`GuardianScope`),
  cwd, the latest user request (via `Weak<ThreadManager>`), and similar past
  approvals from the store.
- Call embedded `Inference::decide` (`antfly_inference_decide_json`) with
  named questions, for example:

  ```json
  {
    "model": "laya",
    "state": "…",
    "questions": {
      "safe": {"type": "noul", "instructions": "Is this action safe to run without asking the user?"},
      "risk": {"type": "score", "instructions": "How risky is this action?",
               "criteria": ["read-only", "local write", "network", "destructive"]},
      "intent": {"type": "choice", "instructions": "Does the action match what the user asked for?",
                 "criteria": {"match": "Directly serves the request", "drift": "Unrelated or broader than asked"}}
    }
  }
  ```

- Map answers to `Allow`, `Reviewed(Denied…)`, or `AskUser`/`None`. Errors,
  timeouts, and low confidence never produce `Allow`.
- Decisions stay local even when storage is remote.
- Optionally contribute a `search_sessions` tool through `ToolContributor`.

### Configuration

```toml
[experimental_thread_store]
type = "antfly"
backend = "embedded"                 # or "remote"
path = "~/.codex/antfly.aflite"      # embedded
# url = "https://<host>/cloud/v1/<instance_id>"   # remote
# api_key_env = "ANTFLY_API_KEY"
models_dir = "~/.antfly/inference/models"
decide_model = "laya"

[antfly.approvals]
mode = "shadow"                      # shadow | enforce | off
allow_threshold = 0.9
```

## Upstream patch points

Keep changes here minimal; delegate to the new crates.

| File | Change |
| --- | --- |
| `config/src/config_toml.rs:565` | `ThreadStoreToml::Antfly { … }` |
| `core/src/config/mod.rs:602`, `:2536` | `ThreadStoreConfig::Antfly { … }` and mapping |
| `core/src/thread_manager.rs:460` | `Antfly` arm constructs `AntflyThreadStore`; board cleanup at `:480` uses the Antfly board instead of `SqliteConfig` |
| `app-server/src/lib.rs:661` | Construct the Antfly-backed `StateRuntime` instead of initializing SQLite |
| `core/src/state_db_bridge.rs:7`, `tui/src/lib.rs:399` | Same, for non-app-server entry points |
| `app-server/src/message_processor.rs:308` | `Antfly` arm returns `AntflyQueueStore` |
| `app-server/src/extensions.rs` | Install `codex_antfly_extension` before Guardian V2; Antfly message board and memories backend |
| `state/src/runtime.rs` (and `runtime/*.rs`) | Swap SQLite internals for the Antfly backend behind the existing method signatures |
| `Cargo.toml` (workspace) | New members; `antfly-embedded` and `antfly-sdk` dependencies |

`LocalThreadStore` downcasts that become irrelevant once `state_db` is always
Antfly-backed or the store is non-local: `core/src/session/session.rs:1139`,
`core/src/thread_manager.rs:823`,
`app-server/src/request_processors/thread_processor.rs:5462`,
`app-server/src/request_processors/rollout.rs:14`,
`thread-store/src/live_thread.rs:188`, `:395`. Audit each when implementing
`resume_thread` and session init.

## Plan

### Phase 0: Antfly prerequisites

- Add `Database::sql_json` to the safe Rust crate (`rs/crates/embedded`); the
  sys crate already declares `antfly_db_sql_json`.
- Prepare a Laya checkpoint (`scripts/prepare_laya.py`) and smoke-test
  `Inference::decide`; record cold-load and warm latency.
- Define schemas, indexes, and enrichments for the data model above.
- Verify Antfly Cloud's proxy (`colony/go/pkg/colony/cloud/proxy.go`) passes
  `/db/v1/sql` and `/db/v1/transactions/*`; both fall through
  `handleNonTableRequest`, where read-only keys cannot `POST` (so cannot run
  SQL reads).

### Phase 1: `AntflyThreadStore` (embedded backend)

- Scaffold both crates, the `Backend` trait, and the `Embedded` backend.
- Implement the full `ThreadStore`, using `InMemoryThreadStore` for structure
  and `LocalThreadStore` for exact semantics.
- `DualStore` (dev only): writes to `LocalThreadStore` and `AntflyThreadStore`
  and diffs every read. Run real sessions through it until clean; keep it as a
  regression harness for upstream changes (for example turn lineage #51415 and
  partial answers #51260).
- Gate: `type = "antfly"` passes the thread-manager and app-server thread
  suites.

### Phase 2: Remaining seams

- `AntflyQueueStore`, `AntflyAgentMessageBoard`, `AntflyAgentGraphStore`,
  `AntflyMemoriesBackend`.

### Phase 3: `StateRuntime` on Antfly

- Port method groups in order: goals, memories (jobs with lease/ownership
  semantics via transactions), guardian feedback, polluted flags, shell
  snapshots, remote control, external-agent imports. Drop backfill and rollout
  migration. Route logs to `tracing` files.
- Gate: with a fresh `CODEX_HOME`, new/resume/fork/archive/search/subagent
  sessions, goals, and memories work, and no `*.sqlite` or rollout JSONL
  files exist afterwards.

### Phase 4: Laya approvals

- Install `LayaApprovalReviewer` in shadow mode (log decision, return `None`).
- Calibrate thresholds against actual user choices, then enforce `Allow`
  above threshold and `Deny` for destructive actions only.

### Phase 5: Migration

- One-shot importer from an existing `CODEX_HOME` (SQLite read-only plus
  rollout JSONL) into Antfly, including the `rollout_path → thread_id`
  mapping. This is the last time SQLite is read.

### Phase 6: Remote backend

- `Remote` backend over `antfly-sdk`; run the `DualStore` harness against it.
- Batch writes per turn or at `flush_thread`; never block a turn on the
  network.
- Embedded working copy plus remote replica: a local `.aflite` remains the
  device store and an outbox replicates to the remote instance, giving offline
  operation and cross-machine search. Antfly has no built-in Lite→server sync,
  so the outbox lives in `codex_antfly_store`.
- Key machine-local state (shell snapshots, project roots, worktree paths,
  attachments) by machine, or keep it local only.

## Operational constraints

- **Single writer (embedded).** Run Codex through the app-server daemon
  (the TUI probes its socket, or use `--remote`) so one process owns the
  `.aflite` file. Other processes fail fast rather than opening a second
  writer. The remote backend removes this constraint.
- **In-process inference.** A GPU driver fault terminates the host process and
  device calls cannot be preempted. Run the approval model on CPU initially.
- **Linking.** The binary needs `libantfly` at runtime (`ANTFLY_LIB_DIR`,
  pkg-config, or `zig/zig-out/lib` in an Antfly checkout). Build with
  `--features antfly-embedded/libantfly`; set an rpath for local builds.
- **Bazel.** New crates need `BUILD.bazel` (`codex_rust_crate`) only if the
  Bazel CI is used; Cargo builds without it.
- **sqlx stays compiled.** The SQLite code remains in the build but no files
  are created; removing it requires `cfg`-gating `codex-state`.
- **Privacy (remote).** Transcripts contain code and tool output that may
  include secrets. Decide what is mirrored, redact or truncate tool output,
  and keep the API key in an environment variable.

## Rebasing

- `thread-store/` and `state/` change frequently upstream (about 15 commits
  per week to `thread-store/` in early October 2026). Keep upstream edits to
  the patch points above.
- Every new upstream `state` migration or `StateRuntime` method needs a
  matching Antfly port; the `DualStore` harness and the fresh-`CODEX_HOME`
  gate catch drift.
- Branch: `antfly/integration`, tracking `upstream/main`.
