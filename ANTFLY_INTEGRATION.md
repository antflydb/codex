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
- **Storage model.** Each kind of state has its own Antfly table
  (`codex-rs/antfly/src/schema.rs`), in one embedded file or on one server:
  - *Relational tables* (`codex_threads`, sections, projects, attachments,
    spawn edges, the paginated-history projection, goals, the queue, memory
    stage outputs and jobs, guardian feedback, remote control, imports,
    backfill, rollout migration, the agent message board) are SQLite's schema
    ported column for column and accessed through `codex_antfly::sql` with
    PostgreSQL-style SQL. `codex_threads` is the single source of thread
    metadata for both `AntflyThreadStore` and `StateRuntime`. SQLite's
    triggers, `AUTOINCREMENT` revisions, and BLOBs are maintained by the
    stores; compare-and-set uses `UPDATE … RETURNING` in READ COMMITTED
    transactions retried on SQLSTATE 40001.
  - *Document tables* hold the text Codex searches: `codex_history_items`
    (raw rollout items, `{thread}:{ordinal}`), `codex_approvals`, and
    `codex_memory_notes`, each with declared filter fields and full-text plus
    (with an embedder) dense indexes over `search_text`.
  - Migrations are idempotent statement lists recorded in
    `codex_schema_migrations`; Antfly DDL is not transactional.
  - On the Antfly backend `StateRuntime` has no SQLite database: its pool
    fields point at a file that is never created, so a missed path fails
    loudly. Logs are a no-op sink.
- **Backends.** `Embedded` runs `libantfly` in-process: document calls on
  8 MiB-stack executor threads, SQL through the Antfly SQLx driver on its own
  connections to the same file (libantfly queues writers across handles).
  `Remote` uses HTTP for document tables (`/db/v1/tables/{t}/batch`,
  `/documents`, `/query`, `/indexes/{name}`) and the server's PostgreSQL wire
  listener (`sql_url`, through `sqlx-postgres`) for relational tables, with
  the same SQL. Config: `path` → embedded; `url` + `sql_url` → remote. The
  replicated (outbox) backend was removed with the move to SQL tables.
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
  in `codex_approvals` for calibration and as precedents.

### Antfly findings

- `libantfly.dylib` used to export `___dso_handle`, libc, compiler-rt, and
  internal symbols, which broke linking with aws-lc (antflydb/antfly#1023).
  Fixed in Antfly `8a8f338623` (#1022): the export trie now holds exactly the
  98 public `antfly_*` functions, and `codex_antfly` no longer needs its
  `___dso_handle` alias. Build `libantfly` from that commit or later.
- #1008 broke Laya decisions twice over: it rejected the checkpoint's
  `position_embedding_type: "absolute"` with `UnsupportedModernBertConfig`,
  and it misnamed Laya-format head weights (`MissingWeight`, which also hit
  OpenDecider-nano). Both are fixed on Antfly main (#1024, #1030,
  `6ade0769b4`). Against that build, Laya and OpenDecider-nano both classify
  `git log` as `none` and `rm -rf /` as `destructive`. Build `libantfly` from
  that commit or later.
- Found while moving to tables, fixed on Antfly main: multi-table catalog,
  catalog DDL and `antfly_search` in embedded SQL (#1034, #1044 → #1037,
  #1053), independent writers on one file (#1042), boolean partial-index
  predicates and a ~30 s `antfly_db_close` stall stopping embedded inference
  (#1054, #1055, both fixed by #1056), and EmbeddingGemma 2 pulls (#1043). Open:
  antflydb/antfly#1057 lists the SQL gaps the stores work around (`CHECK
  … IN`, `strpos`, `LIKE … ESCAPE`, correlated scalar subqueries, `54000` on
  derived tables, a misreported NOT NULL error, and 2 MiB statement and
  transaction limits that cap guardian records below SQLite's 8 MiB).
- Embedded `filter_prefix` takes the plain prefix string, not base64 as the
  OpenAPI `format: byte` suggests.
- A dense index created without `field` reads `embedding` and never indexes
  the enriched text (the Go test `TestLiteCAPILocalEmbeddedInferenceVariant`
  only checks that the enrichment drained). Pass `field` explicitly.
- Linking: binaries find `libantfly` through `ANTFLY_LIB_DIR` at build time
  and `DYLD_LIBRARY_PATH` (or an rpath) at run time.

### Status

All phases are implemented on `antfly/tables` (tables) on top of
`antfly/integration` (the original key-value layout).

| Phase | State |
| --- | --- |
| 0 Antfly prerequisites | Laya checkpoint prepared from a pinned revision, dense index + enrichment over `search_text`. The backend uses the embedded document, index and decision APIs already on Antfly main; `Database::sql_json` (antflydb/antfly#1032) turned out not to be needed |
| 1 Thread store | `AntflyThreadStore` over `codex_threads` and the relational thread tables plus `codex_history_items`: lifecycle with lazy materialization, Legacy and Paginated history (turns/items/realtime projected in the same transaction as items), `list_turns`/`list_items`/`list_timeline`, SQL keyset listing with local cursor formats, hybrid `search_threads`, literal `search_thread_occurrences`, sections (Pinned seeded), attachments, projects, fork/revert for both modes |
| 2 Other seams | Agent message board on relational tables; memories backend on `codex_memory_notes` (hybrid search, filesystem stays the source of truth) |
| 3 StateRuntime on Antfly | `StateRuntime::init_antfly`: every method (≈100, including the thread, project, section, attachment, backfill and rollout-migration methods that previously ran on an in-memory SQLite pool) uses the Antfly tables; no SQLite database exists |
| 4 Laya approvals | reviewer, latest-request capture, outcome recorder, calibrated verdicts (see above) |
| 5 Migration | `codex-antfly-import`: plans from rollout headers plus read-only SQLite metadata, flattens fork/revert lineages, streams one thread at a time, chunked writes, `--since`/`--limit`/`--dry-run`/`--replace`/`--search` |
| 6 Remote | HTTP document tables plus SQL over the PostgreSQL wire (`sql_url`); the replicated backend was removed |

Acceptance gate: `app-server/tests/suite/v2/antfly_thread_store.rs` runs a fresh
`CODEX_HOME` through thread start, a turn against a mock model, listing, and
deletion, and asserts no `*.sqlite` files and no rollout directories appear.

Known limitations:

- antflydb/antfly#1015 (a transient `WouldBlock` in full-text catch-up
  killing the derived worker, after which every write failed with
  `ANTFLY_INTERNAL`) no longer reproduces on Antfly `84dfbf5a95`: 0 errors
  with 32 and 64 concurrent databases, and the Antfly suites pass at normal
  test parallelism. Build `libantfly` from that commit or later. Tests close
  databases with `Antfly::close()` before removing their directories.
- Approval review defaults to `off`. Laya's zero-shot probabilities are only
  moderately separated; collect shadow-mode decisions and outcomes (stored
  in `codex_approvals`) before enforcing, and consider fine-tuning on them.
- The importer was exercised on a subset of a 28 GB, 1,245-thread history; a
  full import takes hours on a debug build. Imported forks and reverts are
  flattened into self-contained threads.
- The SQL-over-PostgreSQL-wire path of the remote backend compiles and has a
  test (`antfly/tests/remote.rs`, set `ANTFLY_TEST_URL` and
  `ANTFLY_TEST_SQL_URL`) but has not yet been run against a live
  `antfly standalone`; Antfly Cloud's proxy does not expose the PostgreSQL
  listener.
- Guardian review records are capped by Antfly's 2 MiB statement limit
  (antflydb/antfly#1057), well below SQLite's 8 MiB.
- Building requires `ANTFLY_LIB_DIR` pointing at a `libantfly` built from
  Antfly main at `551b8b3895` (C ABI version 3, `zig build capi`); CLI binaries and `codex-core` test
  binaries embed it as an rpath (the fs sandbox helper re-execs the binary
  with `DYLD_LIBRARY_PATH` stripped). Other crates' test binaries still need
  `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`.
- `agent::control::tests::ensure_v2_child_loaded_preserves_evicted_parent_authority`
  overflows the default 2 MiB test-thread stack on upstream `3342ee8c07` as
  well; run `codex-core` unit tests with `RUST_MIN_STACK=16777216`.
- The workspace `Cargo.toml` takes `antfly-embedded` from
  `github.com/antflydb/antfly` pinned to `551b8b3895` (with the `sqlx`
  feature); bump `rev` together
  with the `libantfly` build.

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
    "model": "convaiinnovations/laya",
    "input": "…",
    "questions": [
      {"name": "effect", "type": "choice", "instructions": "What is the worst effect of running this action?",
       "choices": [{"value": "none", "description": "Only reads or reports information"},
                   {"value": "local", "description": "Creates or modifies files inside the project"},
                   {"value": "external", "description": "Sends data over the network or changes things outside the project"},
                   {"value": "destructive", "description": "Deletes data, rewrites history, or breaks the system"}]},
      {"name": "intent", "type": "choice", "instructions": "Does the action match what the user asked for?",
       "choices": [{"value": "match", "description": "Directly serves the request"},
                   {"value": "drift", "description": "Unrelated or broader than asked"}]}
    ]
  }
  ```

  This is the OpenAI-style decisions contract Antfly adopted after
  `6ade0769b4` (`docs/guides/decisions.md`): `input`, a named `questions`
  array, `predicate` instead of `noul`, and an `answers` array whose choice
  answers carry `probabilities: [{value, probability}]`.

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
# url = "https://<host>/cloud/v1/<instance_id>"   # remote documents (HTTP)
# sql_url = "postgres://codex:secret@<host>:5432/antfly"  # remote SQL tables
# api_key_env = "ANTFLY_API_KEY"
models_dir = "~/.antfly/inference/models"
decide_model = "convaiinnovations/laya"

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
