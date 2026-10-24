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
  8 MiB-stack executor threads, SQL through the Antfly SQLx driver on the
  same `Database` handle (`with_database`). Independent handles on one file
  also work and, since antflydb/antfly#1080, share one storage owner without
  lease-handoff overhead (#1075) or spurious `FileBusy` (#1076).
  `Remote` uses HTTP for document tables (`/db/v1/tables/{t}/batch`,
  `/documents`, `/query`, `/indexes/{name}`) and the server's PostgreSQL wire
  listener (`sql_url`, through `sqlx-postgres`) for relational tables, with
  the same SQL. Config: `path` → embedded; `url` + `sql_url` → remote. The
  replicated (outbox) backend was removed with the move to SQL tables.
- **Search.** `search_threads` unions full-text matches with the top semantic
  neighbors (dense index over the `search_text` field, embedded by Antfly
  inference with `google/embeddinggemma-2` by default; Antfly detects its
  768 native dimensions and applies the `RETRIEVAL_QUERY` role to searches and
  `RETRIEVAL_DOCUMENT` to writes), then sorts and pages by the requested key with the local store's
  cursor format. The index name carries a hash of the model (and dimensions, if set),
  so changing `embedder_model` builds a new index; the embedded backend drops
  the old one.
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
  `shadow`, so verdicts and outcomes accumulate without changing what the
  user sees. In `enforce` mode only denials are applied, unless
  `approvals.apply_allow = true`. Decisions and observed tool outcomes are
  stored in `codex_approvals` for calibration and as precedents.

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
  (#1054, #1055, both fixed by #1056), and EmbeddingGemma 2 pulls (#1043). The SQL gaps the first port worked around (`CHECK … IN`,
  `strpos`, `LIKE … ESCAPE`, correlated scalar subqueries, `54000` on derived
  tables, a misreported NOT NULL error, 2 MiB statement and transaction
  limits; #1057) are fixed by #1060. The PostgreSQL listener now accepts
  sqlx-postgres's startup parameters (#1062, fixed by #1065). Standalone DDL over the PostgreSQL listener
  (#1067, #1070), prepared statements across DDL, and readiness (#1071,
  #1073) are fixed.
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
- Approval review defaults to `shadow`. Laya's zero-shot probabilities are
  only moderately separated. Enforce only after `codex knowledge benchmark`
  shows the deny precision you need on logged outcomes, and consider
  fine-tuning on them (`antfly inference finetune train laya`).
- The importer was exercised on a subset of a 28 GB, 1,245-thread history; a
  full import takes hours on a debug build. Imported forks and reverts are
  flattened into self-contained threads.
- The remote backend works end to end against `antfly standalone` from
  Antfly main `22eebec31a` (#1062, #1067, #1071 fixed): from a fresh server,
  `antfly/tests/remote.rs` migrates every table over the PostgreSQL listener
  with sqlx's default prepared-statement cache, round-trips SQL, and writes,
  scans, and searches the document tables over HTTP. Run it with
  `ANTFLY_TEST_URL`, `ANTFLY_TEST_SQL_URL`
  (`postgres://admin:<pw>@host:port/default`), and `ANTFLY_TEST_API_KEY` (an
  API key's `encoded` value). The migration runner follows the server's
  publication protocol (retry `53300`; after `55000`, poll for the new
  table). Antfly Cloud does not expose the PostgreSQL listener yet
  (antflydb/colony#766).
- Building requires `ANTFLY_LIB_DIR` pointing at a `libantfly` built from
  Antfly main at `3f8eeafaaf` (C ABI version 3, `zig build capi`); CLI binaries and `codex-core` test
  binaries embed it as an rpath (the fs sandbox helper re-execs the binary
  with `DYLD_LIBRARY_PATH` stripped). Other crates' test binaries still need
  `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`.
- `agent::control::tests::ensure_v2_child_loaded_preserves_evicted_parent_authority`
  overflows the default 2 MiB test-thread stack on upstream `3342ee8c07` as
  well; run `codex-core` unit tests with `RUST_MIN_STACK=16777216`.
- The workspace `Cargo.toml` takes `antfly-embedded` from
  `github.com/antflydb/antfly` pinned to `3f8eeafaaf` (with the `sqlx`
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

### End-to-end run (2026-10-13)

A release `codex` was run against a scratch `CODEX_HOME` on the embedded
backend. The run covered the importer, `codex exec`, and the app-server
(`thread/list`, `thread/search`, an approval turn, memories). Antfly-side
findings are tracked in antflydb/antfly#1126 (fixed by #1127).

Fixed in Codex during the run:

- **NUL in captured output.** Command output with `U+0000`, stored inside a
  JSONB item, failed every write of that item (and so the whole thread
  import). Antfly rejects NUL in `TEXT`/`JSONB` by design (SQLSTATE `22P05`
  since #1127). `codex_antfly::sql` now replaces NUL with `U+FFFD` in text and
  JSON parameters, including object keys.
- **Importer timestamps.** Threads without SQLite metadata took the import
  time as `updated_at`, so memories treated old sessions as active. The
  importer now falls back to the rollout's `SessionMeta` timestamp for
  `created_at` and the file's mtime for `updated_at`.
- **Importer linking.** `codex-antfly-import` embeds the `libantfly` rpath
  (`build.rs`), like the CLI.
- **Shadow reviews of escalated commands.** These set
  `require_fresh_review`, so the reviewer skipped them and
  `codex_approvals` stayed empty. See the reviewer section below.
- **Memories stage 1 on Antfly-native threads.** Stage 1 read rollout files,
  which Antfly threads don't have. When no rollout file exists, it now loads
  history from the thread store (`ThreadManager::load_thread_history`). That
  path can't use `load_history`, which upstream rejects for Paginated
  threads; resume uses the latest model context instead. A defaulted
  `ThreadStore::load_full_history` returns every raw item in either mode. The
  Antfly store serves it from `codex_history_items`; other stores fall back
  to `load_history`, and their Paginated threads keep rollout files.

- **Approval outcomes.** A declined approval ends the tool call normally,
  so `on_tool_finish` couldn't tell it from a failed command. Command items
  don't help either: for unified exec, the app-server synthesizes the
  `declined` item from the client's answer, and core never emits one. A new
  defaulted `ToolLifecycleContributor::on_approval_resolved` hook reports
  every resolution (hook, reviewer or user) with the call id and
  `ReviewDecision`. Core calls it from `Session::request_approval`. The
  recorder stores `decision` and `decided_by`, and labels outcomes
  `executed`, `executed_failed`, `declined`, `blocked`, `failed` or
  `not_executed`.
- **Import speed.** The projection read each turn and item before writing
  it, one statement at a time: about 4 SQL statements per rollout item.
  `ProjectionCache` reads each row at most once per batch. Imports skip
  reads entirely, because the thread's projection starts empty. Rows are
  written as 64-row upserts. The embedded pool no longer pings on acquire.
  A 3,586-item thread went from 61.8 s to 16.2 s, or 6.6 s without the dense
  index. The rest is Antfly: SQL recovery on every statement, and background
  enrichment holding the connection gate while it embeds
  (antflydb/antfly#1130).

  antflydb/antfly#1135 addresses both. Importing frozen copies of a 16,370-item
  and a 72,669-item thread (release importer, fresh database each run):

  | libantfly | 16k thread | 72k thread | Total | Index drain after |
  | --- | --- | --- | --- | --- |
  | pin `2c8240e703` | 47.3 s | 261.6 s | 309 s | 0.3 s |
  | main `0cba59bceb` | 55.4 s | 287.9 s | 344 s | 0.2 s |
  | #1135 `3eeb0f5dc5` | 20.7 s | 82.9 s | 104 s | 11.1 s |

  #1135 imports 3.3× faster than its base. The resulting rows are identical
  and search works. Background enrichment now defers while writes continue
  and catches up afterwards: 11 s of index work remains when the import
  exits. #1135 also regressed commits of transactions that insert under a
  foreign key and then delete through a subquery (`42P01` on `COMMIT`,
  antflydb/antfly#1141, fixed by #1142). Unbounded index lag under steady
  writes (antflydb/antfly#1139) is addressed by #1140. The pin
  (`968f037c9a`) includes all three.

Remaining gaps:

- **V8 for release builds.** A plain `cargo build --release` leaves out
  `codex-code-mode-host`, and shell tools then fail closed. Its `v8` build
  falls back to a download URL that 404s. Build packages with
  `just assemble-codex-package`, which fetches and verifies the matching
  `rusty-v8-v<version>` artifacts from openai/codex. Alternatively, set
  `RUSTY_V8_ARCHIVE` and `RUSTY_V8_SRC_BINDING_PATH` yourself, as
  `.github/actions/setup-rusty-v8` does.
- **Upstream: memory requests rejected after the 2026-10-14 merge.** Since
  openai/codex#53831, a Responses Lite model (the default extraction model
  `gpt-5.6-luna`) gets its base instructions as an input fragment carrying
  `content_item_metadata`. The ChatGPT-account endpoint rejects that with
  `Unknown parameter: input[0].internal_chat_message_metadata_passthrough.content_item_metadata`,
  so stage 1 fails on this setup. Normal turns are unaffected.
  `sample_structured` (claim extraction) avoids the fragment: it sends its
  instructions as a plain developer message and leaves `base_instructions`
  empty (`Prompt::default()`'s are Codex's full agent prompt).
- **Stage 1 eligibility.** `memories.min_rollout_idle_hours` defaults to 6
  and is clamped to at least 1, so stage 1 never processes a recently active
  thread. This is by design for `MEMORY.md`. Claims no longer depend on it;
  see [Where claims come from](#where-claims-come-from).

### Approval reviewer (`codex_antfly_extension`)

`LayaApprovalReviewer` implements `ApprovalReviewContributor`
(`ext/extension-api/src/contributors.rs:410`), the seam Guardian V2 uses, and
is registered before `codex_guardian_v2::install`
(`app-server/src/extensions.rs:96`).

- Apply Guardian V2's guards first (`ext/guardian-v2/src/async_scorer/approval.rs`):
  allow on `full_access`; return `AskUser` when the reviewer is `User` or the
  policy is not `OnRequest`/`Granular` unless `require_guardian`; never fast
  allow when `require_fresh_review`. Escalated shell commands always set
  `require_fresh_review`, so the reviewer still runs and records these
  (`fresh_review_required: true`). In enforce mode a `Deny` is applied. An
  `Allow` is left to Guardian and the user unless `approvals.apply_allow` is
  set, and even then never when a fresh review is required.
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
path = "~/.codex/antfly.aflite"      # embedded (the default without `url`)
# url = "https://<host>/cloud/v1/<instance_id>"            # remote documents (HTTP)
# sql_url = "postgres://codex:secret@<host>:5432/default"  # remote SQL tables
# api_key_env = "ANTFLY_API_KEY"
models_dir = "~/.antfly/inference/models"
decide_model = "convaiinnovations/laya"
# embedder_model = "google/embeddinggemma-2"  # default
# embedder_dims = 256                 # optional Matryoshka truncation; omit for native size
# semantic_search = false            # full text only, no embedder needed
# knowledge_graph = false            # skip concept extraction and the knowledge graph
# extract_model = "fastino/gliner2.5-base-v1"  # default
# concept_screening = "shadow"       # off | shadow (propose generic concepts; default) | enforce (ignore them)
# supersede_threshold = 0.8          # decide-model confidence before a claim supersedes another

[experimental_thread_store.approvals]
mode = "shadow"                      # off | shadow (default) | enforce
allow_threshold = 0.7
deny_threshold = 0.8
# apply_allow = false                # enforce mode: also apply confident allows
# model = "fastino/GLiNER2.5-Decide-1B"  # approval model, if not decide_model
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
| `app-server/src/request_processors/thread_processor.rs` (resume, revert) | The rollout path is optional: Antfly threads have none and are read back from the thread store; client `thread/resume` used to fail with "rollout path missing" |
| `state/src/runtime.rs` (and `runtime/*.rs`) | Swap SQLite internals for the Antfly backend behind the existing method signatures |
| `Cargo.toml` (workspace) | New members; `antfly-embedded` and `antfly-sdk` dependencies |
| `thread-store/src/store.rs` | Defaulted `ThreadStore::load_full_history` (raw items in either history mode) |
| `ext/extension-api/src/contributors.rs`, `contributors/tool_lifecycle.rs`; `core/src/tools/approvals.rs` (`notify_approval_resolved`) | Defaulted `ToolLifecycleContributor::on_approval_resolved`, called for every approval resolution |
| `core/src/thread_manager.rs` (`load_thread_history`), `memories/write/src/phase1.rs`, `memories/write/src/runtime.rs`, `memories/write/src/rollout_input.rs` | Stage 1 reads history from the store when a thread has no rollout file |
| `memories/write/src/detached.rs` (`sample_structured`) | One structured request to the memory extraction model, made like stage 1's, for the claim pipeline |
| `core/src/prompt_debug.rs` (`start_standalone_thread`) | Thread manager plus one thread for standalone commands, factored out of `build_prompt_input` |
| `cli/src/knowledge_cmd.rs`, `cli/src/main.rs` | `codex knowledge status/backfill/consolidate` |

`LocalThreadStore` downcasts that become irrelevant once `state_db` is always
Antfly-backed or the store is non-local: `core/src/session/session.rs:1139`,
`core/src/thread_manager.rs:823`,
`app-server/src/request_processors/thread_processor.rs:5462`,
`app-server/src/request_processors/rollout.rs:14`,
`thread-store/src/live_thread.rs:188`, `:395`. Audit each when implementing
`resume_thread` and session init.

## Knowledge graph

Status: claims, concepts, supersession, retrieval and the `knowledge.search`
tool are implemented; see "Not built yet" for the rest. Findings were made
against Antfly 1258e8cfcb; the pin is now `3f8eeafaaf`, which includes
antflydb/antfly#1096 (fixes for #1089 and #1084), #1111 (query-time
resolution, durable merges), #1114 (resolver ownership, shadow index
replacement), #1127 (NUL diagnostics, statement context in errors), #1135
(cheap SQL recovery, background maintenance outside the connection gate),
#1142 (foreign-key commits after a refresh), #1140 (maintenance progress
under steady writes), #1145 (graph replacement with an inline resolver) and
#1158 (inline-resolver enrichment settles, explicit `models_dir` in
background maintenance, no handle-reopen churn) and #1163 (enrichment and
resolution no longer stall under writes with an inline resolver), #1186
(generated deciders, reversible curation and the entity APIs) and #1223
(schema safety, resolver credential redaction) and #1233 (cascade
suppression, curation provenance and the entity proposal queue) and #1246
(promotion after curation, cross-table traversal starts) and #1254 (Lite
grouped sources advance in the background).

### Goal

Capture ideas and knowledge across sessions and how they relate, not code
structure (ASTs and language servers already cover that). The graph answers
"what did we decide about X, why, and is that still true?": decisions,
rationale, preferences, lessons, constraints and open questions, and how they
evolve. For example, this fork's embedder went bge-small → Qwen3 →
EmbeddingGemma 2. The graph returns the current choice and the chain of
replacements.

### Model

- **Claims** (`codex_knowledge`, `codex_antfly::knowledge`). Typed
  propositions: `decision`, `rationale`, `preference`, `lesson`, `constraint`,
  `open_question`. Each holds one self-contained statement, its thread, memory
  version, `cwd`, `source_updated_at_ms`, a status (`current` / `superseded`),
  `supersedes` and `superseded_by`. Ids are `<version>:<thread>:<index>`, and a
  thread's claims are replaced as a whole when it is re-extracted.
- **Concepts.** Canonical `concept/<slug>` nodes minted by an Antfly resolver.
  Codex's `knowledge::concept_key` reproduces Antfly's `slug` exactly, so
  query-time concepts land on the same keys.
- **Memory notes** (`codex_memory_notes`) link to the same concepts.

Each knowledge table carries two graph indexes:

| Index | Edges | Source |
|---|---|---|
| `codex_concepts_<hash>` | `about` (doc → concept); `part_of`, `alternative_to`, `uses`, `depends_on`, `replaces`, `causes` | GLiNER `extractor` enrichment over `search_text`, plus a resolver |
| `codex_claim_links` | `supersedes` (claim → claim) | the claim's `supersedes` field (`edge_types[].field`) |

The concept index name hashes the extraction model and label schema, so
changing either builds a new version. On embedded databases the new version
replaces the old one atomically (`add_index` with `replaces` and a 250 ms
`wait`): the old index keeps serving, and `Backend::serving_graph` routes
queries to it, until the replacement swaps in. A wait that times out leaves the
build to background maintenance and the next open re-sends the identical
request. A failed build is cancelled and retried on the next open; a
`REPLACEMENT_CONFIGURATION_MISMATCH` cancels the stale pending build and
retries once. The `concepts` resolver keeps its name across versions, so its
merges and fences carry over. Remote backends create the new version and leave
stale ones in place. The
split is required: Antfly rejects a field-derived edge type on an index that
also has an artifact source. It also means a concept rebuild never loses
supersession history.

### Where claims come from

Claims are extracted per turn by the claim pipeline in
`codex_antfly_extension` (`ext/antfly/src/claims/`), not by stage-1 memory
extraction.

**Why not stage 1.** Stage 1 summarizes a whole thread for `MEMORY.md`, and
upstream gates it on idleness: a thread is only eligible once it has been
idle for `memories.min_rollout_idle_hours` (default 6, clamped to 1–48).
Stage 1 also runs only when another session starts a turn, and claims at most
`max_rollouts_per_startup` (default 2) threads per run. The gate is right for
consolidation: it avoids summarizing a moving target and re-running a full
summary on every change. But it left the knowledge graph hours behind, and
empty for the session that produced the knowledge. Relaxing the gate would
only drop its protection. Instead the two consumers are separated:

- **Stage 1 is unchanged from upstream** apart from reading history from the
  thread store for threads without a rollout file. It stays the only path to
  `MEMORY.md`, and no longer emits claims. Moving claims out entirely, rather
  than keeping stage 1 as a backstop, avoids deduplicating two sources of the
  same propositions and shrinks the patch to upstream's memory code.
- **The claim pipeline owns claims.** It extracts finished turns shortly
  after they end. The idle gate's protection comes from settling instead:
  claims from an active session are provisional, so they rank lower and
  cannot supersede other threads' claims until the session settles.

**Pipeline.** Per thread:

1. **Trigger.** When a turn ends or aborts, a debounce timer starts
   (`claim_debounce_seconds`, default 120). If 4 finished turns are waiting,
   extraction runs at once. A thread that stops is extracted and settled
   immediately. A sweep, 60 seconds after the first thread starts in a
   process, queues up to 16 recently updated threads (within
   `memories.max_rollout_age_days`) with unextracted history or unsettled
   claims. That covers crashes, processes that exit during the debounce, and
   imported threads.
2. **Lease.** One worker per process handles jobs one at a time. It leases the
   thread in `codex_claim_progress` (15-minute lease) so two processes never
   extract the same thread.
3. **Read.** It reads `codex_history_items` from the thread's watermark
   (`next_ordinal`) onward, in pages of 2,000 items, and groups them into turns
   by their `task_started`/`task_complete`/`turn_aborted` events. Only finished
   turns are processed; a settling thread also takes its unfinished tail. The
   transcript keeps user and assistant text, tool calls and truncated tool
   outputs. It drops developer messages, host-injected context (environment,
   AGENTS.md, skills) and reasoning, and redacts secrets with
   `codex_secrets::redact_secrets`.
4. **Extract.** Turns are batched up to 60,000 characters (an oversized turn
   keeps its head and tail). Each batch goes to the memory extraction model
   (`memories.extract_model`, else the provider's preferred one) through
   `codex_memories_write::sample_structured`, which makes the request exactly
   as stage 1 does. The input lists the thread's current claims with short ids
   (`c1`…, up to 40) and labels the new turns `[turn N]`. The strict output is
   `{claims: [{turn, kind, statement, replaces}], retracts}`. The model returns
   only new or changed claims: `replaces` names the existing claims a new one
   updates, and `retracts` names ones that no longer hold. Unknown ids and
   kinds are dropped, and at most 8 claims are accepted per batch.
5. **Record.** `Knowledge::append_turn_claims` writes provisional claims keyed
   `turns:{thread}:{turn ordinal}:{nnn}`, with `turn_id` and `turn_ordinal`. It
   marks replaced claims `superseded` (with `superseded_by`) and retracted ones
   `retracted`. Replacements and retractions within a thread need no decide
   model, because the extractor saw the earlier claims. On a thread's first
   batch, any earlier stage-1 claims of that thread are deleted. The watermark
   then advances past the batch, so a crash repeats at most one batch.
6. **Settle.** After an extraction, a settle timer starts
   (`claim_settle_minutes`, default 30). A newer turn's timer replaces it, and
   a stopping thread settles at once. `Knowledge::settle_thread` compares each
   current provisional claim with similar settled claims from other threads
   (see Supersession) and marks the thread's claims `settled`.

**Failures.** A job that fails releases its lease with a retry time: 5 minutes,
doubling per failure up to a day. If no thread is loaded to attribute the
model request to, the job is dropped without penalty and the next turn or
sweep retries it. A single job makes at most 16 model requests; a longer
backlog continues on the next trigger.

**Cost.** One model request per batch of finished turns (usually one per
debounce window), against one per thread for stage 1. Requests use stage 1's
reasoning effort (`low`) and the same detached request metadata.

**Deletion.** Claims are no longer kept per memory version. Forgetting a
thread's memory (`delete_thread_memory`) deletes its claims, and
`clear_memory_data` deletes them all; extraction progress is kept, so cleared
history is not re-extracted. Retention pruning of stage-1 outputs does not
touch claims.

**Configuration** (`[experimental_thread_store]`, with the knowledge graph on):

```toml
claim_extraction = true        # default; false leaves knowledge.search with notes only
claim_debounce_seconds = 120   # quiet period after a turn
claim_settle_minutes = 30      # idle time before claims settle
```

Extraction also requires `memories.generate_memories` (default on), a
non-ephemeral interactive session (`cli`, `vscode`, and the other
`INTERACTIVE_SESSION_SOURCES`), and a thread whose `memory_mode` is `enabled`
(checked before every job).

### Backfill: `codex knowledge`

The live pipeline reaches threads that run turns, plus a startup sweep of at
most 16 threads updated within `memories.max_rollout_age_days`. Imported and
older history never gets claims that way, so a command reaches it on request:

```sh
codex knowledge status                  # threads seen/settled/pending, claims by state, concepts
codex knowledge backfill --dry-run      # what is pending, and at most how many requests
codex knowledge backfill [--since 2026-09-01] [--thread ID]... [--limit 25] [--max-requests N]
codex knowledge consolidate             # apply aliases and run a concept-merge pass now
```

It drives the same worker as the live pipeline (`ext/antfly/src/claims/admin.rs`):

- **Leases and progress:** the same leases and `codex_claim_progress` rows,
  so a run is resumable and never extracts a thread twice.
- **Eligibility:** interactive sources and `memory_mode = 'enabled'`, newest
  first. `--thread` selects threads whatever their age or source.
- **Settling:** backfilled history is old, so each thread is settled as soon
  as it is extracted, which runs cross-thread supersession.
- **Cost:** `--dry-run` reads only. It counts pending history items, and
  pending turns from the turn projection. One extraction request covers one
  or more turns (up to 60,000 characters), so the turn count bounds the
  requests. `--max-requests` stops the run when the budget is spent; rerun to
  continue.
- **Attribution:** model requests are attributed to an ephemeral host thread
  (`codex_core::start_standalone_thread`, factored out of `codex debug
  prompt-input`), as live requests are attributed to a loaded thread.
- **Concurrent processes:** the command opens the embedded database itself.
  It can run while the app-server daemon has it open: Lite serializes
  writes per operation through its writer lease (waiting up to
  `BUSY_TIMEOUT`, 5 s).

Verified on the scratch database: `--max-requests 1` extracted one thread and
stopped, a rerun finished the other, and `status` then showed nothing pending.
The intended flow for existing history is `codex-antfly-import`, then
`codex knowledge backfill --since …`.

**Why Codex runs the backfill, not Antfly.** Antfly already backfills
everything it computes itself. When an index is added, rebuilt or replayed,
it runs over the existing documents:

- concept extraction on claims and notes;
- embeddings;
- graph edges;
- resolver promotion;
- merge replays.

Producing claims is not one of those things, for two reasons:

1. **Credentials and policy.** Extraction uses Codex's model client and the
   user's ChatGPT-account auth. Antfly has a ChatGPT connector
   (`zig/CHATGPT.md`), but it is scoped to explicitly initiated interactive
   generation on a local standalone server. It deliberately refuses to serve
   as a table's background-generation configuration, which a backfill
   enrichment would be. It isn't available in the embedded library either.
2. **Statefulness.** An Antfly enrichment computes one document's output
   from that document. Claim extraction works on a thread:
   - it groups many history items into turns;
   - it shows the model the thread's current claims so it can replace or
     retract them;
   - it settles claims across threads.

So Codex writes the claims, on request and under the user's own session.
Antfly's enrichments then pick them up as usual.

### Supersession

Supersession happens when a thread's claims settle, not when they are written.
A provisional claim from a session still in progress never displaces
established knowledge. Each current claim being settled is compared with up
to three similar `current`, settled claims of the same kind from other threads
(hybrid search). A later claim replaces an earlier one when it restates it,
or replaces, reverses or updates it:

- Statements identical after normalizing case, spacing and a final period
  are restatements; no model is asked.
- Otherwise one decide call asks two questions, and either is enough at
  `supersede_threshold` (default 0.8):
  - the predicate "the later statement replaces, reverses or updates the
    earlier statement about the same subject";
  - a relation choice (`same`, `updates`, `different`, `unrelated`), where
    `same` plus `updates` must reach the threshold and one of them must be
    the most likely.

Laya needs both questions. The predicate catches a changed choice, which the
choice question reads as a different fact: "use EmbeddingGemma 2 instead of
Qwen3" scores `different` 0.94 but predicate 0.88. The choice question catches
restatements, which the predicate misses: a reworded "1,024 items per batch"
scores predicate 0.08 but `same` + `updates` 0.81. A different fact about the
same subject ("the importer keeps threads of every age") stays below both, at
0.36 and 0.63. `supersession_collapses_restatements_but_keeps_distinct_facts`
pins these cases.

When a claim is replaced, the new claim lists the earlier one in
`supersedes`, and the earlier one becomes `superseded` with `superseded_by`
set. Failures are logged and never fail settling. The integration test's
"use EmbeddingGemma 2 instead of Qwen3" claim supersedes "use Qwen3" after
settling and leaves an unrelated preference alone. Within a thread, the
extractor's `replaces` and `retracts` take effect immediately.

**Ranking.** `knowledge.search` ranks provisional claims at half their score
and returns them with `provisional: true` and their `turn_id`, so the model can
weigh a claim from an unfinished session accordingly.

### Retrieval: `knowledge.search`

`Knowledge::related(query, limit, include_superseded)`:

1. **Seeds.** Resolve the query with Antfly's query-time resolution
   (`resolve_query_json` on the claims table's `concepts` resolver). It runs
   the graph's own extractor and resolver, aliases and merges included,
   without writing, so seed keys match write-time keys exactly. Backends
   without it (remote) fall back to local GLiNER extraction plus `concept_key`.
   Add the concepts of the query's direct hybrid-search hits among claims.
2. **Expand.** Traverse `about` edges inward from the seeds, in both the claims
   and memory-notes tables. `deduplicate_nodes: false` returns one path per
   (concept, document), so each result's shared concepts come from the same
   query. Everything goes through the normal search path, so embedded and
   remote behave the same.
3. **Rank.** Personalized PageRank on the concept index (`metrics: {"ppr":
   {"kind": "pagerank"}}`), seeded at the query's concepts with `direction:
   "both"` and `metric_freshness: "fresh"`. Mass flows concept → document →
   concept, so documents more than one hop away surface too. Direct search
   hits get a small boost. If the metric query fails, ranking falls back to
   the count of shared concepts. Superseded claims are dropped unless history
   is requested; superseded claims carry `superseded_by`.

4. **Links.** For each returned claim, a one-hop traversal of the claim
   links graph (`codex_claim_links_v2`, `direction: "in"`) collects the
   current claims that `motivates` it and the alternatives
   `rejected_in_favor_of` it.

`codex-antfly-extension` contributes the tool `knowledge.search(query,
max_results, include_history)` whenever the thread store is Antfly and the
knowledge graph is on. It returns current claims (with thread, turn, `cwd`,
`supersedes`, shared concepts, `motivated_by` and `rejected_alternatives`)
and related memory notes.

### Claim links

The extractor links claims to the decisions they explain:

- A `rationale`, `lesson` or `constraint` lists the decision it explains in
  `motivates`.
- An `alternative` (a claim kind for an option considered and not chosen)
  lists the decision chosen instead in `rejected_in_favor_of`.

Targets are existing claims of the same thread (by short id) or other claims
in the same answer (`n1`, `n2`, … by position). `append_turn_claims` resolves
them to claim ids and drops self-links and links to other threads. Both
fields become field-derived edges, next to `supersedes`, in the claim links
graph. Its name is versioned (`codex_claim_links_v2`, replacing
`codex_claim_links` in place), so changing edge types rebuilds it without a
gap.

### Concept consolidation (synonyms)

The resolver mints concept keys deterministically:
`concept/{{ uslug _entity.text }}`. `uslug` keeps non-ASCII names distinct
instead of collapsing them. So "ThreadStore" and "thread store", or "Antfly"
and "Antfly database", start as separate concepts. Merging them was measured
before it was built:

- **Embeddings alone can't do it.** Under EmbeddingGemma 2, version siblings
  score higher than most true synonyms: "EmbeddingGemma 2" and
  "EmbeddingGemma" 0.986, "Qwen3" and "Qwen2" 0.957, against "Antfly" and
  "Antfly database" 0.897. A cosine threshold in Antfly's resolver scorer
  would merge exactly the concepts that most need to stay apart. The scorer's
  lexical comparators can't require matching version numbers either.
- **Laya can't do it either.** Asked whether two names refer to the same
  thing, it rates "Antfly" and "Antfarm" 0.87 same and "Codex" and "Claude
  Code" 0.80, about as high as true synonyms.

So the resolver stays deterministic, and Codex merges afterwards through
Antfly's entity API (`curate_entity` with `merge`), falling back to
`merge_resolver_keys` per resolver on backends without it. One call
redirects the key in every resolver that writes concepts (claims and memory
notes) and replays affected extractions. A
consolidation pass (`ext/antfly/src/claims/concepts.rs`) runs after a thread
settles, at most every 10 minutes per database. For each promoted concept it
has not examined (`codex_concept_reviews`, migration 24):

1. **Lexical identity** (`concept_identity`): alphanumeric runs lowercased,
   plural `s` dropped, joined without separators. A concept with the same
   identity as an examined one merges into it with no model. Digits are kept,
   so versions never collide.
2. **Candidates:** the nearest examined concept by name embedding (a dense
   index on `codex_concepts.canonical_name`), with the same number tokens and
   a self-normalized similarity of at least `CONCEPT_SIMILARITY_FLOOR` (0.80).
   Queries and stored names are embedded with different roles, so scores are
   divided by the concept's score against itself.
3. **Judgment:** up to 40 candidate pairs per pass go to the memory
   extraction model in one request (`sample_structured`). It answers `same`
   only for synonyms, abbreviations, spellings and non-identifying
   qualifiers, not versions, parts, kinds or sibling products. It also says
   whether it is `sure`. A sure `same` is merged. An unsure `same` becomes a
   `merge` proposal in Antfly's queue (actor `codex/consolidation`, with
   both rendered names as evidence), for `codex knowledge accept` or
   `reject`. Rejecting it pins the pair distinct.

On the labelled pairs (`antfly/tests/fixtures/knowledge_eval/concept_pairs.jsonl`),
4 pairs merge lexically and 14 reach judgment (9 synonyms, 5 hard negatives).
Synonym recall before judgment is 0.93. The miss is the abbreviation "KG"
(0.73); abbreviations belong in configuration:

```toml
[experimental_thread_store.concept_aliases]
KG = "knowledge graph"
```

Aliases are rendered through the resolver (`render_resolver_key`) and merged
during the startup sweep; applied ones render to the same key and are
skipped.

**Upgrading the concept graph.** The concept graph's name hashes the
extraction model and a schema version, so changing either builds a
replacement graph in place. The switch from `slug` to `uslug` is schema
version 3. Since antflydb/antfly#1144 (fixed by #1145), a replacement takes
over the old graph's inline resolver, merges included. A key-template change
on a resolver with merges still needs an explicit namespace migration first.
`EmbeddedBackend` does it before replacing:
`set_resolver_namespace` with a `key_map` that maps every merge endpoint to
itself. `uslug` renders ASCII names to the same keys as `slug`, so the
redirects stay valid. The test `graph_upgrade_moves_resolver_and_keeps_merges`
checks this: a merge made under `slug` still resolves after the upgrade, and
"Café" keeps its accent. If Antfly does refuse a replacement, the old graph
keeps serving and table setup logs a warning instead of failing writes.

On a live database the replacement used to stall, through three Antfly bugs:
- Inline-resolver enrichment never went idle (antflydb/antfly#1159).
- An explicit `models_dir` broke background extraction (antflydb/antfly#1160).
- The enrichment worker went missing under writes (antflydb/antfly#1161).

#1158 and #1163 fixed them. At the current pin, a replacement under one write
per second swaps in about 20 s, and enrichment stays caught up afterwards.

Since #1158 the embedded tests extract for real. GLiNER needs about 2.4 GB of
inference live memory, so under memory pressure (other builds running, say)
loads can be refused with a retryable `MODEL_RESOURCE_BUSY` and these tests
can fail. Isolated reruns pass.

### Curation

Merges, unmerges and suppressions use Antfly's entity API (#1186), which
fans out to every resolver writing into `codex_concepts`:

| Command | Entity action | Effect |
|---|---|---|
| `codex knowledge merges` | resolver state | Lists redirects from every concept resolver |
| `codex knowledge unmerge <concept>` | `unmerge`, `pin_distinct: true` | Restores the concept and pins the pair distinct |
| `codex knowledge unmerge --allow-remerge <concept>` | `unmerge` | Restores it; a later pass may merge it again |
| `codex knowledge ignore <concept>` | proposal `:accept`, else `suppress` with `absorbed: suppress` | Mentions stop minting the concept and what was merged into it; their edges are removed |
| `codex knowledge keep <concept>` (or `unignore`) | proposal `:reject`, else `unsuppress` | Rejects the proposal, or reverses `ignore` for this concept only |
| `codex knowledge ignored` | resolver state with provenance | Lists ignored concepts and why: by hand, screened as generic (accepted or enforced), or ignored with the concept it was merged into |
| `codex knowledge proposals` | open proposals | Lists concepts generic screening proposes to ignore, and pairs consolidation was unsure were the same |
| `codex knowledge accept <concept>` | proposal `:accept` | Applies the open proposal for the concept (ignore or merge) |
| `codex knowledge reject <concept>` | proposal `:reject` | Rejects it; a rejected merge pins the pair distinct |

A concept argument is a `concept/…` key, or a name rendered through the
resolver (`:render`, whose `minted_key` is the key before redirects, so an
unmerge can name the merged-away concept), or a case-insensitive name match.

Consolidation reads the curation state (`Knowledge::concept_curation`) and
never proposes a pair that is pinned distinct, whether lexical twins, name
neighbors or description neighbors.

**Ignoring a merge target.** Suppression uses `absorbed: "suppress"`, so
the concepts merged into the target are suppressed with it in one commit.
Antfly records each as a `cascade` from the target. `unsuppress` restores
only the named concept: the absorbed ones stay suppressed and are no longer
merged.

**Provenance.** Codex writes an `actor` on curation: `codex/user` from
`codex knowledge`, and `codex/generic-screen` from screening, which also
writes a `reason`. Antfly adds the origin (`manual`, `review`, `cascade`,
`config`), time, revision, proposal id and `cascade_from`. `ignored` uses
these to explain each suppression.

**Generic concepts.** Extraction turns generic words in memory notes into
concepts ("note", "content", "add"). Each consolidation pass first screens up
to 40 unscreened concepts in one request to the extraction model, with their
descriptions. Each verdict is logged in `codex_decision_log` (kind
`generic_concept`), and a concept is screened once. `concept_screening`
chooses what happens next:
- `shadow` (default): each generic concept becomes a proposal in Antfly's
  entity proposal queue (`action: suppress`, `options.absorbed: suppress`,
  actor `codex/generic-screen`, with the judged name and description as
  evidence). `codex knowledge proposals` lists the open ones; `ignore`
  accepts one and `keep` rejects it. Antfly keeps the decision, so a
  rejected concept is not proposed again.
- `enforce`: generic concepts are suppressed right away, attributed to
  `codex/generic-screen`.
- `off`: no screening.

Shadow is the default because enforcing was too aggressive. On the E2E
database the first prompt hid 20 of 40 concepts, about 7 of them real
project concepts ("agent", "DNS resolution", "elevated sandbox
permission"). The tightened prompt counts only filler words (pronouns, bare
verbs, single everyday words) as generic and treats domain terms and
multi-word phrases as specific. It proposed 6 of the same 40: "add",
"assistant", "content", "creation", "delete" and "edit".

The remote backend maps the same operations to HTTP:
`POST /tables/{t}/entities/{key}:{merge|unmerge|suppress|unsuppress}`,
`GET /tables/{t}/entities/{key}/redirects`, `GET /tables/{t}/resolvers/{r}`,
`POST /tables/{t}/resolvers/{r}:render|:resolve` and
`POST /tables/{t}/resolvers/{r}/redirects`, and for proposals
`POST|GET /tables/{t}/curation/proposals` and
`POST /tables/{t}/curation/proposals/{id}:accept|:reject`. It retries 429
and 503 with backoff; a 404 means the backend cannot curate. A decision
reporting `complete: false` is retried until its fanout completes.
Curation writes and proposal decisions carry `wait: {timeout_ms: 10000}`.
Without it the server answers 202 before applying the change, and the
CLI's or consolidation's next read misses it.

Verified against `antfly standalone` in `remote_concept_curation`: entity
merges and unmerges with a distinct pin, cascade suppression with
provenance, and the proposal queue. Before antflydb/antfly#1246, any
runtime merge or suppression on the server stopped entity promotion for
`codex_concepts` (#1244).

### History concepts

Concepts are also extracted from conversation history, per turn, through
Antfly's grouped (fan-in) sources (antflydb/antfly#1147). The graph
`codex_history_concepts_<hash>` on `codex_history_items` reads two
table-level enrichments that Codex registers with it:

1. **`<graph>_turns`, a grouped source:** user and agent messages (items with
   `speaker` `user` or `agent`; tool output has neither) grouped by
   `thread_id`, ordered by `ordinal` and segmented by `turn_id`, one unit
   per turn.
2. **`<graph>_extraction`:** GLiNER over each unit, entities only.

The graph maps each unit's mentions through a third `concepts` resolver
into `codex_concepts`, with `nodes.source = thread/{{_doc.value._group_key}}`.
Every thread gets `about` edges to the same `concept/…` keys claims and notes
use. Merges and suppressions apply to it automatically, because entity
curation fans out to every resolver writing into `codex_concepts`.
`knowledge.search` returns `threads` alongside claims and notes: threads
whose turns are about the query's concepts, most shared concepts first
(`Knowledge::threads_about`).

Status:
- **Server:** works end to end (`remote_history_concepts`). Two turns of a
  thread yield `about` edges to `antfly`, `embedder`, `embeddinggemma`,
  `dns` and `macos`; the tool output's "Kubernetes" is excluded.
  `knowledge.related` returns the thread and a memory note on the same
  concept.
- **Lite:** works from ordinary writes since antflydb/antfly#1254 fixed
  #1241 (`history_concepts_link_threads_to_concepts`). Before that, grouped
  sources advanced only when a write waited at `sync_level: enrichments`.

**Traversal into concepts.** Concept nodes live in `codex_concepts`, not in
the table being queried, so traversals start from table-qualified
identities (`{"identities": [{"table": "codex_concepts", "key": …}]}`),
which both backends accept since antflydb/antfly#1246. Bare cross-table keys
used to find nothing on the server, so remote retrieval through concepts
silently returned no claims or notes (#1245). The server traverses incoming
edges only with node deduplication, so Codex runs one traversal per seed
concept to keep which concepts each document shares.

### Memory notes

`AntflyMemoriesBackend::reconcile` indexes notes whose files changed (by
modification time) and drops notes whose files are gone. Hidden entries and
files over 512 KiB are skipped. It runs in the background when a thread starts
and, at most every 30 s, before `memories.search`. Pipeline-written notes
therefore reach search and the concept graph without an agent reading them
first.

### What GLiNER does

GLiNER does not write claims. It links them:

1. **Concept extraction at write time,** as an Antfly enrichment on claims and
   memory notes.
2. **Concept→concept relations,** stable facts only. Relations between claims
   are judgments made by the decide model.
3. **Seed concepts at query time.**

Labels (`knowledge.rs`): `concept`, `technology`, `tool`, `project`,
`organization`, `person`, `goal`, `problem`, `practice`, `constraint`,
`metric`; relations: `part_of`, `alternative_to`, `uses`, `depends_on`,
`replaces`, `causes`.

The label set must be this wide. Antfly serves windowed GLiNER2.5 extraction
only inside its measured long-document profile, whose minimum padded sequence
(103 tokens) was measured with an 11-entity, 6-relation schema. A 6-entity,
3-relation schema made short claims fall below it, and every extraction failed
with `GlinerBoundaryPaddedSequenceLimitExceeded`. Classification is only
qualified on short (≤610-byte) inputs.

**Label descriptions** (`entity_definitions`) are now qualified for windowed
extraction on the FP32 base. They were evaluated before being adopted, with
Antfly's `scripts/eval_extract.py` on 22 hand-labelled claim statements (55
concept mentions, two negatives;
`antfly/tests/fixtures/knowledge_eval/concept_extraction.jsonl`):

| Config | Exact-span F1 (label-agnostic) | Overlap precision / recall | Labelled F1 |
| --- | --- | --- | --- |
| Bare labels | 0.69 | 0.91 / 0.74 | 0.37 |
| Descriptions | 0.64 | 0.87 / 0.84 | 0.32 |

Concept keys are label-free, so span quality is what matters. Descriptions
trade exact spans and precision for overlap recall, so the enrichment keeps
bare labels. The corpus is small; rerun with the fixture configs
(`extract_plain.json`, `extract_descriptions.json`) when it grows.

### Model roles

Model roles are separate settings, so collapsing them into one model later is
a configuration change, not a code change:

| Role | Setting | Default |
|---|---|---|
| Embeddings | `embedder_model` | `google/embeddinggemma-2` |
| Extraction | `extract_model` | `fastino/gliner2.5-base-v1` (long-document qualified) |
| Decide (supersession; approvals unless overridden) | `decide_model` | `convaiinnovations/laya` |
| Decide (approvals) | `approvals.model` | `decide_model` |
| Claim distillation | `memories.extract_model` | the memories pipeline's model |

**Why not one GLiNER2.5 Decide model for both, yet.** Antfly gates every
extraction request against the profiles it was measured on, and fails closed
outside them (`gliner_boundary_qualification.zig`):

- `fastino/GLiNER2.5-multi-Decide` serves extraction and Decide, but only
  through short single-window profiles. The measured entity row is 663 bytes /
  114 words, and there is no long-document profile.
- `fastino/GLiNER2.5-Decide-1B` in FP32 (about 4.7 GB) serves Decide and,
  since its span heads landed, entity/relation extraction. Both are limited
  to 64 prepared sequences of at most 1024 tokens, and there is no
  long-document (`window`) mode.
- The Q8 bundle `antflydb/gliner2.5-decide-1B-gguf` (1.74 GB) serves Decide
  only: one sequence of at most 198 prepared tokens, and no extraction. Its
  qualification is separate from FP32's.

Measured on Codex's own requests (antflydb/antfly#1083 has the table):
- **Approvals:** the FP32 1B handles realistic approval states (about 1,000
  characters, 169 ms). It separates effects more sharply than Laya on short
  examples (p(destructive) 0.72 vs 0.58 for `rm -rf ~/`).
- **Supersession:** its probabilities are flat. It finds 0 of 7
  supersessions at the 0.8 threshold, where Laya scores 11 of 13 with
  precision 5/5.
- **Q8:** it matches FP32 wherever it fits.

To try the 1B for approvals:
- Set `approvals.model = "fastino/GLiNER2.5-Decide-1B"`, and leave
  `decide_model` on Laya for supersession.
- Pull it at Antfly's qualified revision
  (`fastino/GLiNER2.5-Decide-1B:auto@688cd7ba8917a0855ad3ce929cba5a9998932e79`).
  Fastino's `main` has moved on, and Antfly refuses that revision.
- The split GGUF bundles can't be pulled yet; download them with
  `hf download` instead.

These limits are what Antfly has measured, not architectural ones (the 1B's
encoder accepts 7999 tokens). antflydb/antfly#1083 tracks the requests to
lift them: Q8 Decide and extraction at FP32's limits, long-document
extraction for the 1B, and a fine-tuning path for it.

### Concept descriptions

A promoted concept carries only its canonical name, entity type and aliases.
`Knowledge::describe_concepts` adds a one- or two-sentence description,
written by a local generator from the current claims about the concept:
- **Model:** `knowledge.describe_model`, default
  `ggml-org/gemma-4-E4B-it-GGUF`. An empty string turns descriptions off.
- **Evidence:** up to 8 claims, settled first and then the newest, gathered
  by traversing the concept's `about` edges. When claims leave room, up to 3
  lines from memory notes that name the concept are added; this lifted
  coverage on the E2E database from 45 to 87 of 111 concepts.
- **Storage:** `codex_concept_descriptions`, keyed by concept key. The
  description is its `search_text`, so it is full-text searchable and
  embedded.
- **Rewriting:** a description is rewritten only when its evidence hash
  (model, name, type and claims) changes. Concepts merged into another are
  skipped.
- **When it runs:** after each periodic consolidation pass (20 concepts per
  pass), and on demand with `codex knowledge describe [--limit N]`.
- **Where it shows:** `knowledge.search` returns descriptions for its seed
  concepts as `concept_descriptions`.
- **Example output** (from `concepts_are_described_from_their_claims`): "The
  app-server daemon is a process that Codex runs through, as only one
  process may write the embedded Antfly Lite file."

Antfly's own conceptualizer recipe (a `generator` enrichment with
`neighbor_context`) doesn't fit Codex's layout:
- `neighbor_context` samples only a graph index on the concept table itself.
  Codex's `about` edges live in the claims and notes tables.
- It renders neighbor keys, not the claim text a description needs.

### Triage before extraction

Each extraction request is a model call, and most turns hold nothing
durable. Before a batch is sent, the decide model screens each turn
(`codex-rs/ext/antfly/src/claims/triage.rs`):
- **Input:** the turn's first 1,200 and last 800 characters.
- **Question:** a single choice: what does the turn mainly contain? The
  choices are a decision, a preference, a lesson, an open question, or
  routine work.
- **Score:** p(durable) is 1 − p(routine).
- **Why a choice and not a yes/no:** on ten hand-labeled turns with Laya,
  the yes/no predicate scored near chance (13/25 durable–routine pairs
  ordered correctly). The choice ordered all 25: durable turns scored
  0.91–0.98, routine ones 0.20–0.67. GLiNER2.5-Decide-1B did worse on every
  design (best 20/25).

`claim_triage` sets the mode:
- `shadow` (the default) logs every verdict to `codex_decision_log` (kind
  `triage`, subject `<thread>:<turn start>`) and still extracts every turn.
- `enforce` skips turns below `claim_triage_threshold` (default 0.5). A turn
  the model fails on is always extracted.
- `off` turns triage off.

`codex knowledge status` scores the shadow verdicts against
`codex_extraction_log`: at thresholds 0.2, 0.5 and 0.8, how many turns that
produced claims would still be extracted, and how many turns would be
skipped.

### Decide-model benchmark

`codex knowledge benchmark [--model M]... [--limit N]` replays Codex's
labeled questions through each model (`codex-rs/antfly/src/bench.rs`; the
default models are the configured decide and approval models). The tasks:

| Task | Inputs | Label | Score |
|---|---|---|---|
| `supersession` | the hand-labeled pairs in `supersession.jsonl` | `replaces` | `supersession_score`: the `supersedes` predicate, or same + updates when one of them is the top relation. A threshold on this score is Codex's supersession rule |
| `approval_deny` | logged approval states the user resolved | the user denied or aborted | p(destructive) |
| `approval_allow` | the same | approved, and it ran successfully | p(none) + p(local) |
| `triage` | logged extraction turns, rendered from history | extraction found claims | 1 − p(routine) |

For each model and task it reports AUC, precision and recall at 0.5 and
0.8, median latency, and failures, such as requests over a model's
qualified input limit.

First run on the E2E scratch database (approvals and triage had no labels
yet):

| Model | Supersession AUC | At 0.8 |
|---|---|---|
| `convaiinnovations/laya` | 0.98 | precision 100%, recall 71% |
| `fastino/GLiNER2.5-Decide-1B` | 0.69 | no positives |
| `antflydb/gliner2.5-decide-1B-gguf` | 0.69 | no positives; the approval state was rejected as over its 198-token limit |

### Merging with descriptions, and concept links

Every pass first describes concepts whose evidence changed, then merges
synonyms, then links related concepts. It runs periodically after
extraction, and on demand with `codex knowledge consolidate`.

**Choosing the judge.** The candidates were measured on hand-labeled
fixtures:
- `concept_merge.jsonl`: 64 pairs, 18 of them the same thing. 30 are the
  curated name pairs, with descriptions generated from project facts; 34
  are real pairs from the E2E database, with their real descriptions.
- `concept_relations.jsonl`: 47 directed pairs, 27 related.

| Judge | Merge P / R (AUC) | Relations: detection P / R, type right |
|---|---|---|
| Extraction model, names only (before) | 83% / 83% (0.88) | — |
| **Extraction model, with descriptions** | **94% / 94% (0.96)** | **89% / 93%, 24 of 27** |
| Gemma 4 E4B (local generator), with descriptions | 67% / 44% (0.68) | 85% / 81%, 17 of 27 |
| Laya, typed decide | 0.50–0.67 AUC | 0.39 AUC, 6 of 27 |
| GLiNER2.5-Decide-1B, typed decide | 0.58 AUC | 0.60 AUC, 6 of 27 |

The typed decide models are near chance on both tasks. Laya even gets worse
with descriptions. So the extraction model judges, now with descriptions;
this is what `codex knowledge benchmark --llm-judge` measures. The benchmark
scores an unsure `same` as 0.5: it is proposed for review rather than
merged.

**Merging:**
- The judge sees each name followed by its description.
- A concept with no close name neighbor can be paired through its nearest
  description neighbor, at relative similarity 0.9 or more, with the same
  number tokens. This catches synonyms with dissimilar names.
- Description similarity alone is not a merge signal. Descriptions of
  related things sound alike: "embedder" and "EmbeddingGemma 2" scored 0.98.

**Links:**
- **Candidates:** pairs of described concepts that are co-mentioned in
  current claims, or within 0.85 by description.
- **Judging:** the same model answers `a_is_b`, `b_is_a`, `a_part_b`,
  `b_part_a`, `alternatives` or `none`, 40 pairs per request. Each pair is
  judged once (`codex_concept_relation_reviews`).
- **Storage:** edges go to `codex_concept_links`, one document per source
  concept with `is_a`, `part_of` and `alternative_to` arrays. A field-derived
  graph index (`codex_concept_links_v1`) makes them traversable.
- **Retrieval:** `knowledge.search` expands its seed concepts one link in
  either direction. Claims about a linked concept rank at half weight, and
  the result lists `linked_concepts`.
- **On the E2E database:** two passes judged 72 pairs and made 15 links.
  About 11 were clearly right ("EmbeddingGemma 2 is_a embedder",
  "Codex semantic search part_of Codex"). Two were wrong: one reversed, and
  "threads part_of Codex Antfly importer".

**Descriptions** use Gemma 4 E4B with a prompt that keeps each fact attached
to the concept it is actually about. E2B, and the first prompt, moved
properties between concepts ("Antfly … is half the size of Qwen3"). A prompt
version in the evidence hash rewrites descriptions when the prompt changes. When E4B
can't be admitted, Codex falls back to E2B for the rest of the pass, and a
later pass that loads E4B rewrites those descriptions. Admission depends on
macOS memory pressure (Antfly keeps about 9.7 GB in reserve) and on memory
already held by extraction and decide models; E4B needs about 6.7 GB at
peak.

**Maintenance commands:**
- `codex knowledge links` lists the links.
- `codex knowledge unlink <A> <B>` removes a wrong link in either direction
  and keeps the pair from being linked again.
- `codex knowledge merges` lists merges.
- `codex knowledge consolidate --rejudge` forgets which concepts were
  examined (merges are kept) and judges them all again with their
  descriptions, running passes until none are left. On the E2E database it
  merged 7 more concepts and grew the links from 13 to 26.

**Generic words:** the relation judge is told to answer `none` for generic
words. Memory notes made junk links such as "note is_a content" before this;
the change kept fixture detection at 89% precision and 93% recall, with the
type right on 25 of 27.

**Local inference retries:** local inference calls retry Antfly's retryable
`MODEL_RESOURCE_BUSY` for up to 30 s (a model still loading, or memory held
by another model).

### Label logs

Every model call that decides something on Codex's behalf is kept as a
labeled example, for calibrating thresholds, benchmarking decide models and
fine-tuning (`codex-rs/antfly/src/labels.rs`, migration 25):

- **Approvals** (`codex_approvals`, record version 2). Each record holds:
  - the effect probabilities and the intent probability;
  - the verdict, plus the mode, decide model and thresholds that produced it;
  - the user's resolution (`decision`, `decided_by`, `resolved_at_ms`),
    written as soon as it happens, so a restart before the tool finishes
    keeps it;
  - the tool outcome (`outcome`, `finished_at_ms`).

  The working directory is part of `action`.
- **Supersession checks** (`codex_decision_log`, `kind = 'supersession'`).
  Each row holds the decide input and full response, the verdict, the
  threshold and the model. The equal-text shortcut makes no model call and
  is not logged.
- **Claim extraction** (`codex_extraction_log`), one row per turn sent to
  the extractor. Each row holds the turn's ordinals and characters, the
  claims attributed to it, the request's totals and the model. A turn with
  zero claims is a "skip" label for triage before extraction.

### Antfly findings (from the embedded probe)

- These work in embedded libantfly:
  - graph indexes with inline extractor artifacts and resolvers;
  - field-derived edges in a separate graph index;
  - reverse traversal from concept keys with documents;
  - search plus `result_ref:"$query_results"` expansion in one request;
  - matching concepts across tables by key.
- At 1258e8cfcb, a combined `$query_results` + `direction:"both"` +
  `max_depth:2` traversal was rejected as invalid. It works at `d1182ab89d`
  (cause unexplained upstream); retrieval still uses single-hop queries plus
  PageRank.
- At 1258e8cfcb, a traversal reported each reached document once, with one
  path. `deduplicate_nodes: false` (antflydb/antfly#1096) now returns every
  path.
- Since `d1182ab89d`, Lite **promotes** resolved concepts into the resolver's
  `table`. Codex creates `codex_concepts` for this (one document per
  `concept/<slug>`, with `canonical_name`, `entity_type` and `aliases`).
  Without it, `run_until_idle` fails with `UndefinedTable`, resolution stalls
  and no `about` edges land. `promote: false` (antflydb/antfly#1111) would
  make the table optional.
- Since `d1182ab89d`, enrichments load models from the configured
  `models_dir` (`OpenOptions::models_dir`), the same directory as the decide
  runtime. Native errors carry their detail (`Error::Native`), which our
  `AntflyError::Embedded` messages include.
- **Extraction quality.** In the probe, GLiNER missed "EmbeddingGemma 2" in one
  claim and produced both `concept/qwen3` and `concept/qwen3_embedding`. That is
  the synonym problem below.
- **No entity documents in Lite.** Promotion into entity rows is server-only
  (antflydb/antfly#1084), and so is AutoSchemaKG. Claims are Codex's own
  documents, so this design does not depend on #1084.
- **Remote.** Graph indexes are created with `POST /tables/{t}/indexes/{name}`
  (`"type":"graph"`). Stale remote versions are not dropped automatically.

### Evaluation

`antfly/tests/fixtures/knowledge_eval/` holds labelled sets:

- supersession pairs;
- concept-name pairs (synonyms and hard negatives);
- the concept-extraction corpus with both extraction configs.

The opt-in test `knowledge_eval` prints precision and recall for supersession
and for the concept-merge gates:

```sh
cargo test -p codex-antfly --test embedded knowledge_eval -- --ignored --nocapture
```

Current baseline:

- **Supersession:** precision 1.00, recall 0.71. It misses a changed default
  ("off" to "shadow mode") and an open question answered from another thread;
  within a thread, the extractor's `replaces` covers both.
- **Concept gates:** as above.

The sets are seeded from this project's sessions and the calibration runs.
Grow them from real sessions, especially with claim-extraction labels, before
knowledge is injected into context automatically. Today it is only reached
through `knowledge.search`.

### Not built yet

- **Enforcing triage.** Triage runs in shadow mode (see
  [Triage before extraction](#triage-before-extraction)). Enforcing it waits
  until `codex knowledge status` shows recall of about 0.95 or better on
  turns that produced claims, at a threshold that skips a useful share of
  turns.
- **Moving the merge judge into Antfly.** Antfly has what it needs
  (antflydb/antfly#1186): REVIEW-band adjudication with a decider (#1176),
  and generator-backed deciders (`decision_method: generated`, #1187), which
  can use hosted generators. What blocks it is quality, not features. The
  local typed models are near chance on the concept fixtures, and a local
  generated decider (Gemma E4B) reaches only AUC 0.86, so Codex keeps its
  own judge (94%) for now.
  - **Next step:** benchmark a hosted generator as a generated decider
    (OpenAI provider) on the fixtures, as a `codex knowledge benchmark`
    model. This needs an OpenAI API key.
  - **If it matches,** move merge judging into resolution, with concept
    descriptions as an asset on the concept documents rather than a
    separate table.
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

## Antfly memory service (first steps)

Antfly's native memory service (`/memory/v1`, merged in antflydb/antfly#1258) is
the intended home for claims, identity policy, descriptions, relations, history
episodes and recall. Codex has a client for it and has measured its judges
through it; claims are not migrated yet.

**Client.** `codex_antfly::memory::MemorySpace` runs memory actions in process
on Lite (`Backend::memory` → `antfly_db_memory_json`) or over HTTP on a server.

**External judging.** `codex knowledge memory-eval --space S --run R
[--corpus FILE] [--url URL] [--save FILE]` has the extraction model judge an
external evaluation run and submits the verdicts:
- identity uses the merge judge (sure `same`/`different`; unsure abstains);
- relations use the relation judge, one answer per pair for all proposed
  predicates;
- supersession uses a new four-way judge (`supersedes`, `contradicts`,
  `restates`, `independent`).

With `--corpus` (written by Antfly's `scripts/bench/memory_judges.py
--prepare-only`) it creates the space, uploads the cases and starts the run
itself. On Lite, against the 248-case Codex corpus:

| Task | Cases | Precision | Recall | Notes |
| --- | --- | --- | --- | --- |
| Identity, names only | 30 | 1.00 | 0.71 | Misses include spelling twins ("memory pipeline"/"memories pipeline") that Codex merges lexically before judging |
| Identity, with descriptions | 64 | 0.93 | 0.72 | 4 more are unsure (proposals) |
| Relations | 141 | 0.88 | 0.85 | |
| Supersession (binary labels) | 13 | 0.83 | 0.71 | Stable over two runs, 1 unsure |

The first supersession prompt let `restates` absorb updates that repeat a
claim with new detail (recall 0.57); the prompt now counts any new or changed
detail as `supersedes`.

**Subjects for claims.** Supersession candidates must share a canonical
subject. Codex claims are free text, so the first mapping tried is subject =
project, predicate = `states`, value = the statement. Shadow-writing the 26
fixture claims this way on Lite:
- all 26 stay active (no implicit supersession under one subject/predicate);
- with lexical discovery, all 7 replaced claims appear in their replacement's
  top-10 candidates (five at rank 1, one at 4, one at 8). Four of the six
  non-replacing pairs also appear; candidates are not verdicts, so the judge
  decides.

Semantic discovery could not be tried: enabling it needs the embedder's
internal revision digest, which no public API exposes.

**Open on the Antfly side** (antflydb/antfly#1287):
- evaluation pages and supersession candidates fail on the server
  (`InvalidQueryRequest` / `UnsupportedQueryRequest`); Lite works;
- `zig build antfly` fails at 3f8eeafaaf (`exportStandalonePortableCatalog`
  on the distributed client);
- Lite candidates stay `not_ready` until maintenance is advanced explicitly;
- evidence needs a `source_id` (the schema says only `id`);
- the embedder revision is not discoverable.

**Branch.** This work needs `memory_json`, which first ships at 3f8eeafaaf. At
that commit Lite curation applies about 10 s after it is accepted (`wait`
returns only at its deadline) and grouped sources stop advancing again
(antflydb/antfly#1289). Three Codex tests fail as a result. So this lives on
`antfly/memory-service`; `antfly/integration` stays on ddba6584b3 until #1289
is fixed.

## Durable sessions across processes

Prototype (October 23): could a Codex agent survive its process dying, with
the thread picked up by a different app-server, using Antfly as the only
shared state? The driver talks JSON-RPC to two `codex app-server` processes
with separate `CODEX_HOME`s (standing in for two machines) and one store:

It passes against `antfly standalone` (main afd21fc353), with nothing shared
between the two processes but the server, and against a shared Lite file:

1. App-server A starts a thread and a turn that runs `sleep 25 && echo done-A > marker-a.txt`.
2. A is SIGKILLed while the command runs.
3. App-server B calls `thread/resume`. The killed turn reads back as
   `interrupted`, holding only the user message.
4. Asked to finish, B reruns the command; `marker-a.txt` contains `done-A`.

Findings:
- **Client resume of Antfly threads was broken.** `thread/resume` required
  a rollout file path. Fixed: see the patch point above.
- **Hosted Antfly crashed first.** The server aborted within seconds of the
  first turn: two upserts in one pgwire transaction, which the history
  projection does every batch (antflydb/antfly#1267, fixed by #1269).
- **Remote NULL parameters were typed as text.** Codex bound SQL NULL as
  `Option<String>`, a text expression that PostgreSQL semantics reject for
  `BIGINT` and `JSONB` columns (42804). Older servers accepted it. NULLs are
  now sent with no declared type (OID 0), so the server infers each from its
  column (`antfly/src/sql.rs`, `UntypedNull`).
- **In-flight tool calls are not recorded.** Items are persisted on completion,
  so after a crash the store has no trace of a command that started. Here the
  command died with its process. A command on a remote machine
  (`exec-server`) could survive and finish, so durable sessions need a
  "started" record for side-effecting calls.
- **Recovery ownership is local.** The app-server's daemon recovery list is a
  local file (`app-server/src/daemon_thread_recovery.rs`), so only the same
  machine restores its threads. Another host taking over needs a
  lease row in Antfly. Upstream's `continue_daemon_turn` already continues a
  recovered turn rather than only marking it interrupted.

## Rebasing

- `thread-store/` and `state/` change frequently upstream (about 15 commits
  per week to `thread-store/` in early October 2026). Keep upstream edits to
  the patch points above.
- Every new upstream `state` migration or `StateRuntime` method needs a
  matching Antfly port; the `DualStore` harness and the fresh-`CODEX_HOME`
  gate catch drift.
- Branch: `antfly/integration`, tracking `upstream/main`.
- Upstream builds `v8` 152 against Codex-built rusty_v8 artifacts from the
  `rusty-v8-v<version>` release of openai/codex. Locally, download
  `librusty_v8_ptrcomp_sandbox_release_<target>.a.gz` and
  `src_binding_ptrcomp_sandbox_release_<target>.rs`, check them against
  `third_party/v8/rusty_v8_<version>_release_manifests.sha256` (as
  `.github/actions/setup-rusty-v8` does), and set `RUSTY_V8_ARCHIVE` and
  `RUSTY_V8_SRC_BINDING_PATH`. `codex-voice-host` needs GStreamer; exclude
  it from workspace builds without it.
