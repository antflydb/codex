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
- Approval review defaults to `off`. Laya's zero-shot probabilities are only
  moderately separated; collect shadow-mode decisions and outcomes (stored
  in `codex_approvals`) before enforcing, and consider fine-tuning on them.
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
  Antfly main at `1258e8cfcb` (C ABI version 3, `zig build capi`); CLI binaries and `codex-core` test
  binaries embed it as an rpath (the fs sandbox helper re-execs the binary
  with `DYLD_LIBRARY_PATH` stripped). Other crates' test binaries still need
  `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`.
- `agent::control::tests::ensure_v2_child_loaded_preserves_evicted_parent_authority`
  overflows the default 2 MiB test-thread stack on upstream `3342ee8c07` as
  well; run `codex-core` unit tests with `RUST_MIN_STACK=16777216`.
- The workspace `Cargo.toml` takes `antfly-embedded` from
  `github.com/antflydb/antfly` pinned to `1258e8cfcb` (with the `sqlx`
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
path = "~/.codex/antfly.aflite"      # embedded (the default without `url`)
# url = "https://<host>/cloud/v1/<instance_id>"            # remote documents (HTTP)
# sql_url = "postgres://codex:secret@<host>:5432/default"  # remote SQL tables
# api_key_env = "ANTFLY_API_KEY"
models_dir = "~/.antfly/inference/models"
decide_model = "convaiinnovations/laya"
# embedder_model = "google/embeddinggemma-2"  # default
# embedder_dims = 256                 # optional Matryoshka truncation; omit for native size
# semantic_search = false            # full text only, no embedder needed

[experimental_thread_store.approvals]
mode = "shadow"                      # off | shadow | enforce
allow_threshold = 0.7
deny_threshold = 0.8
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

## Knowledge graph (proposed)

Status: design only; nothing below is implemented. Findings are against
Antfly 1258e8cfcb (main adds nothing graph-related beyond it).

### Goal

Capture ideas and knowledge across sessions and how they relate, not code
structure (ASTs and language servers already cover that). The graph should
answer "what did we decide about X, why, and is that still true?": decisions,
rationale, preferences, lessons, constraints, rejected alternatives and open
questions, and how they evolve. For example, this fork's embedder went
bge-small → Qwen3 → EmbeddingGemma 2. The graph should return the current
choice and the chain of reasons.

### Model

Two layers of nodes:

- **Claims.** Typed propositions: `decision`, `rationale`, `preference`,
  `lesson`, `constraint`, `open_question`. Each has a statement, provenance
  (thread, turn, source item), `observed_at`, and a status
  (`current` / `superseded` / `retracted`). Claims are clause-level, so they are
  written by an LLM, not span extraction.
- **Concepts.** Canonical `concept/<slug>` nodes for the ideas, technologies,
  projects, people, goals and problems that claims are about.

Edges:

| Edge | Between | Produced by |
|---|---|---|
| `about` | claim → concept | GLiNER extraction + resolver (mention edges) |
| `from` | claim → thread / history item | Codex, at write time |
| `supersedes`, `contradicts`, `refines` | claim → claim | decide model, at write time |
| `motivated_by`, `rejected_in_favor_of` | claim → claim | the distilling LLM |
| `part_of`, `alternative_to`, `uses` | concept → concept | GLiNER relation extraction |

Supersession is the core of the design. Every edge carries validity times, so
Antfly's bitemporal `edge_filter` (`valid_at`, `known_at`) can return the
current state and still walk the history.

### Where claims come from

- **The memories pipeline.** Codex already has a model distill each session
  (stage 1 rollout summaries, then consolidation). Extend that pass to also
  emit structured claims and claim→claim relations. This needs no new
  inference pass and gives better quality than extracting from raw history.
  To do first: check how structured stage-1 output is today.
- **Sources.** User messages (preferences, corrections, goals), final agent
  answers, and memory notes. Skip tool output and reasoning, which are mostly
  noise for this graph.

### What GLiNER does

GLiNER does not write claims. It does the cheap linking work:

1. **Concept linking at write time.** An `extractor` asset enrichment on the
   claims table, memory notes and history `search_text`. Labels are given
   zero-shot at request time and describe ideas, not code:
   ```json
   {"type":"extractor","config":{"provider":"antfly","model":"fastino/gliner2.5-base-v1",
     "schema":{"entities":["concept","technology","project","person","goal","problem"],
       "entity_definitions":{"concept":"an idea, technique or design notion",
                             "problem":"a bug, limitation or failure mode"},
       "relations":[{"type":"part_of"},{"type":"alternative_to"},{"type":"uses"}]},
     "options":{"include_confidence":true,"long_document":{"mode":"window"}}}}
   ```
   A graph index sources the resulting artifact (`format:"extraction_relation"`,
   `mention_edge_type:"about"`). A resolver with `key_template:
   "concept/{{ slug _entity.text }}"` mints the canonical concept keys.
2. **Concept→concept relations.** Stable facts only. Relations between claims
   are judgments and come from the decide model or the LLM.
3. **Seed concepts at query time.** Run `Inference::extract` with the same labels
   on the user's message (about 100 ms locally). Resolve the hits to concept
   keys and use them as graph seeds.
4. **Triage.** GLiNER2 classification over user messages (`correction`,
   `preference`, `decision`, `question`, `chit-chat`) chooses which turns to
   distill, and flags corrections so the distiller looks for a claim to
   supersede.

Antfly includes GLiNER fine-tuning. Once a labeled set from real sessions
exists, tune the concept labels on it instead of relying on zero-shot prompts.

### Synonyms

"Laya reviewer", "approval reviewer" and "the decider" must resolve to one
concept. Slug matching cannot do that. Use the resolver's embedding candidate
search (`candidate_search:"ann"`, `name_embedding` with EmbeddingGemma 2) and
its scorer. Embedded ANN resolution is unverified and needs a prototype.

### Retrieval

At the start of a turn:

1. Extract seed concepts from the user message.
2. Run seeded personalized PageRank (`graph_metric` with `seed_nodes`), or a
   bounded traversal, over `about` edges.
3. Surface `current` decisions, preferences, lessons and constraints about
   those concepts, each linked back to its source sessions.
4. Follow `supersedes` and `motivated_by` edges only when asked "why".

Expose this as a model-facing tool (a new `ToolContributor`, built like the
memories one). There is no model-facing session search today. Also use it to
expand `search_threads` and memory search through shared concepts.

### Model roles

Model roles are separate settings, so collapsing them into one model later is
a configuration change, not a code change:

| Role | Default | Notes |
|---|---|---|
| Embeddings | `google/embeddinggemma-2` | Already the default; also feeds the synonym resolver |
| Extraction | `fastino/gliner2.5-base-v1` | Installed; qualified for long documents with `window` mode |
| Decide (approvals, supersession) | `convaiinnovations/laya` | To be benchmarked against the GLiNER2.5 Decide models |
| Claim distillation | the session's LLM via the memories pipeline | No local model |

**Why not one GLiNER2.5 Decide model for both, yet.** Antfly gates every
extraction request against the profiles it was measured on, and fails closed
outside them (`gliner_boundary_qualification.zig`):

- `fastino/GLiNER2.5-multi-Decide` serves extraction and Decide, but only
  through short single-window profiles. The measured entity row is 663 bytes /
  114 words, and there is no long-document profile. Most memory notes and many
  messages would be refused.
- `fastino/GLiNER2.5-Decide-1B` serves Decide only. Its entity and relation
  heads are not implemented (antflydb/antfly#1083). Decide calls are limited to
  one item and at most 198 prepared tokens.

Benchmark both against Laya on the approval and supersession questions before
switching. The typed-decisions community benchmark scores Decide-1B at 0.470
zero-shot and the released Laya at 0.361. Short claim-vs-claim predicates fit
both models' limits. The approval prompt (up to 3,000 characters) would need to
shrink to about the action plus a one-line intent.

### Antfly mapping and limits

- **Embedded support.** These all work in embedded libantfly:
  - graph indexes (`antfly_db_add_index_json`, kind `graph`, with inline
    `resolvers`);
  - extractor enrichments with in-process inference;
  - traversal, shortest paths and pattern matching through the C ABI;
  - `graph_queries` inside `antfly_search` JSON.
- **Unverified in embedded:**
  - search plus `result_ref:"$query_results"` graph expansion in one request;
  - seeded PageRank;
  - ANN resolution;
  - cross-table traversal through shared concept keys.
- **No entity documents in Lite.** Resolution produces concept *nodes*, but
  promotion into entity *rows* is server-only (antflydb/antfly#1084), and so is
  AutoSchemaKG. Codex therefore writes claim records into its own table, with
  explicit `_edges` for claim→claim and claim→thread edges. GLiNER is used only
  for concept linking, so the design does not depend on #1084.
- **Per-table opt-in.** `TableSpec` (`antfly/src/backend.rs`) gains optional
  extraction and graph specs. The dense index is currently applied to every
  document table; graph and extraction should be opt-in per table, implemented
  in both `ensure_table` backends.
- **Remote.** The same graph index JSON goes to `POST /tables/{t}/indexes/{name}`.

### Risks

- **Noisy claims.** Build a labeled set from real sessions, this one included.
  Measure claim and supersession precision before surfacing anything to the
  model automatically.
- **Cost.** Extraction is asynchronous (enrichments), but every history write
  adds GLiNER work. Limit it to the sources above.
- **Memory notes are indexed lazily.** Notes written by the memories pipeline
  are only indexed when first read. Index them on write first.

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

### Phase 7: Knowledge graph

See [Knowledge graph (proposed)](#knowledge-graph-proposed).

1. **Prototype (embedded test).** A graph index with a GLiNER extractor and a
   concept resolver on a scratch table. Verify each of these in libantfly:
   - concept nodes and `about` edges;
   - `graph_queries` with `$query_results`;
   - seeded PageRank;
   - ANN synonym resolution.
2. **Claims.** Read the memories stage-1 prompt and output, define the claim
   schema, and have distillation emit claims into a `codex_knowledge` table
   with explicit edges.
3. **Supersession.** At write time, compare each new claim with its top-k
   similar claims using a decide predicate, and add `supersedes` /
   `contradicts` edges.
4. **Retrieval.** Query-time seed extraction, a model-facing knowledge tool,
   and concept expansion for `search_threads` and memory search.
5. **Memory notes.** Index on write, and link notes to the same concepts.
6. **Decide models.** Benchmark Laya, GLiNER2.5-multi-Decide and
   GLiNER2.5-Decide-1B on the approval and supersession questions.
   Gate: a labeled set from real sessions, and precision targets met, before
   anything is surfaced to the model automatically.

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
