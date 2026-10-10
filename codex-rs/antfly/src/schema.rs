//! Every Codex table in Antfly, and the migrations that create them.
//!
//! Relational tables hold state that is read by key, listed with keyset
//! pagination, or updated under leases and compare-and-set; they are created
//! with SQL DDL and accessed through [`crate::sql::Sql`]. Document tables hold
//! text that Codex searches (thread history, approvals, memory notes); they
//! carry Antfly full-text and, with an embedder, dense indexes and are written
//! through the document API. Search results join relational tables through
//! `antfly_search('<table>', ...)` or through the document API.
//!
//! Antfly SQL has no triggers, sequences, or BLOB type, so the SQLite schema's
//! timestamp triggers, `AUTOINCREMENT` revisions, and BLOB records are
//! maintained by the stores, and CHECK constraints spell out `IN` lists with
//! `OR`, which Antfly CHECK expressions do not yet accept.

use crate::error::AntflyError;
use crate::error::AntflyResult;
use crate::sql::Sql;
use crate::sql_params;

/// A relational schema change. Each statement is idempotent (`IF NOT
/// EXISTS`, `ON CONFLICT DO NOTHING`) because Antfly DDL is not
/// transactional: a crash between statements is repaired by rerunning them.
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub statements: &'static [&'static str],
}

pub const THREADS: &str = "codex_threads";
pub const THREAD_SECTIONS: &str = "codex_thread_sections";
pub const PINNED_SECTION_ID: &str = "01984de2-8f74-7c91-a3b2-5c5e937cf318";

/// Searchable rollout items, one document per item (`<thread>:<ordinal>`).
pub const HISTORY_ITEMS: &str = "codex_history_items";
/// Approval decisions and their observed outcomes.
pub const APPROVALS: &str = "codex_approvals";
/// Memory notes mirrored from the memories filesystem.
pub const MEMORY_NOTES: &str = "codex_memory_notes";

/// A document table with the declared fields SQL can filter and join on.
/// Every document also carries `search_text`, which the full-text index
/// matches and the optional dense index embeds.
pub struct DocumentTable {
    pub name: &'static str,
    /// `document_schemas` properties, as JSON Schema property definitions.
    pub fields: &'static [(&'static str, &'static str)],
}

pub const DOCUMENT_TABLES: &[DocumentTable] = &[
    DocumentTable {
        name: HISTORY_ITEMS,
        fields: &[
            ("thread_id", "string"),
            ("ordinal", "integer"),
            ("turn_id", "string"),
            ("speaker", "string"),
            ("created_at_ms", "integer"),
        ],
    },
    DocumentTable {
        name: APPROVALS,
        fields: &[
            ("thread_id", "string"),
            ("approval_id", "string"),
            ("tool_call_id", "string"),
            ("category", "string"),
            ("verdict", "string"),
            ("outcome", "string"),
            ("applied", "boolean"),
            ("decided_at_ms", "integer"),
        ],
    },
    DocumentTable {
        name: MEMORY_NOTES,
        fields: &[
            ("namespace", "string"),
            ("path", "string"),
            ("updated_at_ms", "integer"),
        ],
    },
];

impl DocumentTable {
    /// `antfly_db_create_table_json` / `POST /tables/{name}` schema.
    pub fn schema_json(&self) -> serde_json::Value {
        let mut properties = serde_json::Map::new();
        for (field, kind) in self.fields {
            properties.insert((*field).to_string(), serde_json::json!({ "type": kind }));
        }
        properties.insert(
            crate::SEARCH_TEXT_FIELD.to_string(),
            serde_json::json!({ "type": "string" }),
        );
        serde_json::json!({
            "default_type": "doc",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "type": "object",
                        "properties": properties,
                        "additionalProperties": true,
                    }
                }
            }
        })
    }
}

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "codex state tables",
    statements: &[
        // Sections and projects precede threads, which reference them.
        "CREATE TABLE IF NOT EXISTS codex_thread_sections (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            appearance TEXT
        )",
        "INSERT INTO codex_thread_sections (id, name) \
         VALUES ('01984de2-8f74-7c91-a3b2-5c5e937cf318', 'Pinned') ON CONFLICT (id) DO NOTHING",
        "CREATE TABLE IF NOT EXISTS codex_projects (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            metadata JSONB NOT NULL,
            position BIGINT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS codex_projects_position ON codex_projects (position, id)",
        "CREATE TABLE IF NOT EXISTS codex_project_roots (
            project_id TEXT NOT NULL REFERENCES codex_projects (id) ON DELETE CASCADE,
            position BIGINT NOT NULL,
            path TEXT NOT NULL,
            PRIMARY KEY (project_id, position)
        )",
        "CREATE TABLE IF NOT EXISTS codex_project_idempotency_keys (
            key TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        )",
        // created_at/updated_at/recency_at keep the SQLite seconds columns;
        // the *_ms columns are written explicitly (SQLite derived them with
        // triggers).
        "CREATE TABLE IF NOT EXISTS codex_threads (
            id TEXT PRIMARY KEY,
            rollout_path TEXT NOT NULL,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            recency_at BIGINT NOT NULL,
            recency_at_ms BIGINT NOT NULL,
            source TEXT NOT NULL,
            thread_source TEXT,
            model_provider TEXT NOT NULL,
            model TEXT,
            reasoning_effort TEXT,
            cwd TEXT NOT NULL,
            title TEXT NOT NULL,
            name TEXT,
            preview TEXT NOT NULL DEFAULT '',
            first_user_message TEXT NOT NULL DEFAULT '',
            sandbox_policy TEXT NOT NULL,
            approval_mode TEXT NOT NULL,
            tokens_used BIGINT NOT NULL DEFAULT 0,
            has_user_event BOOLEAN NOT NULL DEFAULT false,
            archived BOOLEAN NOT NULL DEFAULT false,
            archived_at BIGINT,
            git_sha TEXT,
            git_branch TEXT,
            git_origin_url TEXT,
            cli_version TEXT NOT NULL DEFAULT '',
            agent_nickname TEXT,
            agent_role TEXT,
            agent_path TEXT,
            memory_mode TEXT NOT NULL DEFAULT 'enabled',
            history_mode TEXT NOT NULL DEFAULT 'legacy',
            is_pinned BOOLEAN NOT NULL DEFAULT false,
            thread_section_id TEXT REFERENCES codex_thread_sections (id) ON DELETE SET NULL,
            section_position BIGINT,
            section_entered_at_ms BIGINT,
            project_id TEXT REFERENCES codex_projects (id) ON DELETE SET NULL,
            originator TEXT,
            daybreak_enabled BOOLEAN,
            creator_user_id TEXT,
            creator_account_id TEXT,
            extra JSONB
        )",
        "CREATE INDEX IF NOT EXISTS codex_threads_created ON codex_threads (archived, created_at_ms DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS codex_threads_updated ON codex_threads (archived, updated_at_ms DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS codex_threads_recency ON codex_threads (archived, recency_at_ms DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS codex_threads_cwd_created ON codex_threads (archived, cwd, created_at_ms DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS codex_threads_cwd_updated ON codex_threads (archived, cwd, updated_at_ms DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS codex_threads_cwd_recency ON codex_threads (archived, cwd, recency_at_ms DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS codex_threads_visible_recency ON codex_threads (archived, recency_at_ms DESC, id DESC) WHERE preview <> ''",
        "CREATE INDEX IF NOT EXISTS codex_threads_pinned_recency ON codex_threads (archived, recency_at_ms DESC, id DESC) WHERE is_pinned = true",
        "CREATE INDEX IF NOT EXISTS codex_threads_section_recency ON codex_threads (archived, thread_section_id, recency_at_ms DESC, id DESC) WHERE thread_section_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS codex_threads_section_position ON codex_threads (archived, thread_section_id, section_position, id) WHERE thread_section_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS codex_threads_project ON codex_threads (project_id, archived, recency_at_ms DESC, id DESC) WHERE project_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS codex_threads_rollout_path ON codex_threads (rollout_path)",
        "CREATE TABLE IF NOT EXISTS codex_thread_dynamic_tools (
            thread_id TEXT NOT NULL REFERENCES codex_threads (id) ON DELETE CASCADE,
            position BIGINT NOT NULL,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            input_schema JSONB NOT NULL,
            defer_loading BOOLEAN NOT NULL DEFAULT false,
            namespace TEXT,
            PRIMARY KEY (thread_id, position)
        )",
        "CREATE TABLE IF NOT EXISTS codex_thread_spawn_edges (
            child_thread_id TEXT PRIMARY KEY,
            parent_thread_id TEXT NOT NULL,
            status TEXT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS codex_thread_spawn_edges_parent ON codex_thread_spawn_edges (parent_thread_id, status)",
        "CREATE TABLE IF NOT EXISTS codex_thread_attachments (
            id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL REFERENCES codex_threads (id) ON DELETE CASCADE,
            attachment_type TEXT NOT NULL,
            identity_key TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at BIGINT NOT NULL,
            UNIQUE (thread_id, attachment_type, identity_key)
        )",
        "CREATE INDEX IF NOT EXISTS codex_thread_attachments_thread ON codex_thread_attachments (thread_id, created_at, id)",
        "CREATE INDEX IF NOT EXISTS codex_thread_attachments_identity ON codex_thread_attachments (attachment_type, identity_key, thread_id)",
        // Paginated history projection (the SQLite thread history database).
        "CREATE TABLE IF NOT EXISTS codex_thread_turns (
            thread_id TEXT NOT NULL,
            turn_id TEXT NOT NULL,
            rollout_ordinal BIGINT NOT NULL,
            rollout_end_ordinal BIGINT,
            root_turn_id TEXT,
            status TEXT NOT NULL,
            error_json JSONB,
            started_at BIGINT,
            completed_at BIGINT,
            duration_ms BIGINT,
            first_user_item_id TEXT,
            final_agent_item_id TEXT,
            PRIMARY KEY (thread_id, turn_id),
            UNIQUE (thread_id, rollout_ordinal)
        )",
        "CREATE INDEX IF NOT EXISTS codex_thread_turns_end ON codex_thread_turns (thread_id, rollout_end_ordinal, turn_id) WHERE rollout_end_ordinal IS NOT NULL",
        "CREATE TABLE IF NOT EXISTS codex_thread_items (
            thread_id TEXT NOT NULL,
            turn_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            rollout_ordinal BIGINT NOT NULL,
            updated_at_ordinal BIGINT NOT NULL DEFAULT 0,
            item_type TEXT NOT NULL DEFAULT '',
            item_json JSONB NOT NULL,
            created_at_ms BIGINT NOT NULL,
            started_at_ms BIGINT,
            completed_at_ms BIGINT,
            PRIMARY KEY (thread_id, turn_id, item_id),
            UNIQUE (thread_id, rollout_ordinal)
        )",
        "CREATE INDEX IF NOT EXISTS codex_thread_items_turn ON codex_thread_items (thread_id, turn_id, rollout_ordinal)",
        "CREATE INDEX IF NOT EXISTS codex_thread_items_updated ON codex_thread_items (thread_id, updated_at_ordinal)",
        "CREATE INDEX IF NOT EXISTS codex_thread_items_turn_updated ON codex_thread_items (thread_id, turn_id, updated_at_ordinal)",
        "CREATE INDEX IF NOT EXISTS codex_thread_items_user ON codex_thread_items (thread_id, rollout_ordinal) WHERE item_type = 'userMessage'",
        "CREATE TABLE IF NOT EXISTS codex_thread_history_projection_state (
            thread_id TEXT PRIMARY KEY,
            next_rollout_ordinal BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS codex_thread_realtime_items (
            thread_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            rollout_ordinal BIGINT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            item_type TEXT NOT NULL,
            item_json JSONB NOT NULL,
            PRIMARY KEY (thread_id, item_id),
            UNIQUE (thread_id, rollout_ordinal)
        )",
        // Goals.
        "CREATE TABLE IF NOT EXISTS codex_thread_goals (
            thread_id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            objective TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status = 'active' OR status = 'paused' OR status = 'blocked' OR status = 'usage_limited' OR status = 'budget_limited' OR status = 'complete'),
            token_budget BIGINT,
            tokens_used BIGINT NOT NULL DEFAULT 0,
            time_used_seconds BIGINT NOT NULL DEFAULT 0,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS codex_thread_goal_continuation_deferrals (
            thread_id TEXT PRIMARY KEY REFERENCES codex_thread_goals (thread_id) ON DELETE CASCADE
        )",
        // Memories pipeline.
        "CREATE TABLE IF NOT EXISTS codex_stage1_outputs (
            thread_id TEXT PRIMARY KEY,
            source_updated_at BIGINT NOT NULL,
            raw_memory TEXT NOT NULL,
            rollout_summary TEXT NOT NULL,
            rollout_slug TEXT,
            generated_at BIGINT NOT NULL,
            usage_count BIGINT,
            last_usage BIGINT,
            selected_for_phase2 BOOLEAN NOT NULL DEFAULT false,
            selected_for_phase2_source_updated_at BIGINT
        )",
        "CREATE INDEX IF NOT EXISTS codex_stage1_outputs_source ON codex_stage1_outputs (source_updated_at DESC, thread_id DESC)",
        "CREATE TABLE IF NOT EXISTS codex_jobs (
            kind TEXT NOT NULL,
            job_key TEXT NOT NULL,
            status TEXT NOT NULL,
            worker_id TEXT,
            ownership_token TEXT,
            started_at BIGINT,
            finished_at BIGINT,
            lease_until BIGINT,
            retry_at BIGINT,
            retry_remaining BIGINT NOT NULL,
            last_error TEXT,
            input_watermark BIGINT,
            last_success_watermark BIGINT,
            PRIMARY KEY (kind, job_key)
        )",
        "CREATE INDEX IF NOT EXISTS codex_jobs_claim ON codex_jobs (kind, status, retry_at, lease_until)",
        "CREATE TABLE IF NOT EXISTS codex_consolidation_progress (
            singleton BIGINT PRIMARY KEY CHECK (singleton = 1),
            max_thread_count BIGINT NOT NULL DEFAULT 0
        )",
        // Queued turns. `revision` is assigned by the store as
        // MAX(revision) + 1 in the same transaction (SQLite used
        // AUTOINCREMENT and triggers).
        "CREATE TABLE IF NOT EXISTS codex_queued_items (
            id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            queue_order BIGINT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            UNIQUE (thread_id, queue_order)
        )",
        "CREATE TABLE IF NOT EXISTS codex_queued_thread_revisions (
            thread_id TEXT PRIMARY KEY,
            revision BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS codex_queued_thread_revisions_revision ON codex_queued_thread_revisions (revision)",
        // Guardian review feedback: `record` is base64 of the opaque bytes.
        "CREATE TABLE IF NOT EXISTS codex_guardian_review_feedback (
            id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL REFERENCES codex_threads (id) ON DELETE CASCADE,
            record TEXT NOT NULL,
            record_bytes BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS codex_guardian_review_feedback_thread ON codex_guardian_review_feedback (thread_id, id)",
        "CREATE TABLE IF NOT EXISTS codex_remote_control_enrollments (
            websocket_url TEXT NOT NULL,
            account_id TEXT NOT NULL,
            app_server_client_name TEXT NOT NULL,
            server_id TEXT NOT NULL,
            environment_id TEXT NOT NULL,
            server_name TEXT NOT NULL,
            updated_at BIGINT NOT NULL,
            remote_control_enabled BOOLEAN,
            PRIMARY KEY (websocket_url, account_id, app_server_client_name)
        )",
        "CREATE TABLE IF NOT EXISTS codex_external_agent_config_imports (
            import_id TEXT PRIMARY KEY,
            completed_at_ms BIGINT NOT NULL,
            successes JSONB NOT NULL,
            failures JSONB NOT NULL,
            provider_id TEXT
        )",
        "CREATE INDEX IF NOT EXISTS codex_external_agent_config_imports_completed ON codex_external_agent_config_imports (completed_at_ms DESC, import_id DESC)",
        "CREATE TABLE IF NOT EXISTS codex_backfill_state (
            id BIGINT PRIMARY KEY CHECK (id = 1),
            status TEXT NOT NULL,
            last_watermark TEXT,
            last_success_at BIGINT,
            updated_at BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS codex_rollout_migration_state (
            migration_id TEXT PRIMARY KEY,
            last_checked_thread_created_at BIGINT,
            last_checked_thread_id TEXT,
            updated_at BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS codex_rollout_migration_skipped_rollouts (
            migration_id TEXT NOT NULL,
            rollout_path TEXT NOT NULL,
            rollout_size_bytes BIGINT NOT NULL,
            rollout_modified_at_ns BIGINT NOT NULL,
            skip_reason TEXT NOT NULL,
            skipped_at BIGINT NOT NULL,
            PRIMARY KEY (migration_id, rollout_path)
        )",
    ],
}];

const MIGRATIONS_TABLE: &str = "CREATE TABLE IF NOT EXISTS codex_schema_migrations (
    version BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at_ms BIGINT NOT NULL
)";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

/// Applies every migration that `codex_schema_migrations` has not recorded.
/// Safe to run concurrently from several processes: statements are
/// idempotent and the version insert ignores conflicts.
pub async fn migrate(sql: &Sql, migrations: &[Migration]) -> AntflyResult<()> {
    sql.execute(MIGRATIONS_TABLE, vec![]).await?;
    let applied: Vec<i64> = sql
        .fetch_all("SELECT version FROM codex_schema_migrations", vec![])
        .await?
        .iter()
        .map(|row| row.i64("version"))
        .collect::<AntflyResult<_>>()?;
    for migration in migrations {
        if applied.contains(&migration.version) {
            continue;
        }
        for statement in migration.statements {
            sql.execute(statement, vec![])
                .await
                .map_err(|err| match err {
                    AntflyError::Sql { code, message } => AntflyError::Sql {
                        code,
                        message: format!(
                            "migration {} ({}): {message}: {}",
                            migration.version,
                            migration.name,
                            statement.split_whitespace().collect::<Vec<_>>().join(" ")
                        ),
                    },
                    other => other,
                })?;
        }
        sql.execute(
            "INSERT INTO codex_schema_migrations (version, name, applied_at_ms) \
             VALUES ($1, $2, $3) ON CONFLICT (version) DO NOTHING",
            sql_params![migration.version, migration.name, now_ms()],
        )
        .await?;
    }
    Ok(())
}
