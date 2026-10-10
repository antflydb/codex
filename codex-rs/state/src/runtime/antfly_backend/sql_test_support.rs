//! Test harness for the SQL-backed Antfly stores: a `StateRuntime` opened
//! with `init_antfly` on a fresh embedded database in a temp directory.

use std::sync::Arc;

use codex_antfly::Antfly;
use codex_antfly::sql_params;
use codex_protocol::ThreadId;

use crate::StateRuntime;

pub(crate) struct AntflyRuntime {
    pub(crate) runtime: Arc<StateRuntime>,
    pub(crate) antfly: Arc<Antfly>,
    /// Removes the database directory on drop, even when a test panics.
    _dir: tempfile::TempDir,
}

impl AntflyRuntime {
    pub(crate) async fn open() -> Self {
        let dir = tempfile::tempdir().expect("create temp dir");
        let mut config = codex_antfly::AntflyConfig::embedded(dir.path().join("codex.aflite"));
        config.embedder = None;
        let antfly = Arc::new(Antfly::new(config));
        let runtime = StateRuntime::init_antfly(Arc::clone(&antfly), "test-provider".to_string())
            .await
            .expect("init antfly runtime");
        Self {
            runtime,
            antfly,
            _dir: dir,
        }
    }

    /// Inserts a minimal `codex_threads` row (for foreign keys).
    pub(crate) async fn insert_thread(&self, thread_id: ThreadId) {
        self.antfly
            .sql()
            .await
            .expect("sql")
            .execute(
                "INSERT INTO codex_threads (
                    id, rollout_path, created_at, updated_at, created_at_ms, updated_at_ms,
                    recency_at, recency_at_ms, source, model_provider, cwd, title,
                    sandbox_policy, approval_mode
                 ) VALUES ($1, $2, 0, 0, 0, 0, 0, 0, 'cli', 'test-provider', '/tmp', '',
                    'read-only', 'never')",
                sql_params![thread_id.to_string(), format!("/tmp/{thread_id}.jsonl")],
            )
            .await
            .expect("insert thread row");
    }

    /// Closes the runtime and the database; dropping `self` removes the
    /// directory.
    pub(crate) async fn close(self) {
        self.runtime.close().await;
        self.antfly.close().await.expect("close antfly");
    }
}
