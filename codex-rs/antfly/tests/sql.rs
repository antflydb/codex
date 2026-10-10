//! Exercises the SQL layer against a real embedded `libantfly`.

use codex_antfly::Antfly;
use codex_antfly::AntflyConfig;
use codex_antfly::Write;
use codex_antfly::sql::Sql;
use codex_antfly::sql::SqlValue;
use codex_antfly::sql_params;
use pretty_assertions::assert_eq;
use serde_json::json;

fn runtime() -> tokio::runtime::Runtime {
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => panic!("tokio runtime: {err}"),
    }
}

#[test]
fn sql_round_trips_types_transactions_and_errors() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("codex.aflite");
    runtime().block_on(async {
        // The document backend and the SQL pool share the one file.
        let mut config = AntflyConfig::embedded(&path);
        config.embedder = None;
        let antfly = Antfly::open(config)?;
        antfly
            .documents(codex_antfly::schema::HISTORY_ITEMS)
            .write(vec![Write::put("doc:1", json!({"text": "hello"}))])
            .await?;
        let sql = Sql::connect_embedded(&path, true).await?;

        sql.execute(
            "CREATE TABLE IF NOT EXISTS codex_things (id TEXT PRIMARY KEY, n BIGINT NOT NULL, \
             ratio DOUBLE PRECISION, flag BOOLEAN NOT NULL DEFAULT false, meta JSONB, note TEXT)",
            vec![],
        )
        .await?;
        for _ in 0..2 {
            sql.execute(
                "CREATE INDEX IF NOT EXISTS codex_things_by_n ON codex_things (flag, n DESC, id DESC)",
                vec![],
            )
            .await?;
        }

        let inserted = sql
            .fetch_all(
                "INSERT INTO codex_things (id, n, ratio, flag, meta, note) VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (id) DO UPDATE SET n = excluded.n RETURNING n",
                sql_params!["a", 9_007_199_254_740_993_i64, 0.5, true, json!({"k": [1, 2]}), None::<String>],
            )
            .await?;
        assert_eq!(inserted[0].i64("n")?, 9_007_199_254_740_993);
        sql.execute(
            "INSERT INTO codex_things (id, n) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET n = excluded.n",
            sql_params!["a", 2_i64],
        )
        .await?;

        let row = sql
            .fetch_optional("SELECT * FROM codex_things WHERE id = $1", sql_params!["a"])
            .await?
            .ok_or("missing row")?;
        assert_eq!(row.i64("n")?, 2);
        assert_eq!(row.opt_f64("ratio")?, Some(0.5));
        assert!(row.bool("flag")?);
        assert_eq!(row.json("meta")?, json!({"k": [1, 2]}));
        assert_eq!(row.get("note")?, &SqlValue::Null);

        // Transactions: rollback discards, commit publishes.
        let mut tx = sql.begin().await?;
        tx.execute("INSERT INTO codex_things (id, n) VALUES ('b', 1)", vec![])
            .await?;
        tx.rollback().await?;
        let mut tx = sql.begin().await?;
        tx.execute("INSERT INTO codex_things (id, n) VALUES ('c', 3)", vec![])
            .await?;
        tx.commit().await?;
        let ids: Vec<String> = sql
            .fetch_all("SELECT id FROM codex_things ORDER BY id", vec![])
            .await?
            .iter()
            .map(|row| row.string("id"))
            .collect::<Result<_, _>>()?;
        assert_eq!(ids, vec!["a".to_string(), "c".to_string()]);

        let duplicate = sql
            .execute("INSERT INTO codex_things (id, n) VALUES ('a', 1)", vec![])
            .await
            .err()
            .ok_or("duplicate insert succeeded")?;
        assert!(duplicate.is_unique_violation(), "{duplicate}");

        // The document API still works next to the SQL pool.
        assert_eq!(
            antfly
                .documents(codex_antfly::schema::HISTORY_ITEMS)
                .get("doc:1")
                .await?,
            Some(json!({"text": "hello"}))
        );
        sql.close().await;
        antfly.close().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn migrations_create_every_table_and_rerun_cleanly() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("codex.aflite");
    runtime().block_on(async {
        let sql = Sql::connect_embedded(&path, true).await?;
        codex_antfly::schema::migrate(&sql, codex_antfly::schema::MIGRATIONS).await?;
        codex_antfly::schema::migrate(&sql, codex_antfly::schema::MIGRATIONS).await?;
        let pinned = sql
            .fetch_optional(
                "SELECT name FROM codex_thread_sections WHERE id = $1",
                sql_params![codex_antfly::schema::PINNED_SECTION_ID],
            )
            .await?
            .ok_or("missing pinned section")?;
        assert_eq!(pinned.string("name")?, "Pinned");
        let versions = sql
            .fetch_all("SELECT version FROM codex_schema_migrations", vec![])
            .await?;
        assert_eq!(versions.len(), codex_antfly::schema::MIGRATIONS.len());
        // Foreign keys: deleting a section clears it from threads.
        sql.execute(
            "INSERT INTO codex_thread_sections (id, name) VALUES ('s1', 'Work')",
            vec![],
        )
        .await?;
        sql.execute(
            "INSERT INTO codex_threads (id, rollout_path, created_at, updated_at, created_at_ms, \
             updated_at_ms, recency_at, recency_at_ms, source, model_provider, cwd, title, \
             sandbox_policy, approval_mode, thread_section_id) VALUES ('t1', '', 1, 1, 1000, 1000, \
             1, 1000, 'cli', 'openai', '/w', 'T', 'p', 'a', 's1')",
            vec![],
        )
        .await?;
        sql.execute("DELETE FROM codex_thread_sections WHERE id = 's1'", vec![])
            .await?;
        let thread = sql
            .fetch_optional(
                "SELECT thread_section_id, archived FROM codex_threads WHERE id = 't1'",
                vec![],
            )
            .await?
            .ok_or("missing thread")?;
        assert_eq!(thread.opt_string("thread_section_id")?, None);
        assert!(!thread.bool("archived")?);
        sql.close().await;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
