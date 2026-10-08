//! Lineage flattening over synthetic rollout files.

use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::UserMessageEvent;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;
use serde_json::Value;

const TIMESTAMP: &str = "2026-10-07T12-00-00";

fn line(ordinal: u64, item: &RolloutItem) -> String {
    let mut value = serde_json::to_value(item).unwrap_or(Value::Null);
    if let Some(map) = value.as_object_mut() {
        map.insert(
            "timestamp".to_string(),
            Value::String("2026-10-07T12:00:00.000Z".to_string()),
        );
        map.insert("ordinal".to_string(), Value::from(ordinal));
    }
    value.to_string()
}

fn meta(thread_id: ThreadId, base: Option<HistoryPosition>) -> RolloutItem {
    RolloutItem::SessionMeta(SessionMetaLine {
        meta: SessionMeta {
            session_id: thread_id.into(),
            id: thread_id,
            history_mode: ThreadHistoryMode::Paginated,
            history_base: base,
            ..SessionMeta::default()
        },
        git: None,
    })
}

fn user(text: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: text.to_string(),
        ..Default::default()
    }))
}

fn user_texts(items: &[RolloutItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::EventMsg(EventMsg::UserMessage(event)) => Some(event.message.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn reverted_thread_flattens_visible_history() -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let day = home.path().join("sessions/2026/10/07");
    std::fs::create_dir_all(&day)?;
    let thread = ThreadId::new();
    let reverted = ThreadId::new();

    // First segment: ordinals 1..=4. A revert before ordinal 3 starts a new
    // segment whose SessionMeta sits at ordinal 3.
    let first: Vec<String> = [
        line(0, &meta(thread, None)),
        line(1, &user("one")),
        line(2, &user("two")),
        line(3, &user("three (reverted)")),
        line(4, &user("four (reverted)")),
    ]
    .into();
    std::fs::write(
        day.join(format!("rollout-{TIMESTAMP}-{thread}.jsonl")),
        first.join("\n"),
    )?;
    let base = HistoryPosition {
        thread_id: thread,
        end_ordinal_exclusive: 3,
        end_byte_offset: 0,
    };
    let second: Vec<String> = [
        line(3, &meta(thread, Some(base))),
        line(4, &user("three again")),
    ]
    .into();
    std::fs::write(
        day.join(format!("rollout-{TIMESTAMP}-{thread}_{reverted}.jsonl")),
        second.join("\n"),
    )?;

    let plan = codex_antfly_import::load(home.path()).await?;
    assert_eq!(plan.rollout_files, 2);
    assert_eq!(plan.threads.len(), 1);
    assert!(plan.skipped.is_empty());
    let imported = plan.threads[0].materialize().await?;
    assert_eq!(imported.created.thread_id, thread);
    assert_eq!(imported.created.history_base, None);
    assert!(matches!(imported.items[0], RolloutItem::SessionMeta(_)));
    assert_eq!(
        user_texts(&imported.items),
        vec!["one", "two", "three again"]
    );
    Ok(())
}
