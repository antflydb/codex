//! One structured request to the memory extraction model outside any thread
//! turn, made exactly as stage 1 makes its requests. Callers outside the
//! memory pipeline (the Antfly claim pipeline) use this instead of building
//! their own model client.

use crate::runtime::MemoryStartupContext;
use codex_core::CodexThread;
use codex_core::Prompt;
use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_protocol::ResponseItemId;
use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use serde_json::Value;
use std::sync::Arc;

/// Sends `input` with `instructions` to the configured memory extraction
/// model (`memories.extract_model`, else the provider's preferred one) and
/// returns the text of a response constrained to `output_schema`.
///
/// `host_thread` is a live thread whose session settings (source,
/// originator, service tier) attribute the request, as stage 1 attributes
/// its requests to the thread that started it.
pub async fn sample_structured(
    thread_manager: Arc<ThreadManager>,
    host_thread_id: ThreadId,
    host_thread: Arc<CodexThread>,
    config: &Config,
    instructions: String,
    input: &str,
    output_schema: Value,
) -> anyhow::Result<String> {
    let session_source = host_thread.config_snapshot().await.session_source;
    let auth_manager = thread_manager.auth_manager();
    let context = MemoryStartupContext::new(
        thread_manager,
        auth_manager,
        host_thread_id,
        host_thread,
        config,
        session_source,
    );
    let model_name = config.memories.extract_model.clone().unwrap_or_else(|| {
        context
            .provider()
            .memory_extraction_preferred_model()
            .to_string()
    });
    let request = context
        .stage_one_request_context(config, &model_name, crate::stage_one::REASONING_EFFORT)
        .await;
    let message = |role: &str, text: String| ResponseItem::Message {
        status: None,
        encrypted_content: None,
        id: Some(ResponseItemId::new("msg")),
        role: role.to_string(),
        content: vec![ContentItem::InputText { text }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };
    // Plain messages without content-item metadata: the request builder
    // turns non-empty base instructions into a metadata-carrying fragment
    // for Responses Lite models, which the HTTP endpoint can reject.
    let mut prompt = Prompt::default();
    // The default is Codex's full agent prompt, not empty.
    prompt.base_instructions = BaseInstructions {
        text: String::new(),
        provenance: None,
    };
    prompt.input = vec![
        message("developer", instructions),
        message("user", input.to_string()),
    ];
    prompt.output_schema = Some(output_schema);
    prompt.output_schema_strict = true;
    let (text, _usage) = context
        .stream_stage_one_prompt(config, &prompt, &request)
        .await?;
    Ok(text)
}
