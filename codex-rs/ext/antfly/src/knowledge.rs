//! `knowledge.search`: the model's view of the Antfly knowledge graph.
//!
//! Returns current claims (decisions, rationale, preferences, lessons,
//! constraints, open questions) distilled from earlier sessions, and memory
//! notes, that share concepts with the query (see `codex_antfly::knowledge`).

use std::sync::Arc;

use codex_antfly::Antfly;
use codex_core::config::Config;
use codex_core::config::ThreadStoreConfig;
use codex_extension_api::ConfigContributor;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::FunctionCallError;
use codex_extension_api::JsonToolOutput;
use codex_extension_api::ResponsesApiTool;
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadStartInput;
use codex_extension_api::ToolCall;
use codex_extension_api::ToolContributor;
use codex_extension_api::ToolExecutor;
use codex_extension_api::ToolName;
use codex_extension_api::ToolSpec;
use codex_extension_api::parse_tool_input_schema;
use codex_tools::ResponsesApiNamespace;
use codex_tools::ResponsesApiNamespaceTool;
use codex_tools::default_namespace_description;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

pub const KNOWLEDGE_TOOLS_NAMESPACE: &str = "knowledge";
pub const SEARCH_TOOL_NAME: &str = "search";
const DEFAULT_MAX_RESULTS: usize = 8;
const MAX_RESULTS: usize = 25;

/// The thread's Antfly handle, present when the thread store is Antfly and
/// the knowledge graph is enabled.
#[derive(Clone)]
struct KnowledgeState {
    antfly: Arc<Antfly>,
}

impl KnowledgeState {
    fn from_config(config: &Config) -> Option<Self> {
        let ThreadStoreConfig::Antfly(antfly) = &config.experimental_thread_store else {
            return None;
        };
        antfly.knowledge.as_ref()?;
        Some(Self {
            antfly: codex_antfly::shared(antfly),
        })
    }
}

pub(crate) struct KnowledgeExtension;

impl ThreadLifecycleContributor<Config> for KnowledgeExtension {
    fn on_thread_start<'a>(
        &'a self,
        input: ThreadStartInput<'a, Config>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if let Some(state) = KnowledgeState::from_config(input.config) {
                input.thread_store.insert(state);
            }
        })
    }
}

impl ConfigContributor<Config> for KnowledgeExtension {
    fn on_config_changed(
        &self,
        _session_store: &ExtensionData,
        thread_store: &ExtensionData,
        _previous_config: &Config,
        new_config: &Config,
    ) {
        match KnowledgeState::from_config(new_config) {
            Some(state) => {
                thread_store.insert(state);
            }
            None => {
                thread_store.remove::<KnowledgeState>();
            }
        }
    }
}

impl ToolContributor for KnowledgeExtension {
    fn tools(
        &self,
        _session_store: &ExtensionData,
        thread_store: &ExtensionData,
    ) -> Vec<Arc<dyn for<'call> ToolExecutor<ToolCall<'call>>>> {
        match thread_store.get::<KnowledgeState>() {
            Some(state) => vec![Arc::new(SearchTool {
                antfly: Arc::clone(&state.antfly),
            })],
            None => Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArgs {
    query: String,
    max_results: Option<usize>,
    include_history: Option<bool>,
}

struct SearchTool {
    antfly: Arc<Antfly>,
}

fn search_spec() -> ToolSpec {
    let parameters = json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "What you want to know, in natural language: a topic, decision or question.",
            },
            "max_results": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_RESULTS,
                "description": "Maximum claims and notes to return each (default 8).",
            },
            "include_history": {
                "type": "boolean",
                "description": "Also return superseded claims, with the claim that replaced each (default false).",
            },
        },
        "required": ["query"],
        "additionalProperties": false,
    });
    let tool = ResponsesApiTool {
        name: SEARCH_TOOL_NAME.to_string(),
        description: "Search knowledge distilled from earlier sessions: decisions, rationale, \
            user preferences, lessons, constraints and open questions, plus related memory notes. \
            Results are linked by shared concepts and only current claims are returned unless \
            include_history is set. Each claim names the thread (and turn) it came from; claims \
            marked provisional come from a session that is still active and may still change. \
            Decisions include the rationale that motivated them and the alternatives rejected in \
            their favor, when known."
            .to_string(),
        strict: false,
        defer_loading: None,
        parameters: parse_tool_input_schema(&parameters)
            .unwrap_or_else(|err| panic!("knowledge.search schema should parse: {err}")),
        output_schema: None,
    };
    ToolSpec::Namespace(ResponsesApiNamespace {
        name: KNOWLEDGE_TOOLS_NAMESPACE.to_string(),
        description: default_namespace_description(KNOWLEDGE_TOOLS_NAMESPACE),
        tools: vec![ResponsesApiNamespaceTool::Function(tool)],
    })
}

impl<'call> ToolExecutor<ToolCall<'call>> for SearchTool {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced(KNOWLEDGE_TOOLS_NAMESPACE, SEARCH_TOOL_NAME)
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        true
    }

    fn spec(&self) -> ToolSpec {
        search_spec()
    }

    fn handle<'a>(&'a self, call: ToolCall<'call>) -> codex_extension_api::ToolExecutorFuture<'a>
    where
        'call: 'a,
    {
        Box::pin(async move {
            let arguments = call.function_arguments()?;
            let args: SearchArgs = serde_json::from_str(arguments)
                .map_err(|err| FunctionCallError::RespondToModel(err.to_string()))?;
            let query = args.query.trim();
            if query.is_empty() {
                return Err(FunctionCallError::RespondToModel(
                    "query must not be empty".to_string(),
                ));
            }
            let knowledge = self.antfly.knowledge().ok_or_else(|| {
                FunctionCallError::RespondToModel("the knowledge graph is disabled".to_string())
            })?;
            let limit = args
                .max_results
                .unwrap_or(DEFAULT_MAX_RESULTS)
                .clamp(1, MAX_RESULTS);
            let related = knowledge
                .related(query, limit, args.include_history.unwrap_or(false))
                .await
                .map_err(|err| {
                    FunctionCallError::RespondToModel(format!("search failed: {err}"))
                })?;
            // Descriptions are context, not results: a lookup failure only
            // leaves them out.
            let descriptions = knowledge
                .concept_descriptions(&related.seeds)
                .await
                .unwrap_or_default();
            Ok(
                Box::new(JsonToolOutput::new(render(&related, &descriptions)))
                    as Box<dyn codex_extension_api::ToolOutput>,
            )
        })
    }
}

/// The model-facing result: concepts without their key prefix, and claims
/// without storage bookkeeping.
fn render(
    related: &codex_antfly::knowledge::Related,
    descriptions: &std::collections::HashMap<String, String>,
) -> Value {
    let concept = |key: &String| {
        key.strip_prefix(codex_antfly::knowledge::CONCEPT_PREFIX)
            .unwrap_or(key)
            .to_string()
    };
    let mut out = json!({
        "concepts": related.seeds.iter().map(concept).collect::<Vec<_>>(),
        "claims": related.claims.iter().map(|found| {
            let claim = &found.claim;
            let mut out = json!({
                "id": claim.id,
                "kind": claim.kind.as_str(),
                "statement": claim.statement,
                "status": claim.status,
                "thread_id": claim.thread_id,
                "shared_concepts": found.concepts.iter().map(concept).collect::<Vec<_>>(),
            });
            if let Some(cwd) = &claim.cwd {
                out["cwd"] = json!(cwd);
            }
            if !claim.supersedes.is_empty() {
                out["supersedes"] = json!(claim.supersedes);
            }
            if let Some(later) = &claim.superseded_by {
                out["superseded_by"] = json!(later);
            }
            if let Some(turn_id) = &claim.turn_id {
                out["turn_id"] = json!(turn_id);
            }
            if !claim.settled {
                out["provisional"] = json!(true);
            }
            let brief = |claims: &[codex_antfly::knowledge::Claim]| {
                claims
                    .iter()
                    .map(|claim| json!({"id": claim.id, "kind": claim.kind.as_str(), "statement": claim.statement}))
                    .collect::<Vec<_>>()
            };
            if !found.motivated_by.is_empty() {
                out["motivated_by"] = json!(brief(&found.motivated_by));
            }
            if !found.alternatives.is_empty() {
                out["rejected_alternatives"] = json!(brief(&found.alternatives));
            }
            out
        }).collect::<Vec<_>>(),
        "notes": related.notes.iter().map(|note| json!({
            "namespace": note.namespace,
            "path": note.path,
            "shared_concepts": note.concepts.iter().map(concept).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    });
    let described: serde_json::Map<String, Value> = related
        .seeds
        .iter()
        .filter_map(|key| Some((concept(key), json!(descriptions.get(key)?))))
        .collect();
    if !described.is_empty() {
        out["concept_descriptions"] = Value::Object(described);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_antfly::knowledge::Claim;
    use codex_antfly::knowledge::ClaimKind;
    use codex_antfly::knowledge::ClaimStatus;
    use codex_antfly::knowledge::Related;
    use codex_antfly::knowledge::RelatedClaim;
    use codex_antfly::knowledge::RelatedNote;
    use pretty_assertions::assert_eq;

    #[test]
    fn render_strips_concept_prefixes_and_bookkeeping() {
        let related = Related {
            seeds: vec!["concept/embeddinggemma_2".to_string()],
            claims: vec![RelatedClaim {
                claim: Claim {
                    id: "v2:t:0000".to_string(),
                    kind: ClaimKind::Decision,
                    statement: "Codex uses EmbeddingGemma 2.".to_string(),
                    status: ClaimStatus::Current,
                    thread_id: "t".to_string(),
                    version: "v2".to_string(),
                    cwd: Some("/repo".to_string()),
                    source_updated_at_ms: 1,
                    generated_at_ms: 2,
                    supersedes: vec!["v2:s:0000".to_string()],
                    superseded_by: None,
                    motivates: Vec::new(),
                    rejected_in_favor_of: Vec::new(),
                    turn_id: Some("turn-1".to_string()),
                    turn_ordinal: Some(4),
                    settled: false,
                },
                concepts: vec!["concept/embeddinggemma_2".to_string()],
                score: 1.0,
                motivated_by: vec![Claim {
                    id: "turns:t:4:001".to_string(),
                    kind: ClaimKind::Rationale,
                    statement: "Antfly detects its dimensions.".to_string(),
                    status: ClaimStatus::Current,
                    thread_id: "t".to_string(),
                    version: "turns".to_string(),
                    cwd: None,
                    source_updated_at_ms: 1,
                    generated_at_ms: 2,
                    supersedes: Vec::new(),
                    superseded_by: None,
                    motivates: vec!["v2:t:0000".to_string()],
                    rejected_in_favor_of: Vec::new(),
                    turn_id: None,
                    turn_ordinal: None,
                    settled: true,
                }],
                alternatives: Vec::new(),
            }],
            notes: vec![RelatedNote {
                namespace: "memories".to_string(),
                path: "MEMORY.md".to_string(),
                concepts: vec!["concept/embeddinggemma_2".to_string()],
                score: 1.0,
            }],
        };
        assert_eq!(
            render(&related, &std::collections::HashMap::new()),
            json!({
                "concepts": ["embeddinggemma_2"],
                "claims": [{
                    "id": "v2:t:0000",
                    "kind": "decision",
                    "statement": "Codex uses EmbeddingGemma 2.",
                    "status": "current",
                    "thread_id": "t",
                    "cwd": "/repo",
                    "shared_concepts": ["embeddinggemma_2"],
                    "supersedes": ["v2:s:0000"],
                    "turn_id": "turn-1",
                    "provisional": true,
                    "motivated_by": [{"id": "turns:t:4:001", "kind": "rationale", "statement": "Antfly detects its dimensions."}],
                }],
                "notes": [{
                    "namespace": "memories",
                    "path": "MEMORY.md",
                    "shared_concepts": ["embeddinggemma_2"],
                }],
            })
        );
    }

    #[test]
    fn search_spec_is_namespaced() {
        let ToolSpec::Namespace(namespace) = search_spec() else {
            panic!("expected a namespace tool");
        };
        assert_eq!(namespace.name, KNOWLEDGE_TOOLS_NAMESPACE);
    }
}
