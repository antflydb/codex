//! Version-specific extraction contracts; v2 never decodes raw memory.

use codex_protocol::MemoryVersion;
use codex_secrets::redact_secrets;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::truncate_text;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

/// Claim kinds the knowledge graph accepts (see `codex_antfly::knowledge`).
const CLAIM_KINDS: [&str; 6] = [
    "decision",
    "rationale",
    "preference",
    "lesson",
    "constraint",
    "open_question",
];
const MAX_CLAIMS: usize = 12;
const MAX_CLAIM_CHARS: usize = 500;

pub(crate) struct StageOneOutput {
    pub(crate) raw_memory: Option<String>,
    pub(crate) rollout_summary: String,
    pub(crate) rollout_slug: Option<String>,
    pub(crate) claims: Vec<codex_state::MemoryClaim>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputClaim {
    kind: String,
    statement: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyOutput {
    raw_memory: String,
    rollout_summary: String,
    #[serde(default)]
    rollout_slug: Option<String>,
    #[serde(default)]
    claims: Vec<OutputClaim>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SummaryOutput {
    rollout_summary: String,
    rollout_slug: String,
    #[serde(default)]
    claims: Vec<OutputClaim>,
}

/// Keeps known kinds and non-empty statements, redacted and bounded.
fn sanitize_claims(claims: Vec<OutputClaim>) -> Vec<codex_state::MemoryClaim> {
    claims
        .into_iter()
        .filter(|claim| CLAIM_KINDS.contains(&claim.kind.as_str()))
        .filter_map(|claim| {
            let statement = redact_secrets(claim.statement.trim().to_string());
            let statement = match statement.char_indices().nth(MAX_CLAIM_CHARS) {
                Some((end, _)) => statement[..end].to_string(),
                None => statement,
            };
            (!statement.is_empty()).then_some(codex_state::MemoryClaim {
                kind: claim.kind,
                statement,
            })
        })
        .take(MAX_CLAIMS)
        .collect()
}

fn claims_schema() -> Value {
    json!({
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "kind": { "type": "string", "enum": CLAIM_KINDS },
                "statement": { "type": "string" }
            },
            "required": ["kind", "statement"],
            "additionalProperties": false
        }
    })
}

impl StageOneOutput {
    pub(crate) fn parse(source: &str, version: MemoryVersion) -> serde_json::Result<Self> {
        let (raw_memory, rollout_summary, rollout_slug, claims) = match version {
            MemoryVersion::V1 => {
                let output: LegacyOutput = serde_json::from_str(source)?;
                (
                    Some(output.raw_memory),
                    output.rollout_summary,
                    output.rollout_slug,
                    output.claims,
                )
            }
            MemoryVersion::V2 => {
                let output: SummaryOutput = serde_json::from_str(source)?;
                (
                    None,
                    output.rollout_summary,
                    Some(output.rollout_slug),
                    output.claims,
                )
            }
        };
        let mut rollout_summary = redact_secrets(rollout_summary);
        if version == MemoryVersion::V2 {
            rollout_summary = truncate_text(&rollout_summary, TruncationPolicy::Bytes(9_000));
        }
        Ok(Self {
            raw_memory: raw_memory.map(redact_secrets),
            rollout_summary,
            rollout_slug: rollout_slug.map(redact_secrets),
            claims: sanitize_claims(claims),
        })
    }
}

pub(crate) fn output_schema(version: MemoryVersion) -> Value {
    match version {
        MemoryVersion::V1 => json!({
            "type": "object",
            "properties": {
                "rollout_summary": { "type": "string" },
                "rollout_slug": { "type": ["string", "null"] },
                "raw_memory": { "type": "string" },
                "claims": claims_schema()
            },
            "required": ["rollout_summary", "rollout_slug", "raw_memory", "claims"],
            "additionalProperties": false
        }),
        MemoryVersion::V2 => json!({
            "type": "object",
            "properties": {
                "rollout_summary": { "type": "string" },
                "rollout_slug": { "type": "string" },
                "claims": claims_schema()
            },
            "required": ["rollout_summary", "rollout_slug", "claims"],
            "additionalProperties": false
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn claims_are_parsed_filtered_and_bounded() {
        let long = "x".repeat(MAX_CLAIM_CHARS + 10);
        let mut claims = vec![
            json!({"kind": "decision", "statement": "  Codex uses EmbeddingGemma 2.  "}),
            json!({"kind": "gossip", "statement": "Unknown kinds are dropped."}),
            json!({"kind": "lesson", "statement": "   "}),
            json!({"kind": "constraint", "statement": long}),
        ];
        claims.extend((0..20).map(|i| json!({"kind": "preference", "statement": format!("p{i}")})));
        let source = json!({
            "rollout_summary": "summary",
            "rollout_slug": "slug",
            "claims": claims,
        })
        .to_string();
        let output = StageOneOutput::parse(&source, MemoryVersion::V2).expect("parse");
        assert_eq!(output.claims.len(), MAX_CLAIMS);
        assert_eq!(
            output.claims[0],
            codex_state::MemoryClaim {
                kind: "decision".to_string(),
                statement: "Codex uses EmbeddingGemma 2.".to_string(),
            }
        );
        assert_eq!(output.claims[1].kind, "constraint");
        assert_eq!(output.claims[1].statement.chars().count(), MAX_CLAIM_CHARS);
    }

    #[test]
    fn outputs_without_claims_still_parse() {
        let source = json!({"rollout_summary": "s", "rollout_slug": "slug", "raw_memory": "m"});
        let output = StageOneOutput::parse(&source.to_string(), MemoryVersion::V1).expect("parse");
        assert!(output.claims.is_empty());
    }
}
