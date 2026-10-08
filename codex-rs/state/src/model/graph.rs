use serde::Deserialize;
use serde::Serialize;
use strum::AsRefStr;
use strum::Display;
use strum::EnumString;

/// Status attached to a directional thread-spawn edge.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display, EnumString, Serialize, Deserialize,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum DirectionalThreadSpawnEdgeStatus {
    Open,
    Closed,
}
