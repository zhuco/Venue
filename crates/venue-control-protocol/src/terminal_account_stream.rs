//! Connection-local account updates; retained history is bound to the preceding snapshot.
use crate::kol::{KolProtocolError, TerminalAccountProjection};
use serde::{Deserialize, Serialize};

pub const COMPACT_QUERY: &str = "compact=1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TerminalAccountStreamEvent {
    Snapshot(Option<TerminalAccountProjection>),
    Update {
        base_observed_ms: u64,
        projection: TerminalAccountProjection,
        retain_fills: bool,
        retain_position_history: bool,
    },
}

impl TerminalAccountStreamEvent {
    pub fn between(
        previous: Option<&TerminalAccountProjection>,
        current: Option<&TerminalAccountProjection>,
    ) -> Self {
        let (Some(previous), Some(current)) = (previous, current) else {
            return Self::Snapshot(current.cloned());
        };
        if !same_scope(previous, current) || current.observed_ms < previous.observed_ms {
            return Self::Snapshot(Some(current.clone()));
        }
        let retain_fills = previous.fills == current.fills;
        let retain_position_history = previous.position_history == current.position_history;
        let mut projection = current.clone();
        if retain_fills {
            projection.fills.clear();
        }
        if retain_position_history {
            projection.position_history.clear();
        }
        Self::Update {
            base_observed_ms: previous.observed_ms,
            projection,
            retain_fills,
            retain_position_history,
        }
    }

    /// No clock is synthesized: the complete current facts and their observation time
    /// come from the server. A new connection must start with a full snapshot.
    pub fn apply(
        self,
        previous: &mut Option<TerminalAccountProjection>,
    ) -> Result<(), KolProtocolError> {
        let next = match self {
            Self::Snapshot(projection) => projection,
            Self::Update {
                base_observed_ms,
                mut projection,
                retain_fills,
                retain_position_history,
            } => {
                let base = previous
                    .as_ref()
                    .ok_or(KolProtocolError::TerminalProjection)?;
                if !same_scope(base, &projection)
                    || base.observed_ms != base_observed_ms
                    || projection.observed_ms < base.observed_ms
                    || (retain_fills && !projection.fills.is_empty())
                    || (retain_position_history && !projection.position_history.is_empty())
                {
                    return Err(KolProtocolError::TerminalProjection);
                }
                if retain_fills {
                    projection.fills.clone_from(&base.fills);
                }
                if retain_position_history {
                    projection
                        .position_history
                        .clone_from(&base.position_history);
                }
                Some(projection)
            }
        };
        if let Some(projection) = &next {
            projection.validate()?;
        }
        *previous = next;
        Ok(())
    }
}

fn same_scope(a: &TerminalAccountProjection, b: &TerminalAccountProjection) -> bool {
    a.credential_id == b.credential_id
        && a.trading_account_id == b.trading_account_id
        && a.private_generation == b.private_generation
}

#[cfg(test)]
#[path = "terminal_account_stream_tests.rs"]
mod tests;
