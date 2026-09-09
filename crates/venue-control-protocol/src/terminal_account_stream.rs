//! Connection-local changes bound to the preceding owner-checked account snapshot.
use crate::kol::{
    KolProtocolError, TerminalAccountProjection, TerminalAsset, TerminalConditionalOrder,
    TerminalFill, TerminalOpenOrder, TerminalPosition, TerminalPositionHistoryEntry,
    TerminalPositionMode,
};
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
        credential_id: String,
        trading_account_id: String,
        private_generation: u64,
        observed_ms: u64,
        persisted_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position_mode: Option<TerminalPositionMode>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        positions: Option<Vec<TerminalPosition>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        open_orders: Option<Vec<TerminalOpenOrder>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conditional_orders: Option<Vec<TerminalConditionalOrder>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fills: Option<Vec<TerminalFill>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position_history: Option<Vec<TerminalPositionHistoryEntry>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        assets: Option<Vec<TerminalAsset>>,
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
        Self::Update {
            base_observed_ms: previous.observed_ms,
            credential_id: current.credential_id.clone(),
            trading_account_id: current.trading_account_id.clone(),
            private_generation: current.private_generation,
            observed_ms: current.observed_ms,
            persisted_ms: current.persisted_ms,
            position_mode: changed(&previous.position_mode, &current.position_mode),
            positions: changed(&previous.positions, &current.positions),
            open_orders: changed(&previous.open_orders, &current.open_orders),
            conditional_orders: changed(&previous.conditional_orders, &current.conditional_orders),
            fills: changed(&previous.fills, &current.fills),
            position_history: changed(&previous.position_history, &current.position_history),
            assets: changed(&previous.assets, &current.assets),
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
                credential_id,
                trading_account_id,
                private_generation,
                observed_ms,
                persisted_ms,
                position_mode,
                positions,
                open_orders,
                conditional_orders,
                fills,
                position_history,
                assets,
            } => {
                let base = previous
                    .as_ref()
                    .ok_or(KolProtocolError::TerminalProjection)?;
                if base.credential_id != credential_id
                    || base.trading_account_id != trading_account_id
                    || base.private_generation != private_generation
                    || base.observed_ms != base_observed_ms
                    || observed_ms < base.observed_ms
                {
                    return Err(KolProtocolError::TerminalProjection);
                }
                let mut projection = base.clone();
                projection.observed_ms = observed_ms;
                projection.persisted_ms = persisted_ms;
                if let Some(value) = position_mode {
                    projection.position_mode = value;
                }
                if let Some(value) = positions {
                    projection.positions = value;
                }
                if let Some(value) = open_orders {
                    projection.open_orders = value;
                }
                if let Some(value) = conditional_orders {
                    projection.conditional_orders = value;
                }
                if let Some(value) = fills {
                    projection.fills = value;
                }
                if let Some(value) = position_history {
                    projection.position_history = value;
                }
                if let Some(value) = assets {
                    projection.assets = value;
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

fn changed<T: PartialEq + Clone>(previous: &T, current: &T) -> Option<T> {
    (previous != current).then(|| current.clone())
}

fn same_scope(a: &TerminalAccountProjection, b: &TerminalAccountProjection) -> bool {
    a.credential_id == b.credential_id
        && a.trading_account_id == b.trading_account_id
        && a.private_generation == b.private_generation
}

#[cfg(test)]
#[path = "terminal_account_stream_tests.rs"]
mod tests;
