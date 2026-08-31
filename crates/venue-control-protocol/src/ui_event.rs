use serde::{Deserialize, Serialize};

use crate::{CONTROL_SCHEMA_VERSION, GatewayMode, ProtocolError, VenueId};

/// The account scope a browser must match before treating an event as an invalidation. It has no
/// writer, credential, order, fill, or other exchange fact.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct UiAccountScope {
    pub venue: VenueId,
    #[serde(deserialize_with = "crate::deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
}

impl UiAccountScope {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.mode != GatewayMode::Live
            || !venue_domain::is_canonical_trading_account_id(&self.trading_account_id)
        {
            return Err(ProtocolError::EventScope);
        }
        Ok(())
    }
}

/// A state category changed. Consumers must refetch their scoped snapshot; notifications never
/// contain the underlying control, exchange, position, order, fill, or ledger data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiEventKind {
    Snapshot,
    CopyRelation,
    ExecutionFacts,
    Command,
    Delivery,
}

/// Durable pre-cursor notification saved by Control. Cursor values are assigned only while a
/// scoped replay page is read, so a filtered stream has a verifiable cursor chain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UiEventNotification {
    pub schema_version: u16,
    pub event_type: UiEventKind,
    pub scope: UiAccountScope,
    pub observed_ms: u64,
}

impl UiEventNotification {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION {
            return Err(ProtocolError::SchemaVersion);
        }
        self.scope.validate()?;
        if self.observed_ms == 0 {
            return Err(ProtocolError::EventCursor);
        }
        Ok(())
    }
}

/// Wire event used exclusively by `/v2/ui/events`. `previous_cursor` is the preceding cursor in
/// the same exact account scope; zero denotes the beginning of that scoped stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UiEventEnvelope {
    pub schema_version: u16,
    pub cursor: u64,
    pub previous_cursor: u64,
    pub event_type: UiEventKind,
    pub scope: UiAccountScope,
}

impl UiEventEnvelope {
    pub fn from_notification(
        notification: UiEventNotification,
        cursor: u64,
        previous_cursor: u64,
    ) -> Result<Self, ProtocolError> {
        notification.validate()?;
        let envelope = Self {
            schema_version: notification.schema_version,
            cursor,
            previous_cursor,
            event_type: notification.event_type,
            scope: notification.scope,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION {
            return Err(ProtocolError::SchemaVersion);
        }
        self.scope.validate()?;
        if self.cursor == 0 || self.previous_cursor >= self.cursor {
            return Err(ProtocolError::EventCursor);
        }
        Ok(())
    }

    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}
