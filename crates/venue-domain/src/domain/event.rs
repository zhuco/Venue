use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::domain::{AccountBalance, Amount, Fill, Instrument, Order, Position};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventId(String);

impl EventId {
    pub fn new(raw: impl Into<String>) -> Result<Self, EventIdError> {
        let value = raw.into();
        if value.trim().is_empty() {
            Err(EventIdError::Empty)
        } else {
            Ok(Self(value))
        }
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for EventId {
    type Err = EventIdError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::new(raw)
    }
}

impl Serialize for EventId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    PublicMarket,
    PrivateAccount,
    Readback,
    Recovery,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventHeader {
    pub schema_version: u16,
    pub event_id: EventId,
    pub source: EventSource,
    pub source_sequence: Option<u64>,
    pub received_at_ms: u64,
    pub generation: u64,
}

impl EventHeader {
    pub fn validate(&self) -> Result<(), EventIdError> {
        if self.schema_version == 0 || self.generation == 0 {
            return Err(EventIdError::InvalidHeader);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "value")]
pub enum FieldState<T> {
    #[default]
    Missing,
    Null,
    Known(T),
    Unavailable {
        reason: UnknownReason,
    },
    NotApplicable,
}

impl<T> FieldState<T> {
    #[must_use]
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownReason {
    SourceOmitted,
    PermissionDenied,
    VenueUnavailable,
    ParseFailure,
    Ambiguous,
    NotYetObserved,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "payload")]
pub enum DomainEvent {
    Instrument(Instrument),
    Order(Order),
    Fill(Fill),
    Position(Position),
    Balance(AccountBalance),
    Funding(Amount),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FactRecord {
    pub header: EventHeader,
    pub event: DomainEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EventIdError {
    #[error("event identifier must not be empty")]
    Empty,
    #[error("event schema version and generation must be positive")]
    InvalidHeader,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_state_round_trips_without_collapsing_unknown() -> Result<(), serde_json::Error> {
        let state: FieldState<String> = FieldState::Unavailable {
            reason: UnknownReason::PermissionDenied,
        };
        let decoded: FieldState<String> = serde_json::from_str(&serde_json::to_string(&state)?)?;

        assert_eq!(decoded, state);
        Ok(())
    }
}
