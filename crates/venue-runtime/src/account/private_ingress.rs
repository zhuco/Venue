use std::path::PathBuf;

use venue_storage::Journal;

use crate::{
    domain::{DomainEvent, EventHeader, EventId, EventSource, FactRecord, NativeOrderFamily},
    strategy::{PersistedPrivateFact, validate_private_domain_event},
};

/// One normalized private event supplied by an adapter feed.  It contains no durability claim:
/// [`AccountPrivateIngress`] writes it to the account facts journal before Runtime may route it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountPrivateFactInput {
    event_id: EventId,
    generation: u64,
    received_at_ms: u64,
    order_family: Option<NativeOrderFamily>,
    event: DomainEvent,
}

impl AccountPrivateFactInput {
    pub fn new(
        event_id: EventId,
        generation: u64,
        received_at_ms: u64,
        order_family: Option<NativeOrderFamily>,
        event: DomainEvent,
    ) -> Result<Self, AccountPrivateIngressError> {
        if generation == 0
            || received_at_ms == 0
            || matches!(event, DomainEvent::Instrument(_))
            || (matches!(event, DomainEvent::Order(_) | DomainEvent::Fill(_))
                != order_family.is_some())
            || validate_private_domain_event(&event).is_err()
        {
            return Err(AccountPrivateIngressError::Input);
        }
        Ok(Self {
            event_id,
            generation,
            received_at_ms,
            order_family,
            event,
        })
    }
}

/// Account-local adapter ingress backed by the existing normalized `facts.jsonl` journal.  It
/// never retains raw websocket payloads and never accepts a caller-provided persistence flag.
#[derive(Debug)]
pub(crate) struct AccountPrivateIngress {
    facts: Journal,
}

impl AccountPrivateIngress {
    pub(crate) fn open(path: PathBuf) -> Result<Self, AccountPrivateIngressError> {
        Ok(Self {
            facts: Journal::open(path)?,
        })
    }

    pub(crate) fn persist(
        &mut self,
        input: AccountPrivateFactInput,
    ) -> Result<PersistedPrivateFact, AccountPrivateIngressError> {
        let AccountPrivateFactInput {
            event_id,
            generation,
            received_at_ms,
            order_family,
            event,
        } = input;
        let event_for_record = event.clone();
        let persisted = self.facts.append_with_sequence(|sequence| {
            Ok(FactRecord {
                header: EventHeader {
                    schema_version: 1,
                    event_id,
                    source: EventSource::PrivateAccount,
                    source_sequence: Some(sequence),
                    received_at_ms,
                    generation,
                },
                event: event_for_record,
            })
        })?;
        PersistedPrivateFact::from_persisted_fact_record(
            persisted.sequence,
            order_family,
            persisted.record,
        )
        .map_err(|_| AccountPrivateIngressError::Input)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AccountPrivateIngressError {
    #[error("private ingress input is invalid")]
    Input,
    #[error(transparent)]
    Storage(#[from] venue_storage::StorageError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::{Amount, Asset};

    use super::*;

    #[test]
    fn persists_normalized_private_fact_only_after_the_shared_facts_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut ingress = AccountPrivateIngress::open(directory.path().join("facts.jsonl"))?;
        let input = AccountPrivateFactInput::new(
            EventId::new("funding_1")?,
            7,
            100,
            None,
            DomainEvent::Funding(Amount::new(Asset::new("USDT")?, Decimal::ONE)),
        )?;

        let fact = ingress.persist(input)?;

        assert_eq!(fact.record().header.source, EventSource::PrivateAccount);
        assert_eq!(fact.record().header.source_sequence, Some(1));
        assert_eq!(fact.evidence().sequence(), 1);
        assert_eq!(fact.evidence().generation(), 7);
        assert!(fact.evidence().payload_sha256().len() == 64);
        Ok(())
    }

    #[test]
    fn rejects_invalid_before_it_can_reach_the_facts_journal()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("facts.jsonl");
        let mut ingress = AccountPrivateIngress::open(path.clone())?;
        assert!(matches!(
            AccountPrivateFactInput::new(
                EventId::new("invalid")?,
                1,
                1,
                Some(NativeOrderFamily::UmOrder),
                DomainEvent::Funding(Amount::new(Asset::new("USDT")?, Decimal::ONE)),
            ),
            Err(AccountPrivateIngressError::Input)
        ));
        assert!(!path.exists());
        let _ = &mut ingress;
        Ok(())
    }
}
