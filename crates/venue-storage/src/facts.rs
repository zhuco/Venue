use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use venue_domain::{EventId, FactRecord};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptOutcome {
    Accepted,
    Late,
    Duplicate,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradingFacts {
    records: Vec<FactRecord>,
    event_ids: BTreeSet<EventId>,
    source_watermark: u64,
}

impl TradingFacts {
    pub fn accept(&mut self, record: FactRecord) -> AcceptOutcome {
        let event_id = record.header.event_id.clone();
        if !self.event_ids.insert(event_id) {
            return AcceptOutcome::Duplicate;
        }
        let outcome = match record.header.source_sequence {
            Some(sequence) if sequence <= self.source_watermark => AcceptOutcome::Late,
            Some(sequence) => {
                self.source_watermark = sequence;
                AcceptOutcome::Accepted
            }
            None => AcceptOutcome::Accepted,
        };
        self.records.push(record);
        outcome
    }

    pub fn contains(&self, event_id: &EventId) -> bool {
        self.event_ids.contains(event_id)
    }

    pub fn records(&self) -> &[FactRecord] {
        &self.records
    }

    pub const fn source_watermark(&self) -> u64 {
        self.source_watermark
    }
}
