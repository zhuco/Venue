use crate::strategy::scalping::CandidateEvidenceBundle;

pub use venue_storage::ScalpingEvidenceError;

pub type ScalpingEvidenceRecord = venue_storage::ScalpingEvidenceRecord<CandidateEvidenceBundle>;
pub type ScalpingEvidenceJournal = venue_storage::ScalpingEvidenceJournal<CandidateEvidenceBundle>;

impl venue_storage::EvidenceBundle for CandidateEvidenceBundle {
    fn evidence_identities(&self) -> [(u16, &str); 3] {
        [
            (
                self.calibration.identity.schema_version,
                &self.calibration.identity.evidence_id,
            ),
            (
                self.costs.identity.schema_version,
                &self.costs.identity.evidence_id,
            ),
            (
                self.risk.identity.schema_version,
                &self.risk.identity.evidence_id,
            ),
        ]
    }
}
