use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryBinding, ControlSnapshot,
    CopyExecutionEvidence, CopyPlanningFact, ExecutionFactsSnapshot, ProtocolError,
    copy_planning::validate_copy_planning_facts,
};

/// Node-owned, read-only account projection. It conveys no mutation permit or command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeProjectionEnvelope {
    pub schema_version: u16,
    pub binding: AccountDeliveryBinding,
    pub node_id: String,
    pub node_generation: u64,
    pub sequence: u64,
    pub previous_digest: [u8; 32],
    pub digest: [u8; 32],
    pub snapshot: ControlSnapshot,
    pub facts: ExecutionFactsSnapshot,
    /// Bounded fixed-format Copy results, retained only by the node projection outbox until an
    /// exact Control echo. They are deliberately outside the UI facts/snapshot read models.
    #[serde(default)]
    pub copy_execution_evidence: Vec<CopyExecutionEvidence>,
    /// Bounded raw signed facts used solely to construct immutable Copy planner input. These are
    /// intentionally outside browser facts and cannot authorize a delivery or mutation.
    #[serde(default)]
    pub copy_planning_facts: Vec<CopyPlanningFact>,
}

impl NodeProjectionEnvelope {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != ACCOUNT_DELIVERY_SCHEMA_VERSION
            || self.node_id.trim().is_empty()
            || self.node_generation == 0
            || self.sequence == 0
            || self.digest == [0; 32]
        {
            return Err(ProtocolError::SchemaVersion);
        }
        self.binding.validate()?;
        self.snapshot.validate()?;
        self.facts.validate()?;
        validate_copy_execution_evidence(
            &self.copy_execution_evidence,
            self.snapshot.generated_ms,
        )?;
        validate_copy_planning_facts(&self.copy_planning_facts, self.snapshot.generated_ms)?;
        let scoped = self.snapshot.accounts.iter().any(|account| {
            account.venue == self.binding.venue
                && account.mode == self.binding.mode
                && account.trading_account_id == self.binding.trading_account_id
        });
        let exact_account_scope = |venue, mode, trading_account_id: &str| {
            venue == self.binding.venue
                && mode == self.binding.mode
                && trading_account_id == self.binding.trading_account_id
        };
        let exact_strategy_scope = |strategy: &crate::StrategySummary| {
            strategy.venue == self.binding.venue
                && strategy.mode == self.binding.mode
                && strategy.trading_account_id == self.binding.trading_account_id
                && strategy.symbol == self.binding.symbol
                && strategy.instance_id == self.binding.instance_id
                && strategy.config_epoch == self.binding.config_epoch
        };
        let exact_fact_binding = |binding: &crate::ExecutionFactBinding| {
            binding.venue == self.binding.venue
                && binding.mode == self.binding.mode
                && binding.trading_account_id == self.binding.trading_account_id
                && binding.symbol == self.binding.symbol
                && binding.instance_id == self.binding.instance_id
                && binding.config_epoch == self.binding.config_epoch
        };
        let facts_are_account_scoped = self
            .facts
            .orders
            .iter()
            .map(|fact| {
                (
                    &fact.binding.venue,
                    fact.binding.mode,
                    fact.binding.trading_account_id.as_str(),
                )
            })
            .chain(self.facts.positions.iter().map(|fact| {
                (
                    &fact.binding.venue,
                    fact.binding.mode,
                    fact.binding.trading_account_id.as_str(),
                )
            }))
            .chain(self.facts.fills.iter().map(|fact| {
                (
                    &fact.binding.venue,
                    fact.binding.mode,
                    fact.binding.trading_account_id.as_str(),
                )
            }))
            .chain(self.facts.reconciliation.iter().map(|fact| {
                (
                    &fact.binding.venue,
                    fact.binding.mode,
                    fact.binding.trading_account_id.as_str(),
                )
            }))
            .chain(self.facts.copy_ledger.iter().map(|fact| {
                (
                    &fact.binding.venue,
                    fact.binding.mode,
                    fact.binding.trading_account_id.as_str(),
                )
            }))
            .chain(self.facts.drift.iter().map(|fact| {
                (
                    &fact.binding.venue,
                    fact.binding.mode,
                    fact.binding.trading_account_id.as_str(),
                )
            }))
            .chain(self.facts.execution.iter().map(|fact| {
                (
                    &fact.binding.venue,
                    fact.binding.mode,
                    fact.binding.trading_account_id.as_str(),
                )
            }))
            .chain(
                self.facts
                    .risk
                    .iter()
                    .map(|fact| (&fact.venue, fact.mode, fact.trading_account_id.as_str())),
            )
            .chain(
                self.facts
                    .health
                    .iter()
                    .map(|fact| (&fact.venue, fact.mode, fact.trading_account_id.as_str())),
            )
            .all(|(venue, mode, account)| exact_account_scope(*venue, mode, account));
        let facts_are_strategy_scoped = self
            .facts
            .orders
            .iter()
            .map(|fact| &fact.binding)
            .chain(self.facts.positions.iter().map(|fact| &fact.binding))
            .chain(self.facts.fills.iter().map(|fact| &fact.binding))
            .chain(self.facts.reconciliation.iter().map(|fact| &fact.binding))
            .chain(self.facts.copy_ledger.iter().map(|fact| &fact.binding))
            .chain(self.facts.drift.iter().map(|fact| &fact.binding))
            .chain(self.facts.execution.iter().map(|fact| &fact.binding))
            .all(exact_fact_binding);
        if !scoped
            || self.snapshot.generated_ms != self.facts.generated_ms
            || self.snapshot.accounts.iter().any(|account| {
                !exact_account_scope(account.venue, account.mode, &account.trading_account_id)
            })
            || self
                .snapshot
                .strategies
                .iter()
                .any(|strategy| !exact_strategy_scope(strategy))
            || self.copy_execution_evidence.iter().any(|evidence| {
                evidence.binding.venue != self.binding.venue
                    || evidence.binding.mode != self.binding.mode
                    || evidence.binding.trading_account_id != self.binding.trading_account_id
                    || evidence.binding.symbol != self.binding.symbol
                    || evidence.binding.instance_id != self.binding.instance_id
                    || evidence.binding.config_epoch != self.binding.config_epoch
            })
            || self.copy_planning_facts.iter().any(|fact| {
                fact.binding.venue != self.binding.venue
                    || fact.binding.mode != self.binding.mode
                    || fact.binding.trading_account_id != self.binding.trading_account_id
                    || fact.binding.symbol != self.binding.symbol
                    || fact.binding.instance_id != self.binding.instance_id
                    || fact.binding.config_epoch != self.binding.config_epoch
            })
            || !facts_are_account_scoped
            || !facts_are_strategy_scoped
        {
            return Err(ProtocolError::SnapshotTime);
        }
        Ok(())
    }
}

fn validate_copy_execution_evidence(
    evidence: &[CopyExecutionEvidence],
    generated_ms: u64,
) -> Result<(), ProtocolError> {
    if evidence.len() > 16
        || evidence
            .iter()
            .map(|item| item.result_bytes.len())
            .sum::<usize>()
            > 48 * 1024
        || evidence.iter().any(|item| {
            !crate::is_uuid(&item.relation_id)
                || item.relation_revision == 0
                || item.job_id.trim().is_empty()
                || item.result_sha256 == [0; 32]
                || item.result_bytes.is_empty()
                || item.result_bytes.len() > 16 * 1024
                || item.observed_ms == 0
                || item.observed_ms > generated_ms
                || item.binding.validate().is_err()
                || (item.state == crate::CopyExecutionStateProjection::Reconciled
                    && item.result_fact_digest == [0; 32])
                || item
                    .command_id
                    .as_deref()
                    .is_some_and(|command_id| command_id.trim().is_empty())
                || Sha256::digest(item.result_bytes.as_bytes()).as_slice() != item.result_sha256
        })
        || evidence
            .iter()
            .map(|item| (item.job_id.clone(), item.phase))
            .collect::<BTreeSet<_>>()
            .len()
            != evidence.len()
        || evidence.iter().enumerate().any(|(index, item)| {
            item.phase == crate::CopyExecutionPhaseProjection::Adjust
                && evidence[index.saturating_add(1)..].iter().any(|later| {
                    later.job_id == item.job_id
                        && later.phase == crate::CopyExecutionPhaseProjection::ReduceToZero
                })
        })
    {
        return Err(ProtocolError::SnapshotContent);
    }
    Ok(())
}
