use std::path::PathBuf;

use rust_decimal::Decimal;

use crate::strategy::scalping::SemanticIntent;
use crate::strategy::scalping::StrategyBinding;

use super::{
    PrivateExposure, PrivateFactsProjectionInput, PrivateFactsReadiness, ScalpingLiveEntryOutcome,
    ScalpingLiveExitDriveReport, ScalpingLiveExitSettlement, ScalpingLiveGatewayError,
    ScalpingLiveSettlement, ScalpingProtectedGateway, ScalpingResidentSources,
};

/// Resident-owned composition of entry and exit checkpoints. It has no credentials or transport;
/// gateway construction and private-worker fencing remain explicit at the outer process boundary.
pub struct ScalpingLiveDriver {
    entry: ScalpingLiveSettlement,
    exit: ScalpingLiveExitSettlement,
}

impl ScalpingLiveDriver {
    pub fn open(
        artifacts_root: PathBuf,
        binding: StrategyBinding,
    ) -> Result<Self, ScalpingLiveGatewayError> {
        Ok(Self {
            entry: ScalpingLiveSettlement::open(artifacts_root.clone(), binding.clone())?,
            exit: ScalpingLiveExitSettlement::open(artifacts_root, &binding)?,
        })
    }

    pub fn record_entry_outcome(
        &mut self,
        intent_id: &str,
        outcome: &ScalpingLiveEntryOutcome,
    ) -> Result<(), ScalpingLiveGatewayError> {
        self.entry.record_entry_outcome(intent_id, outcome)
    }

    pub fn begin_entry(&mut self, intent: &SemanticIntent) -> Result<(), ScalpingLiveGatewayError> {
        self.entry
            .begin_entry(&intent.intent_id, &intent.idempotency_seed)
    }

    #[must_use]
    pub fn can_accept_entry(&self) -> bool {
        self.entry.is_idle() && self.exit.is_idle()
    }

    /// A successful resident shutdown cannot leave a durable external hand-off or exit cursor.
    #[must_use]
    pub fn is_quiescent(&self) -> bool {
        self.entry.is_idle() && self.exit.is_idle()
    }

    #[must_use]
    pub fn has_active_exit(&self) -> bool {
        !self.exit.is_idle()
    }

    #[must_use]
    pub fn has_active_protection(&self) -> bool {
        self.entry.active_protection_client_algo_id().is_some()
    }

    #[must_use]
    pub fn exit_needs_gateway(&self) -> bool {
        self.exit.needs_gateway()
    }

    #[must_use]
    pub fn awaits_private_reconciliation(&self) -> bool {
        self.entry.awaits_private_reconciliation() || self.exit.awaits_private_reconciliation()
    }

    #[must_use]
    pub fn ready_protection_ids(
        &self,
    ) -> Option<(crate::domain::CommandId, Option<crate::domain::CommandId>)> {
        self.entry.ready_protection_ids()
    }

    pub fn recover_ready_protected_flat(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
    ) -> Result<bool, ScalpingLiveGatewayError> {
        self.entry
            .recover_ready_protected_flat(readiness, projections)
    }

    /// Applies a new private-worker entry settlement to the host before acknowledging it.
    pub fn reconcile_entry(
        &mut self,
        sources: &mut ScalpingResidentSources,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let _ = self
            .entry
            .reconcile_into_resident(sources, readiness, projections)?;
        Ok(())
    }

    /// Starts a durable physical exit for the exact Algo saved by protected-entry settlement.
    pub fn begin_exit(
        &mut self,
        hard_stop_distance_bps: Decimal,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let client_algo_id = self
            .entry
            .active_protection_client_algo_id()
            .cloned()
            .ok_or(ScalpingLiveGatewayError::Settlement)?;
        let target_client_algo_id = self.entry.active_target_client_algo_id().cloned();
        self.exit.begin(
            client_algo_id,
            target_client_algo_id,
            hard_stop_distance_bps,
        )
    }

    pub fn drive_exit(
        &mut self,
        gateway: &mut ScalpingProtectedGateway,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
        now_ms: u64,
    ) -> Result<ScalpingLiveExitDriveReport, ScalpingLiveGatewayError> {
        self.exit
            .drive_gateway(gateway, &mut self.entry, readiness, projections, now_ms)
    }

    /// Retires a stop that the exchange has already executed only after the resident has durably
    /// made the associated episode terminal. This path cannot infer a fill from REST and cannot
    /// run while an explicit exit cursor is active.
    pub fn reconcile_terminal_flat(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
    ) -> Result<bool, ScalpingLiveGatewayError> {
        if !self.exit.is_idle() || !self.has_active_protection() {
            return Ok(false);
        }
        if readiness.exposure == PrivateExposure::Flat && readiness.algo_order_debt {
            let protection_client_algo_id = self
                .entry
                .active_protection_client_algo_id()
                .cloned()
                .ok_or(ScalpingLiveGatewayError::Settlement)?;
            let target_client_algo_id = self.entry.active_target_client_algo_id().cloned();
            self.exit
                .begin_flat_cleanup(protection_client_algo_id, target_client_algo_id)?;
            return Ok(false);
        }
        self.entry.reconcile_flat_exit(readiness, projections)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::{
        domain::{Amount, CommandId, PositionSide},
        execution::{WriterLeaseAuthority, WriterScope, sha256_hex},
        runtime::{
            ExecutionProjection, OwnerProjection, PrivateExposure, PrivateFactsProjectionInput,
            PrivateFactsReadiness, PrivateProjection, ProtectionProjection, RiskBudgetProjection,
            ScalpingLiveEntryOutcome, ScalpingLiveSettlementAction,
        },
        strategy::scalping::StrategyKind,
    };

    use super::*;

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: StrategyKind::Scalping,
            strategy_instance_id: "driver-test".to_owned(),
            run_id: "live-test".to_owned(),
            exchange: "binance".to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "SOL/USDT".parse()?,
            parameter_release_id: "direct-v1".to_owned(),
            owner_scope: "driver-test:live-test:SOL/USDT".to_owned(),
            risk_budget: Amount::new("USDT".parse()?, Decimal::new(10, 0)),
        })
    }

    fn projections(generation: u64, observed_at_ms: u64) -> PrivateFactsProjectionInput {
        PrivateFactsProjectionInput {
            execution: PrivateProjection {
                generation,
                observed_at_ms,
                value: ExecutionProjection::Known,
            },
            owner: PrivateProjection {
                generation,
                observed_at_ms,
                value: OwnerProjection::Clear,
            },
            protection: PrivateProjection {
                generation,
                observed_at_ms,
                value: ProtectionProjection::Complete,
            },
            risk_budget: PrivateProjection {
                generation,
                observed_at_ms,
                value: RiskBudgetProjection::Available,
            },
        }
    }

    #[test]
    fn exit_cannot_begin_before_a_protected_entry_is_confirmed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut driver = ScalpingLiveDriver::open(directory.path().to_path_buf(), binding()?)?;
        assert!(matches!(
            driver.begin_exit(Decimal::ONE),
            Err(ScalpingLiveGatewayError::Settlement)
        ));
        let idempotency_seed = "driver-entry";
        let protection_client_algo_id = CommandId::new(format!(
            "vsa_{}",
            &sha256_hex(idempotency_seed.as_bytes())[..28]
        ))?;
        driver.entry.begin_entry("intent-1", idempotency_seed)?;
        driver.record_entry_outcome(
            "intent-1",
            &ScalpingLiveEntryOutcome::Protected {
                command_id: CommandId::new("ent_1")?,
                position_side: PositionSide::Long,
                quantity: Decimal::ONE,
                protection_strategy_id: "123".to_owned(),
                protection_client_algo_id,
            },
        )?;
        // Recording a gateway result alone cannot start exit: only reconciliation acknowledgement
        // makes its Algo identity active.
        assert!(matches!(
            driver.begin_exit(Decimal::ONE),
            Err(ScalpingLiveGatewayError::Settlement)
        ));
        Ok(())
    }

    #[test]
    fn gateway_outcome_withholds_new_entry_until_private_reconciliation()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut driver = ScalpingLiveDriver::open(directory.path().to_path_buf(), binding()?)?;

        driver.entry.begin_entry("intent-1", "driver-no-fill")?;
        driver.record_entry_outcome(
            "intent-1",
            &ScalpingLiveEntryOutcome::NoFill {
                command_id: CommandId::new("ent_1")?,
            },
        )?;

        assert!(driver.awaits_private_reconciliation());
        assert!(!driver.can_accept_entry());
        assert!(!driver.is_quiescent());
        Ok(())
    }

    #[test]
    fn terminal_semantic_flat_retires_a_confirmed_protected_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let authority = WriterLeaseAuthority::open(
            directory.path().join("writer.json"),
            WriterScope {
                exchange: binding.exchange.clone(),
                account: binding.account.clone(),
                symbol: binding.symbol.clone(),
                owner_scope: binding.owner_scope.clone(),
            },
        )?;
        authority.register_initial(100, 1)?;
        let mut driver = ScalpingLiveDriver::open(directory.path().to_path_buf(), binding)?;
        let seed = "terminal-flat";
        let client_algo_id = CommandId::new(format!("vsa_{}", &sha256_hex(seed.as_bytes())[..28]))?;
        driver.entry.begin_entry("intent-terminal-flat", seed)?;
        driver.record_entry_outcome(
            "intent-terminal-flat",
            &ScalpingLiveEntryOutcome::Protected {
                command_id: CommandId::new("ent_terminal_flat")?,
                position_side: PositionSide::Long,
                quantity: Decimal::ONE,
                protection_strategy_id: "12345".to_owned(),
                protection_client_algo_id: client_algo_id.clone(),
            },
        )?;
        let protected = PrivateFactsReadiness {
            generation: 2,
            observed_at_ms: 200,
            root_cause_fact_id: "private-readback:2:200:protected".to_owned(),
            exposure: PrivateExposure::Open,
            ordinary_order_debt: false,
            algo_order_debt: true,
        };
        let action = driver
            .entry
            .reconcile(&protected, projections(2, 200))?
            .ok_or(ScalpingLiveGatewayError::Settlement)?;
        assert_eq!(
            action,
            ScalpingLiveSettlementAction::ConfirmProtected {
                intent_id: "intent-terminal-flat".to_owned(),
                client_algo_id,
            }
        );
        driver.entry.acknowledge(&action)?;
        assert!(driver.has_active_protection());

        let flat = PrivateFactsReadiness {
            generation: 3,
            observed_at_ms: 300,
            root_cause_fact_id: "private-readback:3:300:flat".to_owned(),
            exposure: PrivateExposure::Flat,
            ordinary_order_debt: false,
            algo_order_debt: false,
        };
        assert!(driver.reconcile_terminal_flat(&flat, projections(3, 300))?);
        assert!(!driver.has_active_protection());
        assert!(driver.is_quiescent());
        assert!(authority.active_session()?.is_none());
        Ok(())
    }
}
