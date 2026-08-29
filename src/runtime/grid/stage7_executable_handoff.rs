use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::execution::ExecutableHandoffReceipt;

use super::stage7_canary_support::{
    STAGE7_LIVE_ADMISSION_FILE, Stage7LiveAdmissionEvidence, basic_canary_hashes,
    canary_cleanup_readback, canonical_digest, executable_sha256, exposure_release_digest,
    valid_sha256,
};
use super::*;

const HANDOFF_SCHEMA_VERSION: u16 = 1;
const RELOCATION_HANDOFF_SCHEMA_VERSION: u16 = 2;
const HANDOFF_DIRECTORY: &str = "stage7_executable_handoffs";
const COMMAND_WAL_ARCHIVE_DIRECTORY: &str = "resolved_command_wal_archives";
const HANDOFF_AUTHORIZATION: &str = "preserve-hedge-positions-cancel-owned-orders-only";
const RELOCATION_HANDOFF_AUTHORIZATION: &str = "preserve-hedge-positions-cross-host-relocation-v1";
// Keep recovery bounded while allowing routine immutable maintenance releases to remain chained
// to the original Canary admission. Cycle detection below remains the primary corruption fence.
// A content-addressed upgrade adds one immutable link. Long-lived personal deployments can
// legitimately exceed a few dozen releases; retain a corruption bound without turning routine
// upgrades into a false cycle after 32 handoffs.
const MAX_HANDOFF_CHAIN_DEPTH: usize = 4_096;
const STOP_RECOVERY_READBACK_ATTEMPTS: usize = 4;
const STOP_CANCEL_SETTLE_ATTEMPTS: usize = 120;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stage7ExecutableHandoffManifest {
    schema_version: u16,
    authorization: String,
    exchange: String,
    symbol: String,
    canonical_root_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    predecessor_canonical_root_sha256: Option<String>,
    predecessor_executable_sha256: String,
    successor_executable_sha256: String,
    predecessor_admission_sha256: String,
    authorized_at_ms: u64,
    valid_until_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stage7ExecutableHandoffReceipt {
    schema_version: u16,
    manifest: Stage7ExecutableHandoffManifest,
    manifest_sha256: String,
    predecessor_admission: Stage7LiveAdmissionEvidence,
    writer_scope_sha256: String,
    writer_generation: u64,
    writer_revision: u64,
    writer_readback_generation: u64,
    control_sha256: String,
    checkpoint_sha256: String,
    command_journal_sha256: String,
    writer_state_sha256: String,
    private_snapshot_sha256: String,
    private_generation: u64,
    observed_at_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    long_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    short_quantity: Decimal,
    hedge_position: bool,
    orders_empty: bool,
    wal_resolved: bool,
    local_transactions_empty: bool,
    order_health_clear: bool,
    #[serde(default)]
    successor_exposure_release_bound: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    successor_exposure_take_profit_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    successor_configuration_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_evidence_recovery_manifest_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_evidence_journal_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_evidence_journal_bytes: Option<u64>,
    handoff_sha256: String,
}

#[derive(Serialize)]
struct Stage7ExecutableHandoffDigest<'a> {
    schema_version: u16,
    manifest: &'a Stage7ExecutableHandoffManifest,
    manifest_sha256: &'a str,
    predecessor_admission: &'a Stage7LiveAdmissionEvidence,
    writer_scope_sha256: &'a str,
    writer_generation: u64,
    writer_revision: u64,
    writer_readback_generation: u64,
    control_sha256: &'a str,
    checkpoint_sha256: &'a str,
    command_journal_sha256: &'a str,
    writer_state_sha256: &'a str,
    private_snapshot_sha256: &'a str,
    private_generation: u64,
    observed_at_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    long_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    short_quantity: Decimal,
    hedge_position: bool,
    orders_empty: bool,
    wal_resolved: bool,
    local_transactions_empty: bool,
    order_health_clear: bool,
    #[serde(skip_serializing_if = "super::stage7_canary_support::is_false")]
    successor_exposure_release_bound: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    successor_exposure_take_profit_sha256: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    successor_configuration_sha256: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    private_evidence_recovery_manifest_sha256: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    private_evidence_journal_sha256: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    private_evidence_journal_bytes: &'a Option<u64>,
}

/// Exact digest shape used by receipts created before exposure-release binding existed.
#[derive(Serialize)]
struct LegacyStage7ExecutableHandoffDigest<'a> {
    schema_version: u16,
    manifest: &'a Stage7ExecutableHandoffManifest,
    manifest_sha256: &'a str,
    predecessor_admission: LegacyStage7AdmissionDigest<'a>,
    writer_scope_sha256: &'a str,
    writer_generation: u64,
    writer_revision: u64,
    writer_readback_generation: u64,
    control_sha256: &'a str,
    checkpoint_sha256: &'a str,
    command_journal_sha256: &'a str,
    writer_state_sha256: &'a str,
    private_snapshot_sha256: &'a str,
    private_generation: u64,
    observed_at_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    long_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    short_quantity: Decimal,
    hedge_position: bool,
    orders_empty: bool,
    wal_resolved: bool,
    local_transactions_empty: bool,
    order_health_clear: bool,
}

/// Serialization shape of admissions embedded by binaries predating exposure release fields.
#[derive(Serialize)]
struct LegacyStage7AdmissionDigest<'a> {
    schema_version: u16,
    capability_binding: &'a CapabilityBinding,
    deployment_binding: &'a HedgedGridBinding,
    parameter_release: &'a HedgedGridParams,
    instrument_rules: &'a super::stage7_canary_support::Stage7InstrumentRulesEvidence,
    release_id: &'a str,
    selection_rule: &'a str,
    executable_sha256: &'a str,
    configuration_sha256: &'a str,
    parameter_release_sha256: &'a str,
    instrument_rules_sha256: &'a str,
    verified_at_ms: u64,
    valid_until_ms: u64,
    private_generation: u64,
    health_generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    predecessor_admission_sha256: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    executable_handoff_manifest_sha256: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    executable_handoff_sha256: &'a Option<String>,
    admission_sha256: &'a str,
}

#[derive(Serialize)]
struct Stage7PrivateStopSnapshot<'a> {
    binding: &'a HedgedGridBinding,
    private_generation: u64,
    observed_at_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    long_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    short_quantity: Decimal,
    hedge_position: bool,
    orders_empty: bool,
}

impl Stage7ExecutableHandoffManifest {
    fn validate_static(&self) -> Result<(), Stage7GridError> {
        let root_transition_valid = match self.schema_version {
            HANDOFF_SCHEMA_VERSION => {
                self.authorization == HANDOFF_AUTHORIZATION
                    && self.predecessor_canonical_root_sha256.is_none()
            }
            RELOCATION_HANDOFF_SCHEMA_VERSION => {
                self.authorization == RELOCATION_HANDOFF_AUTHORIZATION
                    && self
                        .predecessor_canonical_root_sha256
                        .as_deref()
                        .is_some_and(valid_sha256)
                    && self.predecessor_canonical_root_sha256.as_deref()
                        != Some(self.canonical_root_sha256.as_str())
            }
            _ => false,
        };
        if !root_transition_valid
            || self.exchange.trim().is_empty()
            || self.symbol.parse::<crate::domain::Symbol>().is_err()
            || !valid_sha256(&self.canonical_root_sha256)
            || !valid_sha256(&self.predecessor_executable_sha256)
            || !valid_sha256(&self.successor_executable_sha256)
            || !valid_sha256(&self.predecessor_admission_sha256)
            || self.predecessor_executable_sha256 == self.successor_executable_sha256
            || self.authorized_at_ms == 0
            || self.valid_until_ms <= self.authorized_at_ms
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        Ok(())
    }

    fn predecessor_root_sha256(&self) -> &str {
        self.predecessor_canonical_root_sha256
            .as_deref()
            .unwrap_or(&self.canonical_root_sha256)
    }

    fn validate(
        &self,
        binding: &HedgedGridBinding,
        canonical_root_sha256: &str,
        current_executable_sha256: &str,
        now_ms: u64,
    ) -> Result<(), Stage7GridError> {
        self.validate_static()?;
        if self.exchange != binding.exchange
            || self.symbol != binding.symbol.to_string()
            || self.canonical_root_sha256 != canonical_root_sha256
            || self.successor_executable_sha256 != current_executable_sha256
            || now_ms < self.authorized_at_ms
            || now_ms >= self.valid_until_ms
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        Ok(())
    }
}

impl Stage7ExecutableHandoffReceipt {
    fn expected_handoff_sha256(&self) -> Result<String, Stage7GridError> {
        canonical_digest(&Stage7ExecutableHandoffDigest {
            schema_version: self.schema_version,
            manifest: &self.manifest,
            manifest_sha256: &self.manifest_sha256,
            predecessor_admission: &self.predecessor_admission,
            writer_scope_sha256: &self.writer_scope_sha256,
            writer_generation: self.writer_generation,
            writer_revision: self.writer_revision,
            writer_readback_generation: self.writer_readback_generation,
            control_sha256: &self.control_sha256,
            checkpoint_sha256: &self.checkpoint_sha256,
            command_journal_sha256: &self.command_journal_sha256,
            writer_state_sha256: &self.writer_state_sha256,
            private_snapshot_sha256: &self.private_snapshot_sha256,
            private_generation: self.private_generation,
            observed_at_ms: self.observed_at_ms,
            long_quantity: self.long_quantity,
            short_quantity: self.short_quantity,
            hedge_position: self.hedge_position,
            orders_empty: self.orders_empty,
            wal_resolved: self.wal_resolved,
            local_transactions_empty: self.local_transactions_empty,
            order_health_clear: self.order_health_clear,
            successor_exposure_release_bound: self.successor_exposure_release_bound,
            successor_exposure_take_profit_sha256: &self.successor_exposure_take_profit_sha256,
            successor_configuration_sha256: &self.successor_configuration_sha256,
            private_evidence_recovery_manifest_sha256: &self
                .private_evidence_recovery_manifest_sha256,
            private_evidence_journal_sha256: &self.private_evidence_journal_sha256,
            private_evidence_journal_bytes: &self.private_evidence_journal_bytes,
        })
    }

    fn expected_legacy_handoff_sha256(&self) -> Result<String, Stage7GridError> {
        let predecessor = &self.predecessor_admission;
        canonical_digest(&LegacyStage7ExecutableHandoffDigest {
            schema_version: self.schema_version,
            manifest: &self.manifest,
            manifest_sha256: &self.manifest_sha256,
            predecessor_admission: LegacyStage7AdmissionDigest {
                schema_version: predecessor.schema_version,
                capability_binding: &predecessor.capability_binding,
                deployment_binding: &predecessor.deployment_binding,
                parameter_release: &predecessor.parameter_release,
                instrument_rules: &predecessor.instrument_rules,
                release_id: &predecessor.release_id,
                selection_rule: &predecessor.selection_rule,
                executable_sha256: &predecessor.executable_sha256,
                configuration_sha256: &predecessor.configuration_sha256,
                parameter_release_sha256: &predecessor.parameter_release_sha256,
                instrument_rules_sha256: &predecessor.instrument_rules_sha256,
                verified_at_ms: predecessor.verified_at_ms,
                valid_until_ms: predecessor.valid_until_ms,
                private_generation: predecessor.private_generation,
                health_generation: predecessor.health_generation,
                predecessor_admission_sha256: &predecessor.predecessor_admission_sha256,
                executable_handoff_manifest_sha256: &predecessor.executable_handoff_manifest_sha256,
                executable_handoff_sha256: &predecessor.executable_handoff_sha256,
                admission_sha256: &predecessor.admission_sha256,
            },
            writer_scope_sha256: &self.writer_scope_sha256,
            writer_generation: self.writer_generation,
            writer_revision: self.writer_revision,
            writer_readback_generation: self.writer_readback_generation,
            control_sha256: &self.control_sha256,
            checkpoint_sha256: &self.checkpoint_sha256,
            command_journal_sha256: &self.command_journal_sha256,
            writer_state_sha256: &self.writer_state_sha256,
            private_snapshot_sha256: &self.private_snapshot_sha256,
            private_generation: self.private_generation,
            observed_at_ms: self.observed_at_ms,
            long_quantity: self.long_quantity,
            short_quantity: self.short_quantity,
            hedge_position: self.hedge_position,
            orders_empty: self.orders_empty,
            wal_resolved: self.wal_resolved,
            local_transactions_empty: self.local_transactions_empty,
            order_health_clear: self.order_health_clear,
        })
    }

    fn validate_static(&self) -> Result<(), Stage7GridError> {
        self.manifest.validate_static()?;
        self.predecessor_admission.validate()?;
        let private_evidence_snapshot_valid = match (
            &self.private_evidence_journal_sha256,
            self.private_evidence_journal_bytes,
        ) {
            (None, None) => self.private_evidence_recovery_manifest_sha256.is_none(),
            (Some(sha256), Some(bytes)) => {
                valid_sha256(sha256)
                    && bytes > 0
                    && self
                        .private_evidence_recovery_manifest_sha256
                        .as_deref()
                        .is_none_or(valid_sha256)
            }
            _ => false,
        };
        let successor_config_valid = if self.successor_exposure_release_bound {
            self.successor_exposure_take_profit_sha256
                .as_deref()
                .is_none_or(valid_sha256)
                && self
                    .successor_configuration_sha256
                    .as_deref()
                    .is_some_and(valid_sha256)
                && self.successor_configuration_sha256.as_deref()
                    == Some(
                        self.predecessor_admission
                            .configuration_sha256_with_exposure(
                                &self.successor_exposure_take_profit_sha256,
                            )?
                            .as_str(),
                    )
        } else {
            self.successor_exposure_take_profit_sha256.is_none()
                && self.successor_configuration_sha256.is_none()
        };
        if self.schema_version != HANDOFF_SCHEMA_VERSION
            || !private_evidence_snapshot_valid
            || !successor_config_valid
            || !matches!(self.predecessor_admission.schema_version, 1 | 2)
            || self.manifest_sha256 != canonical_digest(&self.manifest)?
            || self.manifest.predecessor_executable_sha256
                != self.predecessor_admission.executable_sha256
            || self.manifest.predecessor_admission_sha256
                != self.predecessor_admission.admission_sha256
            || self.manifest.exchange != self.predecessor_admission.deployment_binding.exchange
            || self.manifest.symbol
                != self
                    .predecessor_admission
                    .deployment_binding
                    .symbol
                    .to_string()
            || self.manifest.valid_until_ms > self.predecessor_admission.valid_until_ms
            || self.observed_at_ms < self.manifest.authorized_at_ms
            || self.observed_at_ms >= self.manifest.valid_until_ms
            || self.private_generation == 0
            || self.writer_generation == 0
            || self.writer_revision == 0
            || self.writer_readback_generation == 0
            || self.private_generation < self.writer_readback_generation
            || (self.long_quantity <= Decimal::ZERO && self.short_quantity <= Decimal::ZERO)
            || !self.hedge_position
            || !self.orders_empty
            || !self.wal_resolved
            || !self.local_transactions_empty
            || !self.order_health_clear
            || [
                &self.manifest_sha256,
                &self.writer_scope_sha256,
                &self.control_sha256,
                &self.checkpoint_sha256,
                &self.command_journal_sha256,
                &self.writer_state_sha256,
                &self.private_snapshot_sha256,
                &self.handoff_sha256,
            ]
            .into_iter()
            .any(|digest| !valid_sha256(digest))
            || if self.successor_exposure_release_bound {
                self.handoff_sha256 != self.expected_handoff_sha256()?
            } else {
                self.handoff_sha256 != self.expected_legacy_handoff_sha256()?
                    && self.handoff_sha256 != self.expected_handoff_sha256()?
            }
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        Ok(())
    }

    fn validate_private_evidence_snapshot(
        &self,
        artifacts_root: &Path,
    ) -> Result<(), Stage7GridError> {
        let (Some(journal_sha256), Some(journal_bytes)) = (
            self.private_evidence_journal_sha256.as_deref(),
            self.private_evidence_journal_bytes,
        ) else {
            return Ok(());
        };
        // Relocation keeps historical receipts in the destination, but their evidence prefix
        // belongs to the manifest's predecessor root. Their receipt digest remains portable;
        // only a receipt bound to this active root can be checked against this root's journal.
        if self.manifest.canonical_root_sha256 != canonical_root_sha256(artifacts_root)? {
            return Ok(());
        }
        verify_stage7_private_evidence_snapshot(
            artifacts_root,
            &self.predecessor_admission.deployment_binding,
            journal_sha256,
            journal_bytes,
            self.private_evidence_recovery_manifest_sha256.as_deref(),
        )
    }
}

pub(super) fn validated_admission_predecessor(
    capability_binding: &CapabilityBinding,
    instrument: &crate::domain::Instrument,
    minimum_quantity: Decimal,
    artifacts_root: &Path,
    now_ms: u64,
    current_executable_sha256: &str,
) -> Result<(Stage7LiveAdmissionEvidence, Stage7LiveAdmissionEvidence), Stage7GridError> {
    let active = ProjectionStore::new(artifacts_root.join(STAGE7_LIVE_ADMISSION_FILE))
        .load::<Stage7LiveAdmissionEvidence>()?
        .ok_or(Stage7GridError::GridCanaryEvidence)?;
    let mut seen_admissions = BTreeSet::new();
    let canonical_root_sha256 = canonical_root_sha256(artifacts_root)?;
    let predecessor = validate_admission_chain(
        capability_binding,
        instrument,
        minimum_quantity,
        artifacts_root,
        now_ms,
        current_executable_sha256,
        &canonical_root_sha256,
        &active,
        0,
        &mut seen_admissions,
    )?;
    Ok((active, predecessor))
}

/// Returns the private generation that sealed the current executable handoff after revalidating
/// its admission-to-receipt link. Consumers use this boundary only to distinguish an inherited,
/// already-running recovery episode from one created by the successor.
pub(super) fn immediate_executable_handoff_private_generation(
    artifacts_root: &Path,
    admission: &Stage7LiveAdmissionEvidence,
) -> Result<Option<u64>, Stage7GridError> {
    admission.validate()?;
    if admission.schema_version != 2 {
        return Ok(None);
    }
    let manifest_sha256 = admission
        .executable_handoff_manifest_sha256
        .as_deref()
        .ok_or(Stage7GridError::ExecutableHandoff)?;
    let receipt = load_receipt(artifacts_root, manifest_sha256)?;
    let expected_root_sha256 = canonical_root_sha256(artifacts_root)?;
    if receipt.manifest_sha256 != manifest_sha256
        || receipt.handoff_sha256
            != admission
                .executable_handoff_sha256
                .as_deref()
                .ok_or(Stage7GridError::ExecutableHandoff)?
        || receipt.predecessor_admission.admission_sha256
            != admission
                .predecessor_admission_sha256
                .as_deref()
                .ok_or(Stage7GridError::ExecutableHandoff)?
        || receipt.manifest.successor_executable_sha256 != admission.executable_sha256
        || receipt.manifest.canonical_root_sha256 != expected_root_sha256
    {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let expected = if receipt.successor_exposure_release_bound || admission.exposure_release_bound {
        receipt
            .predecessor_admission
            .promote_for_executable_handoff_with_exposure(
                receipt.manifest.successor_executable_sha256.clone(),
                receipt.manifest_sha256.clone(),
                receipt.handoff_sha256.clone(),
                receipt.successor_exposure_take_profit_sha256.clone(),
            )?
    } else {
        receipt
            .predecessor_admission
            .promote_for_legacy_unbound_executable_handoff(
                receipt.manifest.successor_executable_sha256.clone(),
                receipt.manifest_sha256.clone(),
                receipt.handoff_sha256.clone(),
            )?
    };
    if expected != *admission {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    Ok(Some(receipt.private_generation))
}

fn validate_admission_chain(
    capability_binding: &CapabilityBinding,
    instrument: &crate::domain::Instrument,
    minimum_quantity: Decimal,
    artifacts_root: &Path,
    now_ms: u64,
    expected_executable_sha256: &str,
    expected_root_sha256: &str,
    active: &Stage7LiveAdmissionEvidence,
    depth: usize,
    seen_admissions: &mut BTreeSet<String>,
) -> Result<Stage7LiveAdmissionEvidence, Stage7GridError> {
    if depth > MAX_HANDOFF_CHAIN_DEPTH {
        warn!(event = "stage7_handoff_chain_depth_exceeded", depth);
        return Err(Stage7GridError::ExecutableHandoff);
    }
    if !seen_admissions.insert(active.admission_sha256.clone()) {
        warn!(event = "stage7_handoff_chain_cycle", depth);
        return Err(Stage7GridError::ExecutableHandoff);
    }
    if let Err(error) = active.validate() {
        warn!(event = "stage7_handoff_chain_admission_invalid", depth, %error);
        return Err(error);
    }
    let matches_current = active.matches_current(
        capability_binding,
        instrument,
        minimum_quantity,
        expected_executable_sha256,
        now_ms,
    )?;
    if !matches_current {
        warn!(
            event = "stage7_handoff_chain_context_mismatch",
            depth,
            validity_current = active.valid_until_ms > now_ms,
            capability_current = active.capability_binding == *capability_binding,
            instrument_current = active.instrument_rules.instrument == *instrument,
            minimum_quantity_current = active.instrument_rules.minimum_quantity == minimum_quantity,
            executable_current = active.executable_sha256 == expected_executable_sha256
        );
        return Err(if active.schema_version == 1 {
            Stage7GridError::GridCanaryEvidence
        } else {
            Stage7GridError::ExecutableHandoff
        });
    }
    if active.schema_version == 1 {
        return Ok(active.clone());
    }
    let manifest_sha256 = active
        .executable_handoff_manifest_sha256
        .as_deref()
        .ok_or(Stage7GridError::ExecutableHandoff)?;
    let receipt = match load_receipt(artifacts_root, manifest_sha256) {
        Ok(receipt) => receipt,
        Err(error) => {
            warn!(
                event = "stage7_handoff_chain_receipt_load_failed",
                depth,
                manifest_sha256,
                %error
            );
            return Err(error);
        }
    };
    if let Err(error) = receipt.validate_static() {
        warn!(event = "stage7_handoff_chain_receipt_invalid", depth, %error);
        return Err(error);
    }
    if receipt.manifest_sha256 != manifest_sha256
        || receipt.handoff_sha256
            != active
                .executable_handoff_sha256
                .as_deref()
                .ok_or(Stage7GridError::ExecutableHandoff)?
        || receipt.predecessor_admission.admission_sha256
            != active
                .predecessor_admission_sha256
                .as_deref()
                .ok_or(Stage7GridError::ExecutableHandoff)?
        || receipt.manifest.successor_executable_sha256 != expected_executable_sha256
        || receipt.manifest.canonical_root_sha256 != expected_root_sha256
        || !match receipt
            .predecessor_admission
            .matches_non_executable_context(
                capability_binding,
                instrument,
                minimum_quantity,
                now_ms,
            ) {
            Ok(matches) => matches,
            Err(error) => {
                warn!(
                    event = "stage7_handoff_chain_predecessor_invalid",
                    depth,
                    %error
                );
                return Err(error);
            }
        }
    {
        warn!(event = "stage7_handoff_chain_link_mismatch", depth);
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let expected_active =
        match if receipt.successor_exposure_release_bound || active.exposure_release_bound {
            receipt
                .predecessor_admission
                .promote_for_executable_handoff_with_exposure(
                    expected_executable_sha256.to_owned(),
                    receipt.manifest_sha256.clone(),
                    receipt.handoff_sha256.clone(),
                    receipt.successor_exposure_take_profit_sha256.clone(),
                )
        } else {
            // This branch validates an immutable historical receipt only. The creation path below
            // always writes `successor_exposure_release_bound=true` for every new handoff.
            receipt
                .predecessor_admission
                .promote_for_legacy_unbound_executable_handoff(
                    expected_executable_sha256.to_owned(),
                    receipt.manifest_sha256.clone(),
                    receipt.handoff_sha256.clone(),
                )
        } {
            Ok(expected) => expected,
            Err(error) => {
                warn!(event = "stage7_handoff_chain_promotion_failed", depth, %error);
                return Err(error);
            }
        };
    if *active != expected_active {
        warn!(event = "stage7_handoff_chain_promotion_mismatch", depth);
        return Err(Stage7GridError::ExecutableHandoff);
    }
    validate_admission_chain(
        capability_binding,
        instrument,
        minimum_quantity,
        artifacts_root,
        now_ms,
        &receipt.manifest.predecessor_executable_sha256,
        receipt.manifest.predecessor_root_sha256(),
        &receipt.predecessor_admission,
        depth + 1,
        seen_admissions,
    )
}

pub fn run_gate_stage7_executable_handoff(
    cfg: &Config,
    request: Stage7ExecutableHandoffRequest,
) -> Result<Stage7ExecutableHandoffReport, Stage7GridError> {
    validate_request(&request)?;
    let binding = gate_binding(cfg)?;
    let writer_scope = stage7_writer_scope(&binding);
    let canonical_root = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    let mut venue = GateGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_executable_handoff(
        cfg,
        request,
        binding,
        writer_scope,
        &canonical_root,
        &mut venue,
    )
}

pub fn run_binance_stage7_executable_handoff(
    cfg: &Config,
    request: Stage7ExecutableHandoffRequest,
) -> Result<Stage7ExecutableHandoffReport, Stage7GridError> {
    validate_request(&request)?;
    let binding = binance_binding(cfg)?;
    let writer_scope = stage7_writer_scope(&binding);
    let canonical_root = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    let mut venue = BinanceGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_executable_handoff(
        cfg,
        request,
        binding,
        writer_scope,
        &canonical_root,
        &mut venue,
    )
}

pub fn run_bitget_stage7_executable_handoff(
    cfg: &Config,
    request: Stage7ExecutableHandoffRequest,
) -> Result<Stage7ExecutableHandoffReport, Stage7GridError> {
    validate_request(&request)?;
    let binding = bitget_binding(cfg)?;
    let writer_scope = stage7_writer_scope(&binding);
    let canonical_root = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    let mut venue = BitgetGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_executable_handoff(
        cfg,
        request,
        binding,
        writer_scope,
        &canonical_root,
        &mut venue,
    )
}

fn validate_request(request: &Stage7ExecutableHandoffRequest) -> Result<(), Stage7GridError> {
    if !request.confirm_mainnet_nonflat_executable_handoff {
        return Err(Stage7GridError::ExecutableHandoffConfirmation);
    }
    if !request.artifacts_root.is_absolute()
        || !request.artifacts_root.is_dir()
        || !request.release_manifest.is_absolute()
        || !request.release_manifest.is_file()
    {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    Ok(())
}

fn run_stage7_executable_handoff<V: Stage7CanaryVenue>(
    cfg: &Config,
    request: Stage7ExecutableHandoffRequest,
    binding: HedgedGridBinding,
    writer_scope: WriterScope,
    canonical_root: &stage7_writer_registry::Stage7CanonicalRootGuard,
    venue: &mut V,
) -> Result<Stage7ExecutableHandoffReport, Stage7GridError> {
    let now_ms = wall_clock_ms()?;
    let current_executable = executable_sha256()?;
    let manifest = load_manifest(&request.release_manifest)?;
    if let Err(error) = manifest.validate(
        &binding,
        canonical_root.canonical_root_sha256(),
        &current_executable,
        now_ms,
    ) {
        warn!(event = "stage7_handoff_manifest_rejected", %error);
        return Err(error);
    }
    let manifest_sha256 = canonical_digest(&manifest)?;

    venue.verify_current_instrument_rules()?;
    let capability_binding = venue.capability_binding();
    let active_store =
        ProjectionStore::new(request.artifacts_root.join(STAGE7_LIVE_ADMISSION_FILE));
    let active = active_store
        .load::<Stage7LiveAdmissionEvidence>()?
        .ok_or(Stage7GridError::GridCanaryEvidence)?;
    let successor_exposure_take_profit_sha256 =
        exposure_release_digest(cfg.hedged_grid.and_then(|grid| grid.exposure_take_profit))?;
    let successor_configuration_sha256 =
        active.configuration_sha256_with_exposure(&successor_exposure_take_profit_sha256)?;

    // Recovery may resume only after the lease itself proves that the exact fenced successor is
    // already active. A receipt alone is not authority: an interrupted pre-fence attempt stays
    // stopped for manual custody review and never retries its predecessor mutation plan.
    if active.executable_sha256 == manifest.predecessor_executable_sha256
        && receipt_path(&request.artifacts_root, &manifest_sha256).exists()
    {
        let receipt = load_receipt(&request.artifacts_root, &manifest_sha256)?;
        if receipt.manifest != manifest
            || receipt.predecessor_admission != active
            || !receipt.successor_exposure_release_bound
            || receipt.successor_exposure_take_profit_sha256
                != successor_exposure_take_profit_sha256
            || receipt.successor_configuration_sha256.as_deref()
                != Some(successor_configuration_sha256.as_str())
            || !active.matches_non_executable_context(
                &capability_binding,
                venue.instrument(),
                venue.minimum_quantity(),
                now_ms,
            )?
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        let authority = WriterLeaseAuthority::open(
            request.artifacts_root.join(WRITER_FILE),
            writer_scope.clone(),
        )?;
        let successor_writer = authority
            .active_session()?
            .ok_or(Stage7GridError::ExecutableHandoff)?;
        if successor_writer.generation <= receipt.writer_generation
            || successor_writer.readback_generation < receipt.private_generation
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        let successor = active.promote_for_executable_handoff_with_exposure(
            current_executable.clone(),
            manifest_sha256.clone(),
            receipt.handoff_sha256.clone(),
            receipt.successor_exposure_take_profit_sha256.clone(),
        )?;
        active_store.save(&successor)?;
        return Ok(report(&binding, &receipt, successor_writer.generation));
    }

    if active.executable_sha256 == current_executable {
        let _ = validated_admission_predecessor(
            &capability_binding,
            venue.instrument(),
            venue.minimum_quantity(),
            &request.artifacts_root,
            now_ms,
            &current_executable,
        )?;
        if active.schema_version != 2
            || active.executable_handoff_manifest_sha256.as_deref()
                != Some(manifest_sha256.as_str())
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        let receipt = load_receipt(&request.artifacts_root, &manifest_sha256)?;
        let expected_active = receipt
            .predecessor_admission
            .promote_for_executable_handoff_with_exposure(
                current_executable.clone(),
                receipt.manifest_sha256.clone(),
                receipt.handoff_sha256.clone(),
                receipt.successor_exposure_take_profit_sha256.clone(),
            )?;
        if receipt.manifest != manifest
            || !receipt.successor_exposure_release_bound
            || receipt.successor_exposure_take_profit_sha256
                != successor_exposure_take_profit_sha256
            || receipt.successor_configuration_sha256.as_deref()
                != Some(successor_configuration_sha256.as_str())
            || expected_active != active
            || receipt.handoff_sha256
                != active
                    .executable_handoff_sha256
                    .clone()
                    .ok_or(Stage7GridError::ExecutableHandoff)?
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        let authority = WriterLeaseAuthority::open(
            request.artifacts_root.join(WRITER_FILE),
            writer_scope.clone(),
        )?;
        let successor_writer = authority.active_session()?.ok_or(Stage7GridError::Writer)?;
        if successor_writer.generation <= receipt.writer_generation
            || successor_writer.readback_generation < receipt.private_generation
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        return Ok(report(&binding, &receipt, successor_writer.generation));
    }

    let mut seen_admissions = BTreeSet::new();
    let canary_predecessor = match validate_admission_chain(
        &capability_binding,
        venue.instrument(),
        venue.minimum_quantity(),
        &request.artifacts_root,
        now_ms,
        &manifest.predecessor_executable_sha256,
        manifest.predecessor_root_sha256(),
        &active,
        0,
        &mut seen_admissions,
    ) {
        Ok(value) => value,
        Err(error) => {
            warn!(event = "stage7_handoff_admission_chain_rejected", %error);
            return Err(error);
        }
    };
    if !active.matches_non_executable_context(
        &capability_binding,
        venue.instrument(),
        venue.minimum_quantity(),
        now_ms,
    )? || active.executable_sha256 != manifest.predecessor_executable_sha256
        || active.admission_sha256 != manifest.predecessor_admission_sha256
        || manifest.valid_until_ms > active.valid_until_ms
        || release_params(cfg, &binding)? != active.parameter_release
    {
        warn!(event = "stage7_handoff_active_context_rejected");
        return Err(Stage7GridError::ExecutableHandoff);
    }
    match require_predecessor_capabilities(
        &capability_binding,
        &canary_predecessor,
        &request.artifacts_root,
        now_ms,
    ) {
        Ok(()) => {}
        Err(Stage7GridError::ExecutableHandoff)
            if super::binance_legacy_stage7_bridge::require_binance_legacy_bridge_admission(
                venue,
                &binding,
                &active.parameter_release,
                &active.exposure_take_profit_sha256,
                &request.artifacts_root,
                &active.executable_sha256,
                now_ms,
            )? => {}
        Err(error) => {
            warn!(event = "stage7_handoff_predecessor_capability_rejected", %error);
            return Err(error);
        }
    }

    let control_path = request.artifacts_root.join(CONTROL_FILE);
    let checkpoint_path = request.artifacts_root.join(CHECKPOINT_FILE);
    let command_path = request.artifacts_root.join(COMMAND_FILE);
    let writer_path = request.artifacts_root.join(WRITER_FILE);
    let control = ProjectionStore::new(&control_path)
        .load::<Stage7GridControl>()?
        .ok_or(Stage7GridError::ExecutableHandoff)?;
    let checkpoint_store = ProjectionStore::new(&checkpoint_path);
    let mut checkpoint = checkpoint_store
        .load::<Stage7GridCheckpoint>()?
        .ok_or(Stage7GridError::ExecutableHandoff)?;
    venue.set_fill_history_start_ms(checkpoint.fill_history_start_ms);
    let checkpoint_generation_before_handoff = checkpoint.private_generation;
    let clean_local_stop = checkpoint.state.phase == GridPhase::Stopping
        && checkpoint.state.owned_orders.is_empty()
        && checkpoint.state.pending_transactions.is_empty()
        && checkpoint.state.pending_replenishments.is_empty();
    let recoverable_local_stop = request.confirm_mainnet_stopped_order_recovery
        && stopped_order_recovery_phase(checkpoint.state.phase)
        && checkpoint.state.pending_replenishments.is_empty();
    if control.schema_version != 1
        || control.binding != binding
        || control.target != HedgedGridControlTarget::Stop
        || checkpoint.schema_version != 1
        || checkpoint.binding != binding
        || checkpoint.state.binding != binding
        || checkpoint.state.params != active.parameter_release
        || (!clean_local_stop && !recoverable_local_stop)
        || !handoff_health_fence_allowed(checkpoint.order_health_fenced, recoverable_local_stop)
    {
        warn!(
            event = "stage7_handoff_stopped_state_rejected",
            control = ?control.target,
            phase = ?checkpoint.state.phase,
            owned_orders = checkpoint.state.owned_orders.len(),
            pending_transactions = checkpoint.state.pending_transactions.len()
        );
        return Err(Stage7GridError::ExecutableHandoff);
    }

    let mut commands = CommandJournal::open(&command_path)?;
    if commands.has_unresolved() && !request.confirm_mainnet_stopped_order_recovery {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let authority = WriterLeaseAuthority::open(&writer_path, writer_scope.clone())?;
    let writer = authority
        .active_entry_session()?
        .ok_or(Stage7GridError::ExecutableHandoff)?;

    let mut evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let recovered_generation = evidence.last_generation();
    let mut private_generation = checkpoint
        .private_generation
        .max(recovered_generation)
        .max(writer.readback_generation);
    let (readback, inventory) =
        canary_cleanup_readback(venue, &mut evidence, &mut private_generation, &binding)?;
    let predecessor_lease_elapsed = writer.valid_until_ms <= inventory.private_observed_at_ms;
    let writer = refresh_expired_stopped_writer_after_signed_readback(
        &authority,
        &writer,
        private_generation,
        inventory.private_observed_at_ms,
    )?;
    let (readback, inventory) = recover_stopped_orders_for_handoff(
        request.confirm_mainnet_stopped_order_recovery,
        &mut checkpoint,
        &checkpoint_store,
        &mut commands,
        venue,
        &authority,
        &writer,
        &binding,
        &mut evidence,
        &mut private_generation,
        readback,
        inventory,
    )?;
    if !all_order_families_empty(&readback)?
        || checkpoint.state.phase != GridPhase::Stopping
        || !checkpoint.state.owned_orders.is_empty()
        || !checkpoint.state.pending_transactions.is_empty()
        || !checkpoint.state.pending_replenishments.is_empty()
        || commands.has_unresolved()
        || (inventory.long_quantity <= Decimal::ZERO && inventory.short_quantity <= Decimal::ZERO)
        || private_generation <= checkpoint_generation_before_handoff
        || !writer_readback_is_not_ahead(private_generation, &writer)
        || (!predecessor_lease_elapsed && writer.valid_until_ms > inventory.private_observed_at_ms)
    {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    if request.archive_resolved_command_wal {
        drop(commands);
        archive_resolved_command_wal(&request.artifacts_root, &command_path)?;
    }
    if handoff_fill_history_window_can_advance(checkpoint.pending_exposure_reduction.is_some())
        && advance_bitget_fill_history_window(&mut checkpoint, venue, now_ms)
    {
        checkpoint_store.save(&checkpoint)?;
    }

    let existing_receipt = receipt_path(&request.artifacts_root, &manifest_sha256);
    // A receipt from an interrupted pre-fence run cannot be replayed against a newer private
    // observation. It remains immutable audit evidence; a human must reconcile that stopped
    // state rather than letting a successor reuse an old custody proof automatically.
    if existing_receipt.exists() {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let receipt = {
        let private_evidence_snapshot =
            stage7_private_evidence_snapshot(&request.artifacts_root, &binding)?;
        let private_snapshot_sha256 = canonical_digest(&Stage7PrivateStopSnapshot {
            binding: &binding,
            private_generation,
            observed_at_ms: inventory.private_observed_at_ms,
            long_quantity: inventory.long_quantity,
            short_quantity: inventory.short_quantity,
            hedge_position: readback.hedge_position,
            orders_empty: all_order_families_empty(&readback)?,
        })?;
        let mut receipt = Stage7ExecutableHandoffReceipt {
            schema_version: HANDOFF_SCHEMA_VERSION,
            manifest,
            manifest_sha256: manifest_sha256.clone(),
            predecessor_admission: active.clone(),
            writer_scope_sha256: canonical_digest(&writer_scope)?,
            writer_generation: writer.generation,
            writer_revision: writer.revision,
            writer_readback_generation: writer.readback_generation,
            control_sha256: file_sha256(&control_path)?,
            checkpoint_sha256: file_sha256(&checkpoint_path)?,
            command_journal_sha256: file_sha256(&command_path)?,
            writer_state_sha256: file_sha256(&writer_path)?,
            private_snapshot_sha256,
            private_generation,
            observed_at_ms: inventory.private_observed_at_ms,
            long_quantity: inventory.long_quantity,
            short_quantity: inventory.short_quantity,
            hedge_position: readback.hedge_position,
            orders_empty: true,
            wal_resolved: true,
            local_transactions_empty: true,
            order_health_clear: true,
            successor_exposure_release_bound: true,
            successor_exposure_take_profit_sha256: successor_exposure_take_profit_sha256.clone(),
            successor_configuration_sha256: Some(successor_configuration_sha256),
            private_evidence_recovery_manifest_sha256: private_evidence_snapshot
                .recovery_manifest_sha256,
            private_evidence_journal_sha256: Some(private_evidence_snapshot.journal_sha256),
            private_evidence_journal_bytes: Some(private_evidence_snapshot.journal_bytes),
            handoff_sha256: String::new(),
        };
        receipt.handoff_sha256 = receipt.expected_handoff_sha256()?;
        receipt.validate_static()?;
        persist_receipt(&request.artifacts_root, &receipt)?;
        receipt
    };

    let handoff_writer_receipt =
        writer_handoff_receipt(&receipt, &writer_scope, &writer, &current_executable)?;
    authority.fence_for_executable_handoff(&handoff_writer_receipt)?;
    let successor_writer = authority.activate_executable_handoff_successor(
        &handoff_writer_receipt,
        &current_executable,
        now_ms,
    )?;

    let successor = active.promote_for_executable_handoff_with_exposure(
        current_executable,
        manifest_sha256,
        receipt.handoff_sha256.clone(),
        receipt.successor_exposure_take_profit_sha256.clone(),
    )?;
    active_store.save(&successor)?;
    Ok(report(&binding, &receipt, successor_writer.generation))
}

fn archive_resolved_command_wal(
    artifacts_root: &Path,
    command_path: &Path,
) -> Result<(), Stage7GridError> {
    let metadata = fs::metadata(command_path).map_err(|source| Stage7GridError::Io {
        path: command_path.to_path_buf(),
        source,
    })?;
    if metadata.len() == 0 {
        return Ok(());
    }
    let source_sha256 = file_sha256(command_path)?;
    let archive_directory = artifacts_root.join(COMMAND_WAL_ARCHIVE_DIRECTORY);
    fs::create_dir_all(&archive_directory).map_err(|source| Stage7GridError::Io {
        path: archive_directory.clone(),
        source,
    })?;
    let archive_path = archive_directory.join(format!("commands-{source_sha256}.jsonl"));
    if archive_path.exists() {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let replacement_path = artifacts_root.join("commands.jsonl.rotating");
    let replacement = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&replacement_path)
        .map_err(|source| Stage7GridError::Io {
            path: replacement_path.clone(),
            source,
        })?;
    replacement
        .sync_all()
        .map_err(|source| Stage7GridError::Io {
            path: replacement_path.clone(),
            source,
        })?;
    fs::rename(command_path, &archive_path).map_err(|source| Stage7GridError::Io {
        path: archive_path,
        source,
    })?;
    fs::rename(&replacement_path, command_path).map_err(|source| Stage7GridError::Io {
        path: command_path.to_path_buf(),
        source,
    })?;
    sync_directory(&archive_directory)?;
    sync_directory(artifacts_root)?;
    Ok(())
}

fn writer_handoff_receipt(
    receipt: &Stage7ExecutableHandoffReceipt,
    scope: &WriterScope,
    predecessor: &WriterSession,
    successor_executable_sha256: &str,
) -> Result<ExecutableHandoffReceipt, Stage7GridError> {
    if predecessor.scope != *scope
        || predecessor.generation != receipt.writer_generation
        || predecessor.revision != receipt.writer_revision
        || predecessor.readback_generation != receipt.writer_readback_generation
        || successor_executable_sha256 != receipt.manifest.successor_executable_sha256
    {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    Ok(ExecutableHandoffReceipt {
        receipt_id: receipt.handoff_sha256.clone(),
        predecessor: predecessor.clone(),
        scope: scope.clone(),
        readback_generation: receipt.private_generation,
        handoff_sha256: receipt.handoff_sha256.clone(),
        successor_executable_sha256: successor_executable_sha256.to_owned(),
    })
}

fn stopped_order_recovery_phase(phase: GridPhase) -> bool {
    matches!(
        phase,
        GridPhase::Running
            | GridPhase::ResettingGrid
            | GridPhase::Stopping
            | GridPhase::BlockedUnknown
    )
}

/// A Stop does not elect a replacement writer. If the predecessor lease elapsed while its first
/// signed Stop readback was settling, reopen that exact session only from this newer signed
/// generation before issuing a cancellation or fencing it for handoff.
fn refresh_expired_stopped_writer_after_signed_readback(
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    private_generation: u64,
    observed_at_ms: u64,
) -> Result<WriterSession, Stage7GridError> {
    if writer.valid_until_ms > observed_at_ms {
        return Ok(writer.clone());
    }
    authority
        .recover_same_scope_after_readback(writer, private_generation, observed_at_ms)
        .map_err(Into::into)
}

const fn writer_readback_is_not_ahead(private_generation: u64, writer: &WriterSession) -> bool {
    private_generation >= writer.readback_generation
}

const fn handoff_health_fence_allowed(fenced: bool, recoverable_local_stop: bool) -> bool {
    !fenced || recoverable_local_stop
}

const fn handoff_fill_history_window_can_advance(pending_exposure_reduction: bool) -> bool {
    !pending_exposure_reduction
}

#[allow(clippy::too_many_arguments)]
fn recover_stopped_orders_for_handoff<V: Stage7CanaryVenue>(
    confirmed: bool,
    checkpoint: &mut Stage7GridCheckpoint,
    checkpoint_store: &ProjectionStore,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    evidence: &mut PrivateEvidenceJournal,
    private_generation: &mut u64,
    mut readback: GridVenueReadback,
    mut inventory: GridInventory,
) -> Result<(GridVenueReadback, GridInventory), Stage7GridError> {
    let recovery_required = !all_order_families_empty(&readback)?
        || commands.has_unresolved()
        || checkpoint.state.phase != GridPhase::Stopping
        || !checkpoint.state.owned_orders.is_empty()
        || !checkpoint.state.pending_transactions.is_empty();
    if !recovery_required {
        return Ok((readback, inventory));
    }
    if !confirmed {
        return Err(Stage7GridError::ExecutableHandoff);
    }

    // Prepared proves no network call began; Submitted is fenced to Unknown before any signed
    // settlement. This successor is still not admitted for opening risk and may only settle WAL
    // plus cancel exact accepted orders under the already durable Stop target.
    for attempt in 0..STOP_RECOVERY_READBACK_ATTEMPTS {
        settle_interrupted_wal_from_signed_readback(
            commands,
            &checkpoint.state,
            binding,
            &readback,
        )?;
        let _ = recovered_owned_orders(commands, binding, &readback)?;
        recover_unresolved(
            commands, venue, authority, writer, binding, &readback, false,
        )?;
        if !commands.has_unresolved() {
            break;
        }
        if attempt + 1 < STOP_RECOVERY_READBACK_ATTEMPTS {
            std::thread::sleep(std::time::Duration::from_millis(REJECTED_CANCEL_RETRY_MS));
            (readback, inventory) =
                canary_cleanup_readback(venue, evidence, private_generation, binding)?;
        }
    }
    if commands.has_unresolved() {
        return Err(Stage7GridError::ExecutableHandoff);
    }

    let owned = recovered_owned_orders(commands, binding, &readback)?;
    checkpoint.state.phase = GridPhase::Stopping;
    checkpoint.state.reconcile_stopping_orders(owned)?;
    checkpoint.private_generation = *private_generation;
    checkpoint_store.save(checkpoint)?;

    if !all_order_families_empty(&readback)? {
        cancel_visible_owned_orders(
            commands,
            venue,
            authority,
            writer,
            binding,
            &checkpoint.state,
            &readback.orders,
        )?;
        let mut empty = false;
        for _ in 0..STOP_CANCEL_SETTLE_ATTEMPTS {
            std::thread::sleep(std::time::Duration::from_millis(REJECTED_CANCEL_RETRY_MS));
            (readback, inventory) =
                canary_cleanup_readback(venue, evidence, private_generation, binding)?;
            if all_order_families_empty(&readback)? {
                empty = true;
                break;
            }
            let _ = recovered_owned_orders(commands, binding, &readback)?;
            if !signed_orders_have_accepted_cancels(commands, &readback)? {
                return Err(Stage7GridError::ExecutableHandoff);
            }
        }
        if !empty {
            return Err(Stage7GridError::ExecutableHandoff);
        }
    }

    checkpoint
        .state
        .reconcile_stopping_orders(BTreeMap::new())?;
    checkpoint.private_generation = *private_generation;
    // A signed empty owned-order surface resolves the exact condition guarded by order health.
    // Clearing the checkpoint fence here does not reopen risk: durable control and phase remain
    // Stop/Stopping, and only a later explicit Reset may start a successor.
    checkpoint.order_health_fenced = false;
    checkpoint_store.save(checkpoint)?;
    if commands.has_unresolved() {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    Ok((readback, inventory))
}

fn all_order_families_empty(readback: &GridVenueReadback) -> Result<bool, Stage7GridError> {
    readback
        .all_order_families_empty()
        .map_err(|_| Stage7GridError::OrderFamily)
}

/// A durable Stop or an expired BlockedUnknown fence may terminate interrupted WAL identities
/// only from a complete signed order and fill readback. Resident recovery and executable handoff
/// share this fail-closed settlement boundary.
pub(super) fn settle_interrupted_wal_from_signed_readback(
    commands: &mut CommandJournal,
    state: &HedgedGridState,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
) -> Result<(), Stage7GridError> {
    let _ = commands.fence_interrupted_dispatches()?;
    settle_signed_visible_order_receipts(commands, state, binding, readback)?;
    settle_signed_fill_place_receipts(commands, binding, readback)?;
    reject_ineffective_unknown_cancels(commands, readback)?;
    reject_absent_unknown_places_after_signed_readback(commands, readback)?;
    Ok(())
}

fn settle_signed_fill_place_receipts(
    commands: &mut CommandJournal,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
) -> Result<(), Stage7GridError> {
    for record in &readback.fills {
        let FieldState::Known(client_order_id) = &record.client_order_id else {
            continue;
        };
        let Ok(client_order_id) = CommandId::new(client_order_id) else {
            continue;
        };
        let Some(command_id) = commands.command_id_by_client_id(&client_order_id).cloned() else {
            continue;
        };
        let Some(command) = commands.place_by_client_id(&client_order_id).cloned() else {
            continue;
        };
        let state = commands
            .receipt(&command_id)
            .map(|receipt| receipt.state.clone())
            .ok_or(Stage7GridError::Unresolved)?;
        if state.terminal() {
            continue;
        }
        validate_owner_binding(&command.owner, binding)?;
        if record.fill.order_id.trim().is_empty()
            || record.fill.symbol != binding.symbol
            || record.fill.side != command.side
            || record.fill.position_side != FieldState::Known(command.position_side)
            || record.fill.quantity <= Decimal::ZERO
            || record.fill.quantity > command.quantity
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        commands.transition(
            &command_id,
            CommandState::Accepted {
                venue_order_id: record.fill.order_id.clone(),
            },
        )?;
    }
    Ok(())
}

fn reject_ineffective_unknown_cancels(
    commands: &mut CommandJournal,
    readback: &GridVenueReadback,
) -> Result<(), Stage7GridError> {
    for command_id in commands.unresolved_command_ids() {
        let Some(cancel) = commands.cancel(&command_id).cloned() else {
            continue;
        };
        let target_still_open = readback.orders.iter().any(|order| {
            matches!(order.state, OrderState::New | OrderState::PartiallyFilled)
                && matches!(
                    &order.client_order_id,
                    FieldState::Known(client_order_id)
                        if client_order_id == cancel.target_client_order_id.as_str()
                )
        });
        if target_still_open {
            commands.transition(
                &command_id,
                CommandState::Rejected {
                    reason: "signed_target_still_open_after_stopped_recovery_deadline".to_owned(),
                },
            )?;
        }
    }
    Ok(())
}

fn reject_absent_unknown_places_after_signed_readback(
    commands: &mut CommandJournal,
    readback: &GridVenueReadback,
) -> Result<(), Stage7GridError> {
    for command_id in commands.unresolved_command_ids() {
        let Some(receipt) = commands.receipt(&command_id) else {
            return Err(Stage7GridError::Unresolved);
        };
        if !matches!(receipt.state, CommandState::Unknown { .. }) {
            continue;
        }
        let Some(place) = commands.place(&command_id).cloned() else {
            continue;
        };
        let visible = readback.orders.iter().any(|order| {
            matches!(order.state, OrderState::New | OrderState::PartiallyFilled)
                && matches!(
                    &order.client_order_id,
                    FieldState::Known(client_order_id)
                        if client_order_id == place.client_order_id.as_str()
                )
        });
        let filled = readback.fills.iter().any(|record| {
            matches!(
                &record.client_order_id,
                FieldState::Known(client_order_id)
                    if client_order_id == place.client_order_id.as_str()
            )
        });
        if !visible && !filled {
            commands.transition(
                &command_id,
                CommandState::Rejected {
                    reason: "absent_from_complete_signed_orders_and_fill_history".to_owned(),
                },
            )?;
        }
    }
    Ok(())
}

fn signed_orders_have_accepted_cancels(
    commands: &CommandJournal,
    readback: &GridVenueReadback,
) -> Result<bool, Stage7GridError> {
    for order in &readback.orders {
        let FieldState::Known(client_order_id) = &order.client_order_id else {
            return Err(Stage7GridError::ForeignOrders);
        };
        let client_order_id =
            CommandId::new(client_order_id).map_err(|_| Stage7GridError::ForeignOrders)?;
        if !commands.has_accepted_cancel_for(&client_order_id) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn require_predecessor_capabilities(
    binding: &CapabilityBinding,
    predecessor: &Stage7LiveAdmissionEvidence,
    artifacts_root: &Path,
    now_ms: u64,
) -> Result<(), Stage7GridError> {
    let current = CapabilityEvidenceStore::open(artifacts_root.join(CAPABILITY_EVIDENCE_FILE))?
        .current(binding, now_ms)?;
    let required = [
        Capability::InstrumentRules,
        Capability::PublicMarket,
        Capability::PrivateReadback,
        Capability::PrivateStream,
        Capability::PlaceLimit,
        Capability::Cancel,
        Capability::ReduceOnly,
        Capability::Reconciliation,
    ];
    let expected = basic_canary_hashes(
        binding,
        &predecessor.deployment_binding,
        &predecessor.parameter_release,
        &predecessor.executable_sha256,
        &required,
    )?;
    let basics_valid = required
        .into_iter()
        .zip(expected)
        .all(|(capability, expected_hash)| {
            current
                .get(&capability)
                .is_some_and(|evidence| evidence.evidence_hash == expected_hash)
        });
    let lifecycle_valid = current
        .get(&Capability::GridLifecycle)
        .is_some_and(|evidence| {
            evidence.evidence_hash == predecessor.admission_sha256
                && evidence.verified_at_ms == predecessor.verified_at_ms
                && evidence.valid_until_ms == predecessor.valid_until_ms
        });
    if basics_valid && lifecycle_valid {
        Ok(())
    } else {
        Err(Stage7GridError::ExecutableHandoff)
    }
}

fn report(
    binding: &HedgedGridBinding,
    receipt: &Stage7ExecutableHandoffReceipt,
    writer_generation: u64,
) -> Stage7ExecutableHandoffReport {
    Stage7ExecutableHandoffReport {
        exchange: binding.exchange.clone(),
        symbol: binding.symbol.to_string(),
        predecessor_executable_sha256: receipt.manifest.predecessor_executable_sha256.clone(),
        successor_executable_sha256: receipt.manifest.successor_executable_sha256.clone(),
        private_generation: receipt.private_generation,
        writer_generation,
        handoff_sha256: receipt.handoff_sha256.clone(),
        positions_preserved: receipt.long_quantity > Decimal::ZERO
            || receipt.short_quantity > Decimal::ZERO,
    }
}

fn load_manifest(path: &Path) -> Result<Stage7ExecutableHandoffManifest, Stage7GridError> {
    let encoded = fs::read(path).map_err(|source| Stage7GridError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&encoded).map_err(|_| Stage7GridError::ExecutableHandoff)
}

fn receipt_path(artifacts_root: &Path, manifest_sha256: &str) -> PathBuf {
    artifacts_root
        .join(HANDOFF_DIRECTORY)
        .join(format!("{manifest_sha256}.json"))
}

fn load_receipt(
    artifacts_root: &Path,
    manifest_sha256: &str,
) -> Result<Stage7ExecutableHandoffReceipt, Stage7GridError> {
    if !valid_sha256(manifest_sha256) {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let path = receipt_path(artifacts_root, manifest_sha256);
    let encoded = fs::read(&path).map_err(|source| Stage7GridError::Io {
        path: path.clone(),
        source,
    })?;
    let receipt: Stage7ExecutableHandoffReceipt =
        serde_json::from_slice(&encoded).map_err(|_| Stage7GridError::ExecutableHandoff)?;
    receipt.validate_static()?;
    receipt.validate_private_evidence_snapshot(artifacts_root)?;
    Ok(receipt)
}

fn persist_receipt(
    artifacts_root: &Path,
    receipt: &Stage7ExecutableHandoffReceipt,
) -> Result<(), Stage7GridError> {
    receipt.validate_static()?;
    let directory = artifacts_root.join(HANDOFF_DIRECTORY);
    fs::create_dir_all(&directory).map_err(|source| Stage7GridError::Io {
        path: directory.clone(),
        source,
    })?;
    let path = receipt_path(artifacts_root, &receipt.manifest_sha256);
    let encoded = serde_json::to_vec(receipt).map_err(CapabilityEvidenceError::Encode)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|source| Stage7GridError::Io {
            path: path.clone(),
            source,
        })?;
    file.write_all(&encoded)
        .and_then(|()| file.sync_all())
        .map_err(|source| Stage7GridError::Io {
            path: path.clone(),
            source,
        })?;
    sync_directory(&directory)
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> Result<(), Stage7GridError> {
    OpenOptions::new()
        .read(true)
        .open(directory)
        .and_then(|handle| handle.sync_all())
        .map_err(|source| Stage7GridError::Io {
            path: directory.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) -> Result<(), Stage7GridError> {
    Ok(())
}

fn file_sha256(path: &Path) -> Result<String, Stage7GridError> {
    fs::read(path)
        .map(sha256_hex)
        .map_err(|source| Stage7GridError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn canonical_root_sha256(artifacts_root: &Path) -> Result<String, Stage7GridError> {
    let canonical = fs::canonicalize(artifacts_root).map_err(|source| Stage7GridError::Io {
        path: artifacts_root.to_path_buf(),
        source,
    })?;
    let canonical = canonical
        .to_str()
        .ok_or(Stage7GridError::ExecutableHandoff)?;
    Ok(sha256_hex(canonical.as_bytes()))
}

#[cfg(test)]
#[path = "stage7_executable_handoff_tests.rs"]
mod tests;
