use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::domain::Asset;
use crate::{config::ExposureTakeProfitConfig, domain::Instrument};

use super::*;

const CANARY_POSITION_WAIT_ATTEMPTS: u8 = 120;
const CANARY_POSITION_WAIT_INTERVAL_MS: u64 = 250;
const CANARY_PUBLIC_WARMUP_ATTEMPTS: u8 = 150;
const CANARY_PUBLIC_WARMUP_INTERVAL_MS: u64 = 100;
pub(super) const STAGE7_LIVE_ADMISSION_FILE: &str = "stage7_live_admission.json";
const STAGE7_LIVE_ADMISSION_SCHEMA_VERSION: u16 = 1;
const STAGE7_HANDOFF_ADMISSION_SCHEMA_VERSION: u16 = 2;
const STAGE7_RELEASE_ID: &str = "stage7_low_balance_hedged_grid_v1";
const STAGE7_SELECTION_RULE: &str = "stage7_mainstream_usdt_linear_5_to_6_quote_v1";
const STAGE7_CONFIGURED_SELECTION_RULE: &str = "stage7_configured_linear_5_to_6_quote_v2";

/// The Live admission record contains only canonical deployment identities, normalized rules and
/// digests. It deliberately excludes credentials, raw private payloads, balances and orders.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stage7LiveAdmissionEvidence {
    pub(super) schema_version: u16,
    pub(super) capability_binding: CapabilityBinding,
    pub(super) deployment_binding: HedgedGridBinding,
    pub(super) parameter_release: HedgedGridParams,
    pub(super) instrument_rules: Stage7InstrumentRulesEvidence,
    pub(super) release_id: String,
    pub(super) selection_rule: String,
    pub(super) executable_sha256: String,
    #[serde(default)]
    pub(super) exposure_release_bound: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) exposure_take_profit_sha256: Option<String>,
    pub(super) configuration_sha256: String,
    pub(super) parameter_release_sha256: String,
    pub(super) instrument_rules_sha256: String,
    pub(super) verified_at_ms: u64,
    pub(super) valid_until_ms: u64,
    pub(super) private_generation: u64,
    pub(super) health_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) predecessor_admission_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) executable_handoff_manifest_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) executable_handoff_sha256: Option<String>,
    pub(super) admission_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stage7InstrumentRulesEvidence {
    pub(super) instrument: Instrument,
    #[serde(with = "rust_decimal::serde::str")]
    pub(super) minimum_quantity: Decimal,
}

#[derive(Serialize)]
struct Stage7ConfigurationDigest<'a> {
    release_id: &'a str,
    selection_rule: &'a str,
    capability_binding: &'a CapabilityBinding,
    deployment_binding: &'a HedgedGridBinding,
    grid_count: u8,
    #[serde(with = "rust_decimal::serde::str")]
    single_order_max_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    replenishment_max_notional: Decimal,
    #[serde(skip_serializing_if = "is_false")]
    exposure_release_bound: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    exposure_take_profit_sha256: &'a Option<String>,
}

#[derive(Serialize)]
struct Stage7ExposureReleaseDigest {
    enabled: bool,
    shadow: bool,
    #[serde(with = "rust_decimal::serde::str")]
    position_equity_multiple: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    unrealized_pnl_equity_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    reduce_ratio: Decimal,
    snapshot_interval_ms: u64,
    max_snapshot_age_ms: u64,
    rearm_clear_generations: u8,
}

#[derive(Serialize)]
struct Stage7AdmissionDigest<'a> {
    schema_version: u16,
    capability_binding: &'a CapabilityBinding,
    deployment_binding: &'a HedgedGridBinding,
    parameter_release: &'a HedgedGridParams,
    instrument_rules: &'a Stage7InstrumentRulesEvidence,
    release_id: &'a str,
    selection_rule: &'a str,
    executable_sha256: &'a str,
    #[serde(skip_serializing_if = "is_false")]
    exposure_release_bound: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    exposure_take_profit_sha256: &'a Option<String>,
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
}

impl Stage7LiveAdmissionEvidence {
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub(super) fn new(
        capability_binding: CapabilityBinding,
        deployment_binding: HedgedGridBinding,
        parameter_release: HedgedGridParams,
        instrument: Instrument,
        minimum_quantity: Decimal,
        executable_sha256: String,
        verified_at_ms: u64,
        valid_until_ms: u64,
        private_generation: u64,
        health_generation: u64,
    ) -> Result<Self, Stage7GridError> {
        Self::new_with_exposure(
            capability_binding,
            deployment_binding,
            parameter_release,
            instrument,
            minimum_quantity,
            None,
            executable_sha256,
            verified_at_ms,
            valid_until_ms,
            private_generation,
            health_generation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_exposure(
        capability_binding: CapabilityBinding,
        deployment_binding: HedgedGridBinding,
        parameter_release: HedgedGridParams,
        instrument: Instrument,
        minimum_quantity: Decimal,
        exposure_take_profit: Option<ExposureTakeProfitConfig>,
        executable_sha256: String,
        verified_at_ms: u64,
        valid_until_ms: u64,
        private_generation: u64,
        health_generation: u64,
    ) -> Result<Self, Stage7GridError> {
        capability_binding.validate()?;
        deployment_binding.validate()?;
        parameter_release.validate()?;
        instrument
            .validate()
            .map_err(|_| Stage7GridError::GridCanaryEvidence)?;
        if !valid_sha256(&executable_sha256)
            || minimum_quantity <= Decimal::ZERO
            || verified_at_ms == 0
            || valid_until_ms <= verified_at_ms
            || private_generation == 0
            || health_generation == 0
            || !stage7_context_matches(
                &capability_binding,
                &deployment_binding,
                &parameter_release,
                &instrument,
            )
        {
            return Err(Stage7GridError::GridCanaryEvidence);
        }
        let instrument_rules = Stage7InstrumentRulesEvidence {
            instrument,
            minimum_quantity,
        };
        let selection_rule = selection_rule_for(&deployment_binding, &parameter_release).to_owned();
        let exposure_take_profit_sha256 = exposure_release_digest(exposure_take_profit)?;
        let configuration_sha256 = configuration_digest(
            &capability_binding,
            &deployment_binding,
            parameter_release.grid_count,
            &selection_rule,
            true,
            &exposure_take_profit_sha256,
        )?;
        let parameter_release_sha256 = canonical_digest(&parameter_release)?;
        let instrument_rules_sha256 = canonical_digest(&instrument_rules)?;
        let mut evidence = Self {
            schema_version: STAGE7_LIVE_ADMISSION_SCHEMA_VERSION,
            capability_binding,
            deployment_binding,
            parameter_release,
            instrument_rules,
            release_id: STAGE7_RELEASE_ID.to_owned(),
            selection_rule,
            executable_sha256,
            exposure_release_bound: true,
            exposure_take_profit_sha256,
            configuration_sha256,
            parameter_release_sha256,
            instrument_rules_sha256,
            verified_at_ms,
            valid_until_ms,
            private_generation,
            health_generation,
            predecessor_admission_sha256: None,
            executable_handoff_manifest_sha256: None,
            executable_handoff_sha256: None,
            admission_sha256: String::new(),
        };
        evidence.admission_sha256 = evidence.expected_admission_sha256()?;
        evidence.validate()?;
        Ok(evidence)
    }

    pub(super) fn validate(&self) -> Result<(), Stage7GridError> {
        self.capability_binding.validate()?;
        self.deployment_binding.validate()?;
        self.parameter_release.validate()?;
        self.instrument_rules
            .instrument
            .validate()
            .map_err(|_| Stage7GridError::GridCanaryEvidence)?;
        let handoff_fields_valid = match self.schema_version {
            STAGE7_LIVE_ADMISSION_SCHEMA_VERSION => {
                self.predecessor_admission_sha256.is_none()
                    && self.executable_handoff_manifest_sha256.is_none()
                    && self.executable_handoff_sha256.is_none()
            }
            STAGE7_HANDOFF_ADMISSION_SCHEMA_VERSION => {
                self.predecessor_admission_sha256
                    .as_deref()
                    .is_some_and(valid_sha256)
                    && self
                        .executable_handoff_manifest_sha256
                        .as_deref()
                        .is_some_and(valid_sha256)
                    && self
                        .executable_handoff_sha256
                        .as_deref()
                        .is_some_and(valid_sha256)
            }
            _ => false,
        };
        let exposure_fields_valid =
            self.exposure_release_bound || self.exposure_take_profit_sha256.is_none();
        if !handoff_fields_valid
            || !exposure_fields_valid
            || self
                .exposure_take_profit_sha256
                .as_deref()
                .is_some_and(|digest| !valid_sha256(digest))
            || self.release_id != STAGE7_RELEASE_ID
            || self.selection_rule
                != selection_rule_for(&self.deployment_binding, &self.parameter_release)
            || self.instrument_rules.minimum_quantity <= Decimal::ZERO
            || !valid_sha256(&self.executable_sha256)
            || !valid_sha256(&self.configuration_sha256)
            || !valid_sha256(&self.parameter_release_sha256)
            || !valid_sha256(&self.instrument_rules_sha256)
            || !valid_sha256(&self.admission_sha256)
            || self.verified_at_ms == 0
            || self.valid_until_ms <= self.verified_at_ms
            || self.private_generation == 0
            || self.health_generation == 0
            || !stage7_context_matches(
                &self.capability_binding,
                &self.deployment_binding,
                &self.parameter_release,
                &self.instrument_rules.instrument,
            )
            || self.configuration_sha256
                != configuration_digest(
                    &self.capability_binding,
                    &self.deployment_binding,
                    self.parameter_release.grid_count,
                    &self.selection_rule,
                    self.exposure_release_bound,
                    &self.exposure_take_profit_sha256,
                )?
            || self.parameter_release_sha256 != canonical_digest(&self.parameter_release)?
            || self.instrument_rules_sha256 != canonical_digest(&self.instrument_rules)?
            || self.admission_sha256 != self.expected_admission_sha256()?
        {
            return Err(Stage7GridError::GridCanaryEvidence);
        }
        Ok(())
    }

    fn expected_admission_sha256(&self) -> Result<String, Stage7GridError> {
        canonical_digest(&Stage7AdmissionDigest {
            schema_version: self.schema_version,
            capability_binding: &self.capability_binding,
            deployment_binding: &self.deployment_binding,
            parameter_release: &self.parameter_release,
            instrument_rules: &self.instrument_rules,
            release_id: &self.release_id,
            selection_rule: &self.selection_rule,
            executable_sha256: &self.executable_sha256,
            exposure_release_bound: self.exposure_release_bound,
            exposure_take_profit_sha256: &self.exposure_take_profit_sha256,
            configuration_sha256: &self.configuration_sha256,
            parameter_release_sha256: &self.parameter_release_sha256,
            instrument_rules_sha256: &self.instrument_rules_sha256,
            verified_at_ms: self.verified_at_ms,
            valid_until_ms: self.valid_until_ms,
            private_generation: self.private_generation,
            health_generation: self.health_generation,
            predecessor_admission_sha256: &self.predecessor_admission_sha256,
            executable_handoff_manifest_sha256: &self.executable_handoff_manifest_sha256,
            executable_handoff_sha256: &self.executable_handoff_sha256,
        })
    }

    #[cfg(test)]
    pub(super) fn promote_for_executable_handoff(
        &self,
        successor_executable_sha256: String,
        manifest_sha256: String,
        handoff_sha256: String,
    ) -> Result<Self, Stage7GridError> {
        self.promote_for_executable_handoff_with_exposure(
            successor_executable_sha256,
            manifest_sha256,
            handoff_sha256,
            self.exposure_take_profit_sha256.clone(),
        )
    }

    pub(super) fn promote_for_executable_handoff_with_exposure(
        &self,
        successor_executable_sha256: String,
        manifest_sha256: String,
        handoff_sha256: String,
        successor_exposure_take_profit_sha256: Option<String>,
    ) -> Result<Self, Stage7GridError> {
        self.validate()?;
        if !matches!(
            self.schema_version,
            STAGE7_LIVE_ADMISSION_SCHEMA_VERSION | STAGE7_HANDOFF_ADMISSION_SCHEMA_VERSION
        ) || !valid_sha256(&successor_executable_sha256)
            || successor_executable_sha256 == self.executable_sha256
            || !valid_sha256(&manifest_sha256)
            || !valid_sha256(&handoff_sha256)
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        let mut successor = self.clone();
        successor.schema_version = STAGE7_HANDOFF_ADMISSION_SCHEMA_VERSION;
        successor.predecessor_admission_sha256 = Some(self.admission_sha256.clone());
        successor.executable_handoff_manifest_sha256 = Some(manifest_sha256);
        successor.executable_handoff_sha256 = Some(handoff_sha256);
        successor.executable_sha256 = successor_executable_sha256;
        successor.exposure_release_bound = true;
        successor.exposure_take_profit_sha256 = successor_exposure_take_profit_sha256;
        successor.configuration_sha256 = configuration_digest(
            &successor.capability_binding,
            &successor.deployment_binding,
            successor.parameter_release.grid_count,
            &successor.selection_rule,
            true,
            &successor.exposure_take_profit_sha256,
        )?;
        successor.admission_sha256 = successor.expected_admission_sha256()?;
        successor.validate()?;
        Ok(successor)
    }

    /// Reconstructs only an already-durable historical handoff created before exposure release
    /// binding existed. New handoffs must use `promote_for_executable_handoff_with_exposure` and
    /// can never call this compatibility path.
    pub(super) fn promote_for_legacy_unbound_executable_handoff(
        &self,
        successor_executable_sha256: String,
        manifest_sha256: String,
        handoff_sha256: String,
    ) -> Result<Self, Stage7GridError> {
        self.validate()?;
        if self.exposure_release_bound
            || self.exposure_take_profit_sha256.is_some()
            || !matches!(
                self.schema_version,
                STAGE7_LIVE_ADMISSION_SCHEMA_VERSION | STAGE7_HANDOFF_ADMISSION_SCHEMA_VERSION
            )
            || !valid_sha256(&successor_executable_sha256)
            || successor_executable_sha256 == self.executable_sha256
            || !valid_sha256(&manifest_sha256)
            || !valid_sha256(&handoff_sha256)
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        let mut successor = self.clone();
        successor.schema_version = STAGE7_HANDOFF_ADMISSION_SCHEMA_VERSION;
        successor.predecessor_admission_sha256 = Some(self.admission_sha256.clone());
        successor.executable_handoff_manifest_sha256 = Some(manifest_sha256);
        successor.executable_handoff_sha256 = Some(handoff_sha256);
        successor.executable_sha256 = successor_executable_sha256;
        successor.admission_sha256 = successor.expected_admission_sha256()?;
        successor.validate()?;
        Ok(successor)
    }

    pub(super) fn matches_non_executable_context(
        &self,
        capability_binding: &CapabilityBinding,
        instrument: &Instrument,
        minimum_quantity: Decimal,
        now_ms: u64,
    ) -> Result<bool, Stage7GridError> {
        self.validate()?;
        Ok(self.valid_until_ms > now_ms
            && self.capability_binding == *capability_binding
            && self.instrument_rules.instrument == *instrument
            && self.instrument_rules.minimum_quantity == minimum_quantity)
    }

    pub(super) fn matches_current(
        &self,
        capability_binding: &CapabilityBinding,
        instrument: &Instrument,
        minimum_quantity: Decimal,
        executable_sha256: &str,
        now_ms: u64,
    ) -> Result<bool, Stage7GridError> {
        Ok(self.matches_non_executable_context(
            capability_binding,
            instrument,
            minimum_quantity,
            now_ms,
        )? && self.executable_sha256 == executable_sha256)
    }

    pub(super) fn matches_current_exposure(
        &self,
        capability_binding: &CapabilityBinding,
        instrument: &Instrument,
        minimum_quantity: Decimal,
        executable_sha256: &str,
        exposure_take_profit_sha256: &Option<String>,
        now_ms: u64,
    ) -> Result<bool, Stage7GridError> {
        Ok(self.matches_current(
            capability_binding,
            instrument,
            minimum_quantity,
            executable_sha256,
            now_ms,
        )? && self.exposure_release_bound
            && self.exposure_take_profit_sha256 == *exposure_take_profit_sha256)
    }

    pub(super) fn configuration_sha256_with_exposure(
        &self,
        exposure_take_profit_sha256: &Option<String>,
    ) -> Result<String, Stage7GridError> {
        configuration_digest(
            &self.capability_binding,
            &self.deployment_binding,
            self.parameter_release.grid_count,
            &self.selection_rule,
            true,
            exposure_take_profit_sha256,
        )
    }
}

fn stage7_context_matches(
    capability_binding: &CapabilityBinding,
    deployment_binding: &HedgedGridBinding,
    parameter_release: &HedgedGridParams,
    instrument: &Instrument,
) -> bool {
    expected_deployment_binding_for(
        capability_binding,
        &deployment_binding.config_version,
        &deployment_binding.account,
    )
    .is_ok_and(|expected| expected == *deployment_binding)
        && stage7_release_matches(deployment_binding, parameter_release)
        && instrument.symbol == deployment_binding.symbol
        && instrument.market == crate::domain::MarketKind::LinearPerpetual
        && instrument
            .settlement_asset
            .as_ref()
            .is_some_and(|asset| asset.as_str() == deployment_binding.symbol.quote())
        && parameter_release.order_notional.asset.as_str() == deployment_binding.symbol.quote()
}

fn stage7_release_matches(
    deployment_binding: &HedgedGridBinding,
    parameter_release: &HedgedGridParams,
) -> bool {
    deployment_binding
        .symbol
        .quote()
        .parse()
        .ok()
        .and_then(|asset| HedgedGridParams::fixed_release(asset, parameter_release.grid_count).ok())
        .is_some_and(|expected| expected == *parameter_release)
}

fn selection_rule_for(
    deployment_binding: &HedgedGridBinding,
    parameter_release: &HedgedGridParams,
) -> &'static str {
    if deployment_binding.symbol.quote() == "USDT" && parameter_release.grid_count == 3 {
        STAGE7_SELECTION_RULE
    } else {
        STAGE7_CONFIGURED_SELECTION_RULE
    }
}

#[cfg(test)]
pub(super) fn expected_deployment_binding(
    capability_binding: &CapabilityBinding,
) -> Result<HedgedGridBinding, Stage7GridError> {
    expected_deployment_binding_for(
        capability_binding,
        "stage7",
        "00000000-0000-4000-8000-000000000001",
    )
}

fn expected_deployment_binding_for(
    capability_binding: &CapabilityBinding,
    config_version: &str,
    trading_account_id: &str,
) -> Result<HedgedGridBinding, Stage7GridError> {
    capability_binding.validate()?;
    match (
        capability_binding.exchange.as_str(),
        capability_binding.account_binding.as_str(),
    ) {
        ("gate", "usdt_futures_dual")
        | ("bitget", "uta_usdt_futures_hedge")
        | ("binance", "portfolio_margin_um") => {}
        _ => return Err(Stage7GridError::GridCanaryEvidence),
    }
    if !crate::domain::is_canonical_trading_account_id(trading_account_id) {
        return Err(Stage7GridError::GridCanaryEvidence);
    }
    if capability_binding.symbol.split_once('/').is_none() {
        return Err(Stage7GridError::GridCanaryEvidence);
    }
    let symbol: crate::domain::Symbol = capability_binding
        .symbol
        .parse()
        .map_err(|_| Stage7GridError::GridCanaryEvidence)?;
    let strategy_instance_id = format!(
        "hedged_grid_{}_{}",
        symbol.base().to_ascii_lowercase(),
        symbol.quote().to_ascii_lowercase()
    );
    let binding = HedgedGridBinding {
        owner_scope: format!("{strategy_instance_id}_primary"),
        strategy_instance_id,
        run_id: "primary".to_owned(),
        exchange: capability_binding.exchange.clone(),
        account: trading_account_id.to_owned(),
        symbol,
        config_version: config_version.to_owned(),
    };
    binding.validate()?;
    Ok(binding)
}

#[cfg(test)]
pub(super) fn stage7_parameter_release() -> Result<HedgedGridParams, Stage7GridError> {
    HedgedGridParams::fixed_release(
        Asset::new("USDT").map_err(|_| Stage7GridError::GridCanaryEvidence)?,
        3,
    )
    .map_err(Stage7GridError::Strategy)
}

fn configuration_digest(
    capability_binding: &CapabilityBinding,
    deployment_binding: &HedgedGridBinding,
    grid_count: u8,
    selection_rule: &str,
    exposure_release_bound: bool,
    exposure_take_profit_sha256: &Option<String>,
) -> Result<String, Stage7GridError> {
    canonical_digest(&Stage7ConfigurationDigest {
        release_id: STAGE7_RELEASE_ID,
        selection_rule,
        capability_binding,
        deployment_binding,
        grid_count,
        single_order_max_notional: SINGLE_ORDER_MAX_NOTIONAL,
        replenishment_max_notional: REPLENISHMENT_MAX_NOTIONAL,
        exposure_release_bound,
        exposure_take_profit_sha256,
    })
}

pub(super) fn is_false(value: &bool) -> bool {
    !*value
}

pub(super) fn exposure_release_digest(
    config: Option<ExposureTakeProfitConfig>,
) -> Result<Option<String>, Stage7GridError> {
    config
        .map(|config| {
            config.validate().map_err(|_| Stage7GridError::GridConfig)?;
            canonical_digest(&Stage7ExposureReleaseDigest {
                enabled: config.enabled,
                shadow: config.shadow,
                position_equity_multiple: config.position_equity_multiple,
                unrealized_pnl_equity_ratio: config.unrealized_pnl_equity_ratio,
                reduce_ratio: config.reduce_ratio,
                snapshot_interval_ms: config.snapshot_interval_ms,
                max_snapshot_age_ms: config.max_snapshot_age_ms,
                rearm_clear_generations: config.rearm_clear_generations,
            })
        })
        .transpose()
}

pub(super) fn canonical_digest(value: &impl Serialize) -> Result<String, Stage7GridError> {
    Ok(sha256_hex(
        serde_json::to_vec(value).map_err(CapabilityEvidenceError::Encode)?,
    ))
}

pub(super) fn executable_sha256() -> Result<String, Stage7GridError> {
    let path = std::env::current_exe().map_err(|_| Stage7GridError::GridCanaryEvidence)?;
    let bytes = fs::read(path).map_err(|_| Stage7GridError::GridCanaryEvidence)?;
    Ok(sha256_hex(bytes))
}

pub(super) fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn basic_canary_hashes(
    binding: &CapabilityBinding,
    deployment_binding: &HedgedGridBinding,
    params: &HedgedGridParams,
    executable_sha256: &str,
    capabilities: &[Capability],
) -> Result<Vec<String>, Stage7GridError> {
    let binding_matches = expected_deployment_binding_for(
        binding,
        &deployment_binding.config_version,
        &deployment_binding.account,
    )
    .is_ok_and(|expected| expected == *deployment_binding);
    if !binding_matches || !stage7_release_matches(deployment_binding, params) {
        return Err(Stage7GridError::GridCanaryEvidence);
    }
    let context = sha256_hex(
        [
            executable_sha256.as_bytes(),
            configuration_digest(
                binding,
                deployment_binding,
                params.grid_count,
                selection_rule_for(deployment_binding, params),
                false,
                &None,
            )?
            .as_bytes(),
            canonical_digest(params)?.as_bytes(),
        ]
        .concat(),
    );
    Ok(capabilities
        .iter()
        .map(|capability| sha256_hex(format!("{context}:{capability:?}")))
        .collect())
}

/// `Decimal::is_sign_positive` is true for zero, so presence must be expressed explicitly.
/// This is a Canary safety boundary: a confirmed flat leg must not consume the full retry window
/// and make a later, independent hedge-side test look like an exchange failure.
pub(super) fn position_presence_matches(quantity: Decimal, present: bool) -> bool {
    (!quantity.is_zero()) == present
}

pub(super) fn canary_readback<V: Stage7CanaryVenue>(
    venue: &mut V,
    public_market: &mut Stage7PublicRuntime,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
) -> Result<(GridVenueReadback, GridInventory, Price, Price), Stage7GridError> {
    let public_ready = wait_for_canary_public_ready(
        CANARY_PUBLIC_WARMUP_ATTEMPTS,
        || public_market.drive(venue, wall_clock_ms()?),
        || thread::sleep(Duration::from_millis(CANARY_PUBLIC_WARMUP_INTERVAL_MS)),
    )?;
    if !public_ready {
        return Err(Stage7GridError::PublicMarket);
    }
    // The public capture may have drained a busy socket.  Timestamp private evidence and consume
    // its quote only after that fsynced boundary has completed.
    let now_ms = wall_clock_ms()?;
    let readback = venue.readback()?;
    require_complete_order_family_readback(&readback)?;
    *generation = generation.checked_add(1).ok_or(Stage7GridError::Clock)?;
    for payload in &readback.raw_private_payloads {
        append_private_payload(evidence, *generation, now_ms, payload.clone())?;
    }
    let (bid, ask) = venue.best_bid_ask(now_ms)?;
    let inventory = inventory(&readback, *generation, now_ms, bid, ask, &binding.symbol)?;
    if !readback.hedge_position
        || !stage7_balance_asset_matches_binding(binding, readback.balance.asset.as_str())
    {
        return Err(Stage7GridError::Inventory);
    }
    Ok((readback, inventory, bid, ask))
}

pub(super) fn stage7_balance_asset_matches_binding(
    binding: &HedgedGridBinding,
    balance_asset: &str,
) -> bool {
    balance_asset == binding.symbol.quote()
        || (binding.exchange == "binance"
            && binding.symbol.quote() == "USDC"
            && balance_asset == "USDT")
}

#[cfg(test)]
mod balance_asset_tests {
    use super::*;

    fn binding(
        exchange: &str,
        account: &str,
        symbol: &str,
    ) -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
        Ok(HedgedGridBinding {
            strategy_instance_id: "grid".to_owned(),
            run_id: "primary".to_owned(),
            exchange: exchange.to_owned(),
            account: account.to_owned(),
            symbol: symbol.parse()?,
            config_version: "shared-grid-v1".to_owned(),
            owner_scope: "grid_primary".to_owned(),
        })
    }

    #[test]
    fn portfolio_margin_usdc_accepts_the_normalized_account_risk_asset_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let portfolio = binding("binance", "portfolio_margin_um", "SOL/USDC")?;
        assert!(stage7_balance_asset_matches_binding(&portfolio, "USDC"));
        assert!(stage7_balance_asset_matches_binding(&portfolio, "USDT"));
        assert!(!stage7_balance_asset_matches_binding(&portfolio, "BTC"));

        let ordinary = binding("gate", "usdt_futures", "DOGE/USDT")?;
        assert!(stage7_balance_asset_matches_binding(&ordinary, "USDT"));
        assert!(!stage7_balance_asset_matches_binding(&ordinary, "USDC"));
        Ok(())
    }
}

fn wait_for_canary_public_ready<Drive, Pause>(
    attempts: u8,
    mut drive: Drive,
    mut pause: Pause,
) -> Result<bool, Stage7GridError>
where
    Drive: FnMut() -> Result<bool, Stage7GridError>,
    Pause: FnMut(),
{
    for attempt in 0..attempts {
        if drive()? {
            return Ok(true);
        }
        if attempt + 1 < attempts {
            pause();
        }
    }
    Ok(false)
}

pub(super) fn canary_preflight(
    readback: &GridVenueReadback,
    inventory: &GridInventory,
) -> Result<(), Stage7GridError> {
    if !readback
        .all_order_families_empty()
        .map_err(|_| Stage7GridError::OrderFamily)?
        || !inventory.long_quantity.is_zero()
        || !inventory.short_quantity.is_zero()
    {
        return Err(Stage7GridError::Canary);
    }
    if readback.balance.available_balance <= Decimal::ZERO {
        return Err(Stage7GridError::InsufficientMargin);
    }
    Ok(())
}

pub(super) fn canary_quantity<V: Stage7CanaryVenue>(
    venue: &V,
    bid: Price,
    ask: Price,
    minimum_price: Price,
) -> Result<Decimal, Stage7GridError> {
    let reference = (bid.value() + ask.value()) / Decimal::TWO;
    stage7_quantity(venue, Decimal::new(5, 0), reference, minimum_price)
}

pub(super) fn canary_owner(binding: &HedgedGridBinding, purpose: OrderPurpose) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: binding.strategy_instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        purpose,
    }
}

pub(super) fn command_id(value: String) -> Result<CommandId, Stage7GridError> {
    CommandId::new(value).map_err(|_| Stage7GridError::Command)
}

pub(super) fn wait_for_canary_position<V: Stage7CanaryVenue>(
    venue: &mut V,
    public_market: &mut Stage7PublicRuntime,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
    side: PositionSide,
    present: bool,
) -> Result<(GridVenueReadback, GridInventory, Price, Price), Stage7GridError> {
    for _ in 0..CANARY_POSITION_WAIT_ATTEMPTS {
        let output = match canary_readback(venue, public_market, evidence, generation, binding) {
            Ok(output) => output,
            // An immediately accepted exchange mutation can briefly precede its complete
            // account/order/fill projection.  Keep the probe closed to further mutation while
            // boundedly retrying a fresh, signed view rather than treating that short gap as a
            // reason to abandon the remaining hedge verification.
            Err(Stage7GridError::PublicMarket | Stage7GridError::Venue { .. }) => {
                thread::sleep(Duration::from_millis(CANARY_POSITION_WAIT_INTERVAL_MS));
                continue;
            }
            Err(error) => return Err(error),
        };
        let quantity = match side {
            PositionSide::Long => output.1.long_quantity,
            PositionSide::Short => output.1.short_quantity,
            PositionSide::Net => return Err(Stage7GridError::Canary),
        };
        // Gate can report an IOC's filled position before its open-order projection has caught
        // up.  Do not start the next Canary leg until the same signed readback is clean on both
        // surfaces; otherwise a valid short leg can be mistaken for an unsafe overlap.
        if position_presence_matches(quantity, present) && output.0.orders.is_empty() {
            return Ok(output);
        }
        thread::sleep(Duration::from_millis(CANARY_POSITION_WAIT_INTERVAL_MS));
    }
    Err(Stage7GridError::Canary)
}

/// Emergency cleanup never needs a public quote to cancel owned orders or reduce an observed
/// hedge leg.  It remains entirely signed-private so a public-feed outage cannot turn a bounded
/// exposure into a stranded one.
pub(super) fn canary_cleanup_readback<V: Stage7CanaryVenue>(
    venue: &mut V,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
) -> Result<(GridVenueReadback, GridInventory), Stage7GridError> {
    let readback = venue.readback()?;
    require_complete_order_family_readback(&readback)?;
    *generation = generation.checked_add(1).ok_or(Stage7GridError::Clock)?;
    let now_ms = wall_clock_ms()?;
    for payload in &readback.raw_private_payloads {
        append_private_payload(evidence, *generation, now_ms, payload.clone())?;
    }
    if !readback.hedge_position
        || !stage7_balance_asset_matches_binding(binding, readback.balance.asset.as_str())
    {
        return Err(Stage7GridError::Inventory);
    }
    let long = position_or_flat(&readback.positions, PositionSide::Long, &binding.symbol)?;
    let short = position_or_flat(&readback.positions, PositionSide::Short, &binding.symbol)?;
    let flat_mark = Price::new(Decimal::ONE).map_err(|_| Stage7GridError::Inventory)?;
    let mark_price = match (long.mark_price, short.mark_price) {
        (Some(long_mark), Some(short_mark)) if long_mark == short_mark => long_mark,
        (Some(mark), None) if short.quantity.is_zero() => mark,
        (None, Some(mark)) if long.quantity.is_zero() => mark,
        (None, None) if long.quantity.is_zero() && short.quantity.is_zero() => flat_mark,
        _ => return Err(Stage7GridError::Inventory),
    };
    Ok((
        readback,
        GridInventory {
            private_generation: *generation,
            private_observed_at_ms: now_ms,
            mark_price,
            long_quantity: long.quantity,
            short_quantity: short.quantity,
        },
    ))
}

pub(super) fn wait_for_canary_cleanup_position<V: Stage7CanaryVenue>(
    venue: &mut V,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
    side: PositionSide,
    present: bool,
) -> Result<(GridVenueReadback, GridInventory), Stage7GridError> {
    for _ in 0..CANARY_POSITION_WAIT_ATTEMPTS {
        let output = canary_cleanup_readback(venue, evidence, generation, binding)?;
        let quantity = match side {
            PositionSide::Long => output.1.long_quantity,
            PositionSide::Short => output.1.short_quantity,
            PositionSide::Net => return Err(Stage7GridError::Canary),
        };
        if position_presence_matches(quantity, present) {
            return Ok(output);
        }
        thread::sleep(Duration::from_millis(CANARY_POSITION_WAIT_INTERVAL_MS));
    }
    Err(Stage7GridError::Canary)
}

pub(super) fn reduce_canary_market<V: Stage7CanaryVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    command: crate::domain::MarketReduceCommand,
) -> Result<(), Stage7GridError> {
    commands.prepare_market_reduce(command.clone())?;
    if let Err(error) = venue.validate_client_order_id(command.client_order_id.as_str()) {
        commands.transition(
            &command.command_id,
            CommandState::Rejected {
                reason: error.to_string(),
            },
        )?;
        return Err(Stage7GridError::Rejected);
    }
    commands.transition(&command.command_id, CommandState::Submitted)?;
    let _guard = authority.persistent_dispatch_guard(writer)?;
    match venue.place_market_reduce(&command) {
        Ok(venue_order_id) => {
            commands.transition(
                &command.command_id,
                CommandState::Accepted { venue_order_id },
            )?;
            Ok(())
        }
        Err(error) if is_rejected(&error) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: error.to_string(),
                },
            )?;
            Err(Stage7GridError::Rejected)
        }
        Err(error) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: error.to_string(),
                },
            )?;
            Err(Stage7GridError::Unresolved)
        }
    }
}

pub(super) fn append_stage7_canary_capabilities(
    binding: &CapabilityBinding,
    deployment_binding: &HedgedGridBinding,
    params: &HedgedGridParams,
    artifacts_root: &Path,
    _generation: u64,
    valid_until_ms: u64,
) -> Result<(), Stage7GridError> {
    let now_ms = wall_clock_ms()?;
    let capabilities = [
        Capability::InstrumentRules,
        Capability::PublicMarket,
        Capability::PrivateReadback,
        Capability::PrivateStream,
        Capability::PlaceLimit,
        Capability::Cancel,
        Capability::ReduceOnly,
        Capability::Reconciliation,
    ];
    let hashes = basic_canary_hashes(
        binding,
        deployment_binding,
        params,
        &executable_sha256()?,
        &capabilities,
    )?;
    let probes = capabilities
        .into_iter()
        .zip(hashes)
        .map(|(capability, evidence_hash)| {
            CapabilityProbe::new(
                capability,
                "stage7_place_cancel_hedge_reduce_canary_v2",
                evidence_hash,
                valid_until_ms,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    CapabilityEvidenceStore::open(artifacts_root.join(CAPABILITY_EVIDENCE_FILE))?
        .append_successes(binding, now_ms, &probes)?;
    Ok(())
}

pub(super) fn require_stage7_canary<V: Stage7CanaryVenue>(
    venue: &V,
    deployment_binding: &HedgedGridBinding,
    params: &HedgedGridParams,
    artifacts_root: &Path,
    now_ms: u64,
) -> Result<(), Stage7GridError> {
    let capability_binding = venue.capability_binding();
    let executable = executable_sha256()?;
    let (_, predecessor) = super::stage7_executable_handoff::validated_admission_predecessor(
        &capability_binding,
        venue.instrument(),
        venue.minimum_quantity(),
        artifacts_root,
        now_ms,
        &executable,
    )?;
    let current = CapabilityEvidenceStore::open(artifacts_root.join(CAPABILITY_EVIDENCE_FILE))?
        .current(&capability_binding, now_ms)?;
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
        &capability_binding,
        deployment_binding,
        params,
        &predecessor.executable_sha256,
        &required,
    )?;
    if required
        .into_iter()
        .zip(expected)
        .all(|(capability, expected_hash)| {
            current
                .get(&capability)
                .is_some_and(|evidence| evidence.evidence_hash == expected_hash)
        })
    {
        Ok(())
    } else {
        Err(Stage7GridError::CanaryEvidence)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the lifecycle receipt commits all release and evidence identities atomically"
)]
pub(super) fn append_stage7_grid_lifecycle_capability(
    capability_binding: &CapabilityBinding,
    deployment_binding: &HedgedGridBinding,
    parameter_release: &HedgedGridParams,
    instrument: &Instrument,
    minimum_quantity: Decimal,
    exposure_take_profit: Option<ExposureTakeProfitConfig>,
    artifacts_root: &Path,
    private_generation: u64,
    health_generation: u64,
    valid_until_ms: u64,
) -> Result<(), Stage7GridError> {
    let now_ms = wall_clock_ms()?;
    let admission = Stage7LiveAdmissionEvidence::new_with_exposure(
        capability_binding.clone(),
        deployment_binding.clone(),
        parameter_release.clone(),
        instrument.clone(),
        minimum_quantity,
        exposure_take_profit,
        executable_sha256()?,
        now_ms,
        valid_until_ms,
        private_generation,
        health_generation,
    )?;
    // Persist the exact context first. A crash before the matching capability append leaves the
    // old capability hash unable to authorize this new context; the two-file boundary is closed.
    ProjectionStore::new(artifacts_root.join(STAGE7_LIVE_ADMISSION_FILE)).save(&admission)?;
    let probe = CapabilityProbe::new(
        Capability::GridLifecycle,
        "stage7_configured_grid_replenish_fill_restart_flat_canary_v3",
        admission.admission_sha256,
        valid_until_ms,
    )?;
    CapabilityEvidenceStore::open(artifacts_root.join(CAPABILITY_EVIDENCE_FILE))?
        .append_successes(capability_binding, now_ms, &[probe])?;
    Ok(())
}

pub(super) fn require_stage7_grid_lifecycle<V: Stage7CanaryVenue>(
    venue: &V,
    deployment_binding: &HedgedGridBinding,
    parameter_release: &HedgedGridParams,
    exposure_take_profit: Option<ExposureTakeProfitConfig>,
    artifacts_root: &Path,
    now_ms: u64,
) -> Result<(), Stage7GridError> {
    let capability_binding = venue.capability_binding();
    require_stage7_grid_lifecycle_context(
        &capability_binding,
        deployment_binding,
        parameter_release,
        venue.instrument(),
        venue.minimum_quantity(),
        &exposure_release_digest(exposure_take_profit)?,
        artifacts_root,
        now_ms,
        &executable_sha256()?,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "the admission check deliberately compares every persisted lifecycle identity"
)]
fn require_stage7_grid_lifecycle_context(
    capability_binding: &CapabilityBinding,
    deployment_binding: &HedgedGridBinding,
    parameter_release: &HedgedGridParams,
    instrument: &Instrument,
    minimum_quantity: Decimal,
    exposure_take_profit_sha256: &Option<String>,
    artifacts_root: &Path,
    now_ms: u64,
    executable_sha256: &str,
) -> Result<(), Stage7GridError> {
    let current = CapabilityEvidenceStore::open(artifacts_root.join(CAPABILITY_EVIDENCE_FILE))?
        .current(capability_binding, now_ms)?;
    let (admission, predecessor) =
        super::stage7_executable_handoff::validated_admission_predecessor(
            capability_binding,
            instrument,
            minimum_quantity,
            artifacts_root,
            now_ms,
            executable_sha256,
        )?;
    let lifecycle = current.get(&Capability::GridLifecycle);
    if admission.matches_current_exposure(
        capability_binding,
        instrument,
        minimum_quantity,
        executable_sha256,
        exposure_take_profit_sha256,
        now_ms,
    )? && admission.deployment_binding == *deployment_binding
        && admission.parameter_release == *parameter_release
        && lifecycle.is_some_and(|evidence| {
            evidence.evidence_hash == predecessor.admission_sha256
                && evidence.verified_at_ms == predecessor.verified_at_ms
                && evidence.valid_until_ms == predecessor.valid_until_ms
        })
    {
        Ok(())
    } else {
        Err(Stage7GridError::GridCanaryEvidence)
    }
}

#[cfg(test)]
mod admission_tests {
    use crate::config::ExposureTakeProfitConfig;
    use crate::domain::{Amount, MarketKind, Price};

    use super::*;

    #[test]
    fn canary_public_warmup_retries_until_the_snapshot_bridge_is_ready()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut turns = 0_u8;
        let mut pauses = 0_u8;
        let ready = wait_for_canary_public_ready(
            3,
            || {
                turns = turns.saturating_add(1);
                Ok(turns == 2)
            },
            || pauses = pauses.saturating_add(1),
        )?;
        assert!(ready);
        assert_eq!(turns, 2);
        assert_eq!(pauses, 1);

        let mut bounded_turns = 0_u8;
        assert!(!wait_for_canary_public_ready(
            2,
            || {
                bounded_turns = bounded_turns.saturating_add(1);
                Ok(false)
            },
            || {},
        )?);
        assert_eq!(bounded_turns, 2);
        Ok(())
    }

    fn capability_binding() -> CapabilityBinding {
        CapabilityBinding {
            exchange: "gate".to_owned(),
            account_binding: "usdt_futures_dual".to_owned(),
            symbol: "DOGE/USDT".to_owned(),
            api_key_sha256: "a".repeat(64),
        }
    }

    fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
        Ok(Instrument {
            symbol: "DOGE/USDT".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(Asset::new("USDT")?),
            generation: 7,
            price_tick: Price::new(Decimal::new(1, 5))?,
            quantity_step: Decimal::new(10, 0),
            minimum_notional: Amount::new(Asset::new("USDT")?, Decimal::ZERO),
        })
    }

    fn exposure(shadow: bool) -> ExposureTakeProfitConfig {
        ExposureTakeProfitConfig {
            enabled: true,
            shadow,
            position_equity_multiple: Decimal::new(3, 0),
            unrealized_pnl_equity_ratio: Decimal::new(5, 2),
            reduce_ratio: Decimal::new(30, 2),
            snapshot_interval_ms: 120_000,
            max_snapshot_age_ms: 3_000,
            rearm_clear_generations: 2,
        }
    }

    fn persist_admission(
        root: &Path,
        executable: &str,
    ) -> Result<Stage7LiveAdmissionEvidence, Box<dyn std::error::Error>> {
        let capability_binding = capability_binding();
        let evidence = Stage7LiveAdmissionEvidence::new(
            capability_binding.clone(),
            expected_deployment_binding(&capability_binding)?,
            stage7_parameter_release()?,
            instrument()?,
            Decimal::new(10, 0),
            executable.to_owned(),
            10,
            100,
            11,
            12,
        )?;
        ProjectionStore::new(root.join(STAGE7_LIVE_ADMISSION_FILE)).save(&evidence)?;
        CapabilityEvidenceStore::open(root.join(CAPABILITY_EVIDENCE_FILE))?.append_successes(
            &capability_binding,
            evidence.verified_at_ms,
            &[CapabilityProbe::new(
                Capability::GridLifecycle,
                "stage7_three_by_three_replenish_fill_restart_flat_canary_v2",
                evidence.admission_sha256.clone(),
                evidence.valid_until_ms,
            )?],
        )?;
        Ok(evidence)
    }

    #[test]
    fn live_admission_persists_exact_non_secret_release_and_rules_watermarks()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let executable = "b".repeat(64);
        let evidence = persist_admission(temporary.path(), &executable)?;
        let deployment = expected_deployment_binding(&capability_binding())?;
        let params = stage7_parameter_release()?;

        require_stage7_grid_lifecycle_context(
            &capability_binding(),
            &deployment,
            &params,
            &instrument()?,
            Decimal::new(10, 0),
            &None,
            temporary.path(),
            20,
            &executable,
        )?;
        assert_eq!(evidence.parameter_release.grid_count, 3);
        assert_eq!(evidence.instrument_rules.instrument.generation, 7);
        assert_eq!(evidence.selection_rule, STAGE7_SELECTION_RULE);
        assert!(valid_sha256(&evidence.executable_sha256));
        assert!(valid_sha256(&evidence.configuration_sha256));
        assert!(valid_sha256(&evidence.parameter_release_sha256));
        assert!(valid_sha256(&evidence.instrument_rules_sha256));
        assert!(valid_sha256(&evidence.admission_sha256));
        let persisted = fs::read_to_string(temporary.path().join(STAGE7_LIVE_ADMISSION_FILE))?;
        assert!(!persisted.contains("api_secret"));
        assert!(!persisted.contains("passphrase"));
        Ok(())
    }

    #[test]
    fn changing_shadow_invalidates_the_existing_live_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let capability = capability_binding();
        let executable = "b".repeat(64);
        let evidence = Stage7LiveAdmissionEvidence::new_with_exposure(
            capability.clone(),
            expected_deployment_binding(&capability)?,
            stage7_parameter_release()?,
            instrument()?,
            Decimal::new(10, 0),
            Some(exposure(true)),
            executable.clone(),
            10,
            100,
            11,
            12,
        )?;
        ProjectionStore::new(temporary.path().join(STAGE7_LIVE_ADMISSION_FILE)).save(&evidence)?;
        CapabilityEvidenceStore::open(temporary.path().join(CAPABILITY_EVIDENCE_FILE))?
            .append_successes(
                &capability,
                10,
                &[CapabilityProbe::new(
                    Capability::GridLifecycle,
                    "risk_release_bound_canary",
                    evidence.admission_sha256,
                    100,
                )?],
            )?;

        let changed = exposure_release_digest(Some(exposure(false)))?;
        assert!(matches!(
            require_stage7_grid_lifecycle_context(
                &capability,
                &expected_deployment_binding(&capability)?,
                &stage7_parameter_release()?,
                &instrument()?,
                Decimal::new(10, 0),
                &changed,
                temporary.path(),
                20,
                &executable,
            ),
            Err(Stage7GridError::GridCanaryEvidence)
        ));
        Ok(())
    }

    #[test]
    fn executable_api_binding_and_instrument_rule_changes_require_a_new_canary()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let executable = "b".repeat(64);
        let _ = persist_admission(temporary.path(), &executable)?;
        let deployment = expected_deployment_binding(&capability_binding())?;
        let params = stage7_parameter_release()?;

        assert!(matches!(
            require_stage7_grid_lifecycle_context(
                &capability_binding(),
                &deployment,
                &params,
                &instrument()?,
                Decimal::new(10, 0),
                &None,
                temporary.path(),
                20,
                &"c".repeat(64),
            ),
            Err(Stage7GridError::GridCanaryEvidence)
        ));

        let mut changed_api = capability_binding();
        changed_api.api_key_sha256 = "d".repeat(64);
        assert!(matches!(
            require_stage7_grid_lifecycle_context(
                &changed_api,
                &deployment,
                &params,
                &instrument()?,
                Decimal::new(10, 0),
                &None,
                temporary.path(),
                20,
                &executable,
            ),
            Err(Stage7GridError::GridCanaryEvidence)
        ));

        let mut changed_rules = instrument()?;
        changed_rules.price_tick = Price::new(Decimal::new(2, 5))?;
        changed_rules.generation = 8;
        assert!(matches!(
            require_stage7_grid_lifecycle_context(
                &capability_binding(),
                &deployment,
                &params,
                &changed_rules,
                Decimal::new(10, 0),
                &None,
                temporary.path(),
                20,
                &executable,
            ),
            Err(Stage7GridError::GridCanaryEvidence)
        ));
        Ok(())
    }

    #[test]
    fn parameter_or_configuration_tampering_cannot_reuse_lifecycle_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let executable = "b".repeat(64);
        let mut evidence = persist_admission(temporary.path(), &executable)?;
        let deployment = expected_deployment_binding(&capability_binding())?;
        let params = stage7_parameter_release()?;
        evidence.parameter_release.grid_count = 4;
        evidence.parameter_release_sha256 = canonical_digest(&evidence.parameter_release)?;
        evidence.admission_sha256 = evidence.expected_admission_sha256()?;
        ProjectionStore::new(temporary.path().join(STAGE7_LIVE_ADMISSION_FILE)).save(&evidence)?;

        assert!(matches!(
            require_stage7_grid_lifecycle_context(
                &capability_binding(),
                &deployment,
                &params,
                &instrument()?,
                Decimal::new(10, 0),
                &None,
                temporary.path(),
                20,
                &executable,
            ),
            Err(Stage7GridError::GridCanaryEvidence)
        ));
        Ok(())
    }

    #[test]
    fn basic_canary_hashes_are_bound_to_the_executable_and_api_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let capability = [Capability::PlaceLimit];
        let deployment = expected_deployment_binding(&capability_binding())?;
        let params = stage7_parameter_release()?;
        let original = basic_canary_hashes(
            &capability_binding(),
            &deployment,
            &params,
            &"b".repeat(64),
            &capability,
        )?;
        let changed_executable = basic_canary_hashes(
            &capability_binding(),
            &deployment,
            &params,
            &"c".repeat(64),
            &capability,
        )?;
        let mut changed_api = capability_binding();
        changed_api.api_key_sha256 = "d".repeat(64);
        let changed_api = basic_canary_hashes(
            &changed_api,
            &deployment,
            &params,
            &"b".repeat(64),
            &capability,
        )?;

        assert_ne!(original, changed_executable);
        assert_ne!(original, changed_api);
        Ok(())
    }

    #[test]
    fn admission_is_self_describing_for_binance_usdc_ten_levels()
    -> Result<(), Box<dyn std::error::Error>> {
        let capability = CapabilityBinding {
            exchange: "binance".to_owned(),
            account_binding: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDC".to_owned(),
            api_key_sha256: "a".repeat(64),
        };
        let deployment = expected_deployment_binding_for(
            &capability,
            "shared-grid-v1",
            "00000000-0000-4000-8000-000000000001",
        )?;
        let params = HedgedGridParams::fixed_release(Asset::new("USDC")?, 10)?;
        let instrument = Instrument {
            symbol: "SOL/USDC".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(Asset::new("USDC")?),
            generation: 9,
            price_tick: Price::new(Decimal::new(1, 3))?,
            quantity_step: Decimal::new(1, 2),
            minimum_notional: Amount::new(Asset::new("USDC")?, Decimal::ZERO),
        };
        let evidence = Stage7LiveAdmissionEvidence::new(
            capability,
            deployment,
            params,
            instrument,
            Decimal::new(1, 2),
            "b".repeat(64),
            10,
            100,
            11,
            12,
        )?;
        assert_eq!(evidence.parameter_release.grid_count, 10);
        assert_eq!(
            evidence.parameter_release.order_notional.asset.as_str(),
            "USDC"
        );
        assert_eq!(evidence.selection_rule, STAGE7_CONFIGURED_SELECTION_RULE);
        assert_eq!(
            evidence
                .instrument_rules
                .instrument
                .settlement_asset
                .as_ref()
                .map(Asset::as_str),
            Some("USDC")
        );
        evidence.validate()?;
        Ok(())
    }
}
