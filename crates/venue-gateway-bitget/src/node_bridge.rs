//! Fail-closed bridge from the generic account-node command shape to reviewed Bitget primitives.
//!
//! This module does not implement a writer, WAL, capability probe, or runtime. It only binds an
//! already validated five-face turn to the regular-only execution profile and translates one
//! host-owned semantic command into one non-cloneable adapter request.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};
use venue_domain::domain::{CommandId, ExecutionCommand, NativeOrderFamily, PositionSide};

use crate::{
    BitgetCancelIntent, BitgetConfig, BitgetExecutionError, BitgetMutationKind,
    BitgetOrderFamilyCandidate, BitgetOrderFamilyError, BitgetOrderFamilyEvidence,
    BitgetOrderFamilyScope, BitgetPlaceIntent, BitgetPreparedMutation, BitgetReduceOnceIntent,
    BitgetTimeInForce, instrument::BitgetInstrumentRules, prepare_cancel_request,
    prepare_place_request, prepare_reduce_once_request, private::BitgetPrivateGenerationCandidate,
    validate_order_families,
};

/// A replay-checked node readback candidate. `attempt_id` is the monotonically increasing private
/// turn, while `generation` is the authenticated private connection generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetNodeReadbackCandidate {
    families: BitgetOrderFamilyCandidate,
    commitment_sha256: String,
}

impl BitgetNodeReadbackCandidate {
    pub fn validate<I>(
        scope: BitgetOrderFamilyScope,
        rules: &BitgetInstrumentRules,
        validated_at_ms: u64,
        evidence: I,
    ) -> Result<Self, BitgetNodeBridgeError>
    where
        I: IntoIterator<Item = BitgetOrderFamilyEvidence>,
    {
        let families = validate_order_families(scope, rules, validated_at_ms, evidence)?;
        let commitment_sha256 = commitment(&families)?;
        Ok(Self {
            families,
            commitment_sha256,
        })
    }

    #[must_use]
    pub const fn families(&self) -> &BitgetOrderFamilyCandidate {
        &self.families
    }

    #[must_use]
    pub const fn private(&self) -> &BitgetPrivateGenerationCandidate {
        self.families.private()
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.families.scope().generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.families.scope().attempt_id
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &str {
        self.commitment_sha256.as_str()
    }

    #[must_use]
    pub const fn supports(&self, family: NativeOrderFamily) -> bool {
        matches!(family, NativeOrderFamily::UmOrder)
    }
}

/// The command identity remains beside the consumed native request for host-side UNKNOWN routing.
#[derive(Debug, Eq, PartialEq)]
pub struct BitgetNodePreparedMutation {
    command_id: CommandId,
    mutation: BitgetPreparedMutation,
}

impl BitgetNodePreparedMutation {
    #[must_use]
    pub const fn command_id(&self) -> &CommandId {
        &self.command_id
    }

    #[must_use]
    pub const fn kind(&self) -> BitgetMutationKind {
        self.mutation.kind
    }

    #[must_use]
    pub fn client_order_id(&self) -> Option<&str> {
        self.mutation.client_order_id()
    }

    #[must_use]
    pub fn into_mutation(self) -> BitgetPreparedMutation {
        self.mutation
    }
}

/// Converts only the three reviewed normal-family operations. Conditional/Algo commands and
/// exposure-increasing market orders remain explicitly unsupported.
pub fn prepare_node_mutation(
    candidate: &BitgetNodeReadbackCandidate,
    rules: &BitgetInstrumentRules,
    config: &BitgetConfig,
    command: &ExecutionCommand,
    mutation_attempt_id: u64,
    now_ms: u64,
) -> Result<BitgetNodePreparedMutation, BitgetNodeBridgeError> {
    command
        .validate()
        .map_err(|_| BitgetNodeBridgeError::Command)?;
    validate_command_scope(candidate, rules, config, command)?;
    let mutation = match command {
        ExecutionCommand::PlaceLimit(command) => prepare_place_request(
            &candidate.private().binding,
            config,
            rules,
            mutation_attempt_id,
            &BitgetPlaceIntent {
                client_order_id: command.client_order_id.as_str().to_owned(),
                side: command.side,
                position_side: command.position_side,
                quantity: command.quantity,
                limit_price: command.limit_price,
                time_in_force: BitgetTimeInForce::from_limit_time_in_force(command.time_in_force),
                reduce_only: command.reduce_only,
            },
            now_ms,
        )?,
        ExecutionCommand::Cancel(command) => prepare_cancel_request(
            &candidate.private().binding,
            config,
            candidate.connection_generation(),
            mutation_attempt_id,
            &BitgetCancelIntent {
                order_id: None,
                client_order_id: Some(command.target_client_order_id.as_str().to_owned()),
            },
        )?,
        ExecutionCommand::MarketReduce(command) => {
            let position = candidate
                .private()
                .positions
                .iter()
                .find(|position| position.side == command.position_side)
                .ok_or(BitgetNodeBridgeError::Position)?;
            prepare_reduce_once_request(
                &candidate.private().binding,
                config,
                rules,
                mutation_attempt_id,
                &BitgetReduceOnceIntent {
                    client_order_id: command.client_order_id.as_str().to_owned(),
                    position_side: command.position_side,
                    quantity: command.quantity,
                },
                position,
                now_ms,
            )?
        }
        ExecutionCommand::PlaceMarket(_)
        | ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_) => {
            return Err(BitgetNodeBridgeError::UnsupportedCommand);
        }
    };
    Ok(BitgetNodePreparedMutation {
        command_id: command.command_id().clone(),
        mutation,
    })
}

fn validate_command_scope(
    candidate: &BitgetNodeReadbackCandidate,
    rules: &BitgetInstrumentRules,
    config: &BitgetConfig,
    command: &ExecutionCommand,
) -> Result<(), BitgetNodeBridgeError> {
    let binding = &candidate.private().binding;
    let owner = command.mutation_owner();
    if binding.mode != config.mode()
        || binding != &rules.raw.binding
        || binding.symbol != owner.symbol
        || binding.trading_account_id != owner.account
        || owner.exchange != binding.venue.as_str()
        || candidate.connection_generation() != rules.snapshot.metadata.instrument.generation
    {
        return Err(BitgetNodeBridgeError::Scope);
    }
    if matches!(command, ExecutionCommand::MarketReduce(command) if command.position_side == PositionSide::Net)
    {
        return Err(BitgetNodeBridgeError::Position);
    }
    Ok(())
}

fn commitment(candidate: &BitgetOrderFamilyCandidate) -> Result<String, BitgetNodeBridgeError> {
    let mut digest = Sha256::new();
    let scope = candidate.scope();
    digest
        .update(serde_json::to_vec(&scope.binding).map_err(|_| BitgetNodeBridgeError::Commitment)?);
    for value in [
        scope.profile_version,
        scope.attempt_id,
        scope.generation,
        scope.observed_at_ms,
        scope.expires_at_ms,
    ] {
        digest.update(value.to_be_bytes());
    }
    digest.update(candidate.regular_payload_digest());
    digest.update(candidate.conditional().profile_version.to_be_bytes());
    digest.update(candidate.algo().profile_version.to_be_bytes());
    for raw in &candidate.private().raw_pages {
        digest.update([raw.surface as u8]);
        digest.update(raw.page_index.to_be_bytes());
        digest.update(raw.received_at_ms.to_be_bytes());
        digest.update(raw.payload_sha256.as_bytes());
        digest.update(raw.payload.as_bytes());
    }
    let bytes: [u8; 32] = digest.finalize().into();
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").map_err(|_| BitgetNodeBridgeError::Commitment)?;
    }
    Ok(encoded)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetNodeBridgeError {
    #[error("Bitget node readback does not close all canonical order families")]
    OrderFamilies(#[from] BitgetOrderFamilyError),
    #[error("Bitget node command is invalid")]
    Command,
    #[error("Bitget node command is outside the fixed binding")]
    Scope,
    #[error("Bitget node command is outside the regular-only execution profile")]
    UnsupportedCommand,
    #[error("Bitget node reduce-once command has no matching signed Hedge leg")]
    Position,
    #[error("Bitget node mutation could not be normalized")]
    Execution(#[from] BitgetExecutionError),
    #[error("Bitget node readback commitment could not be encoded")]
    Commitment,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde_json::json;
    use venue_domain::domain::{
        CancelCommand, ExecutionCommand, MarketReduceCommand, OrderCommand, OrderOwner,
        OrderPurpose, OrderSide, PositionSide, Price,
    };
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    use super::*;
    use crate::{
        BITGET_ORDER_PROFILE_VERSION, BitgetOrderFamilyEvidence, BitgetUnsupportedEvidence,
        instrument::{BitgetRawInstrumentPayload, parse_instrument_rules},
        private::{
            BitgetPrivateFace, BitgetPrivateSurface, BitgetRawPrivatePage, complete_private_turn,
            parse_account_face, parse_fill_page, parse_positions_face, parse_regular_order_page,
            parse_settings_face,
        },
    };

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    fn rules(mode: GatewayMode) -> Result<BitgetInstrumentRules, Box<dyn std::error::Error>> {
        let raw = BitgetRawInstrumentPayload::new(
            binding(mode)?,
            7,
            50,
            1_000,
            include_str!("../tests/fixtures/bitget_uta_btcusdt_instrument.json").to_owned(),
        )?;
        Ok(parse_instrument_rules(raw, 60)?)
    }

    fn raw(
        mode: GatewayMode,
        surface: BitgetPrivateSurface,
        data: serde_json::Value,
    ) -> Result<BitgetRawPrivatePage, Box<dyn std::error::Error>> {
        Ok(BitgetRawPrivatePage::new_with_generation(
            surface,
            binding(mode)?,
            9,
            7,
            0,
            None,
            (surface == BitgetPrivateSurface::Fills).then_some(10),
            100,
            json!({"code":"00000", "data":data}).to_string(),
        )?)
    }

    fn node_candidate(
        mode: GatewayMode,
    ) -> Result<(BitgetInstrumentRules, BitgetNodeReadbackCandidate), Box<dyn std::error::Error>>
    {
        let private = complete_private_turn(vec![
            BitgetPrivateFace::Account(parse_account_face(raw(
                mode,
                BitgetPrivateSurface::Account,
                json!({
                    "imr":"0", "mmr":"0",
                    "assets":[{"coin":"USDT", "balance":"20", "available":"20"}]
                }),
            )?)?),
            BitgetPrivateFace::Settings(parse_settings_face(raw(
                mode,
                BitgetPrivateSurface::Settings,
                json!({"holdMode":"hedge_mode"}),
            )?)?),
            BitgetPrivateFace::Positions(parse_positions_face(raw(
                mode,
                BitgetPrivateSurface::Positions,
                json!({"list":[{
                    "symbol":"BTCUSDT", "marginCoin":"USDT", "holdMode":"hedge_mode",
                    "posSide":"long", "total":"2", "avgPrice":"100", "markPrice":"101"
                }]}),
            )?)?),
            BitgetPrivateFace::RegularOrders(vec![parse_regular_order_page(raw(
                mode,
                BitgetPrivateSurface::RegularOrders,
                json!({"list":[], "cursor":null}),
            )?)?]),
            BitgetPrivateFace::Fills(vec![parse_fill_page(raw(
                mode,
                BitgetPrivateSurface::Fills,
                json!({"list":[], "cursor":null}),
            )?)?]),
        ])?;
        let rules = rules(mode)?;
        let candidate = BitgetNodeReadbackCandidate::validate(
            BitgetOrderFamilyScope {
                binding: binding(mode)?,
                profile_version: BITGET_ORDER_PROFILE_VERSION,
                attempt_id: 9,
                generation: 7,
                observed_at_ms: 100,
                expires_at_ms: 900,
            },
            &rules,
            200,
            [
                BitgetOrderFamilyEvidence::Regular(Box::new(private)),
                BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::conditional(
                    BITGET_ORDER_PROFILE_VERSION,
                )),
                BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::algo(
                    BITGET_ORDER_PROFILE_VERSION,
                )),
            ],
        )?;
        Ok((rules, candidate))
    }

    fn owner(purpose: OrderPurpose) -> Result<OrderOwner, Box<dyn std::error::Error>> {
        Ok(OrderOwner {
            strategy_instance_id: "grid_1".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "bitget".to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            purpose,
        })
    }

    #[test]
    fn node_candidate_keeps_connection_and_private_turn_generations_distinct()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, candidate) = node_candidate(GatewayMode::Live)?;
        assert_eq!(candidate.private().raw_pages.len(), 5);
        assert_eq!(candidate.connection_generation(), 7);
        assert_eq!(candidate.private_generation(), 9);
        assert!(candidate.supports(NativeOrderFamily::UmOrder));
        assert!(!candidate.supports(NativeOrderFamily::UmConditional));
        assert!(!candidate.supports(NativeOrderFamily::UmAlgo));
        assert_eq!(candidate.commitment_sha256().len(), 64);
        Ok(())
    }

    #[test]
    fn node_bridge_emits_only_post_only_place_exact_cancel_and_reduce_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let (rules, candidate) = node_candidate(GatewayMode::Live)?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let place = ExecutionCommand::PlaceLimit(OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new("place_1")?,
            client_order_id: CommandId::new("venue_place_1")?,
            owner: owner(OrderPurpose::Entry)?,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 3),
            limit_price: Price::new(Decimal::from(100_000))?,
            reduce_only: false,
        });
        let place = prepare_node_mutation(&candidate, &rules, &config, &place, 11, 200)?;
        assert_eq!(place.kind(), BitgetMutationKind::Place);
        assert!(
            String::from_utf8(place.mutation.body.clone())?
                .contains("\"timeInForce\":\"post_only\"")
        );

        let cancel = ExecutionCommand::Cancel(CancelCommand {
            command_id: CommandId::new("cancel_1")?,
            owner: owner(OrderPurpose::Entry)?,
            target_client_order_id: CommandId::new("venue_place_1")?,
        });
        let cancel = prepare_node_mutation(&candidate, &rules, &config, &cancel, 12, 200)?;
        assert_eq!(cancel.kind(), BitgetMutationKind::Cancel);
        assert_eq!(cancel.client_order_id(), Some("venue_place_1"));

        let reduce = ExecutionCommand::MarketReduce(MarketReduceCommand {
            command_id: CommandId::new("reduce_1")?,
            client_order_id: CommandId::new("venue_reduce_1")?,
            owner: owner(OrderPurpose::ExposureTakeProfit)?,
            position_side: PositionSide::Long,
            side: OrderSide::Sell,
            quantity: Decimal::ONE,
            risk_episode_id: CommandId::new("risk_1")?,
            position_generation: 9,
        });
        let reduce = prepare_node_mutation(&candidate, &rules, &config, &reduce, 13, 200)?;
        assert_eq!(reduce.kind(), BitgetMutationKind::ReduceOnce);
        let body = String::from_utf8(reduce.mutation.body.clone())?;
        assert!(body.contains("\"orderType\":\"market\""));
        assert!(body.contains("\"posSide\":\"long\""));
        Ok(())
    }
}
