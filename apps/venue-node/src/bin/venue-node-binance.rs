use std::process::ExitCode;

use clap::Parser;
use venue_gateway_api::{GatewayMode, VenueId};
use venue_gateway_binance::BinanceConfig;
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, reject_unintegrated_legacy_test_runtime, report_result,
};

const PROGRAM: &str = "venue-node-binance";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Binance)?;
    let arguments = launch.legacy_runtime_arguments(PROGRAM)?;
    let cli = venue::Cli::try_parse_from(arguments)?;
    let config =
        venue::config::Config::load(&cli.config).map_err(|error| NodeError::ExistingRuntime {
            venue: VenueId::Binance,
            message: error.to_string(),
        })?;
    launch.validate_runtime_scope(&config.trading_account_id, &config.symbol)?;
    let account_binding = config
        .binance_config()
        .map_err(|error| NodeError::ExistingRuntime {
            venue: VenueId::Binance,
            message: error.to_string(),
        })?;
    let adapter = BinanceConfig::for_binding(account_binding.account_binding, launch.binding())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Binance))?;
    AdapterIsolation {
        venue: VenueId::Binance,
        mode: adapter.mode(),
        endpoints: &[
            adapter.portfolio_rest_origin(),
            adapter.usd_m_public_rest_origin(),
            adapter.public_stream_origin(),
            adapter.private_stream_origin(),
        ],
        credential_environment: &["BINANCE_API_KEY", "BINANCE_API_SECRET"],
        credential_prefix: "BINANCE_",
        account_binding: adapter.account_binding().as_str(),
    }
    .validate(launch.binding())?;
    if launch.binding().mode == GatewayMode::Test {
        return reject_unintegrated_legacy_test_runtime(VenueId::Binance);
    }
    venue::start_hedged_grid_binance_deployment(cli).map_err(|error| NodeError::ExistingRuntime {
        venue: VenueId::Binance,
        message: error.to_string(),
    })
}

/// Candidate-only physical bridge. The active binary continues to delegate LIVE to Stage 7; this
/// module therefore cannot acquire a writer or grant capability by construction.
#[allow(dead_code)]
mod candidate_bridge {
    use std::collections::BTreeSet;

    use venue_domain::domain::{ExecutionCommand, NativeOrderFamily, Symbol};
    use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding};
    use venue_gateway_binance::{
        BinanceConfig, BinanceInstrumentRules, BinancePhysicalMutationOutcome,
        BinancePreparedMutation, BinancePrivateReadbackCandidate, BinanceTransportError,
        prepare_execution_command, settle_mutation_ack,
    };
    use venue_node::{
        CommandReadbackKey, DispatchPermit, FamilyReadbackCoverage, GatewayAcknowledgement,
        GatewayDispatchResult, GatewayRecoveryPermit, PhysicalGateway, ReadbackCommandState,
        SignedCommandReadback, SignedReadbackReceipt, SignedReadbackRequest,
    };

    const REGULAR_REJECTED: &str = "binance_adapter_preflight_rejected";
    const VENUE_REJECTED: &str = "binance_http_client_rejected";

    pub trait BinanceCandidateBackend {
        fn connect(&mut self, request: BinanceRecoveryScope) -> Result<(), BinanceBridgeError>;

        fn signed_readback(
            &mut self,
            request: &SignedReadbackRequest,
        ) -> Result<BinanceCandidateReadback, BinanceBridgeError>;

        fn dispatch_once(
            &mut self,
            request: BinancePreparedMutation,
        ) -> BinancePhysicalMutationOutcome;
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BinanceRecoveryScope {
        pub binding: GatewayBinding,
        pub config_epoch: u64,
        pub unresolved_commands: usize,
        pub predecessor_writer_generation: Option<u64>,
        pub connection_generation_floor: u64,
        pub private_generation_floor: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BinanceExactCommandEvidence {
        pub key: CommandReadbackKey,
        pub state: ReadbackCommandState,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BinanceCandidateReadback {
        pub connection_generation: u64,
        pub observed_ms: u64,
        pub evidence_digest: [u8; 32],
        pub private: BinancePrivateReadbackCandidate,
        pub command_results: Vec<BinanceExactCommandEvidence>,
    }

    pub struct BinancePhysicalGatewayCandidate<B> {
        binding: GatewayBinding,
        config: BinanceConfig,
        rules: BinanceInstrumentRules,
        backend: B,
        latest_private: Option<BinancePrivateReadbackCandidate>,
        issued_readbacks: Vec<SignedReadbackReceipt>,
    }

    impl<B> BinancePhysicalGatewayCandidate<B> {
        pub fn new(
            config: BinanceConfig,
            rules: BinanceInstrumentRules,
            backend: B,
        ) -> Result<Self, BinanceBridgeError> {
            let binding = config.gateway_binding().clone();
            if rules.instrument.generation == 0
                || rules.instrument.symbol != binding.symbol
                || rules.native_symbol != venue_gateway_binance::native_symbol(&binding.symbol)
            {
                return Err(BinanceBridgeError::Binding);
            }
            Ok(Self {
                binding,
                config,
                rules,
                backend,
                latest_private: None,
                issued_readbacks: Vec::new(),
            })
        }

        fn convert_readback(
            &mut self,
            expected_binding: &GatewayBinding,
            after_connection_generation: u64,
            after_private_generation: u64,
            expected_commands: &[CommandReadbackKey],
            candidate: BinanceCandidateReadback,
        ) -> Result<SignedReadbackReceipt, BinanceBridgeError> {
            let private_generation = candidate.private.scope().private_generation();
            if expected_binding != &self.binding
                || candidate.private.scope().binding() != &self.binding
                || candidate.private.scope().instrument_generation()
                    != self.rules.instrument.generation
                || candidate.connection_generation < after_connection_generation
                || private_generation <= after_private_generation
                || candidate.observed_ms == 0
                || candidate.evidence_digest == [0; 32]
            {
                return Err(BinanceBridgeError::ReadbackScope);
            }

            // The current host request exposes UNKNOWN identities but not the complete durable
            // Owner map. Treating a native client ID as ownership would violate recovery custody.
            if !candidate.private.regular().orders.is_empty()
                || !candidate.private.algo().orders.is_empty()
            {
                return Err(BinanceBridgeError::OwnerEvidenceUnavailable);
            }

            let expected_keys = expected_commands.iter().collect::<BTreeSet<_>>();
            let actual_keys = candidate
                .command_results
                .iter()
                .map(|result| &result.key)
                .collect::<BTreeSet<_>>();
            if expected_keys != actual_keys || actual_keys.len() != candidate.command_results.len()
            {
                return Err(BinanceBridgeError::CommandEvidence);
            }
            let command_results = candidate
                .command_results
                .into_iter()
                .map(|result| SignedCommandReadback::new(result.key, result.state))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| BinanceBridgeError::CommandEvidence)?;
            let nonzero_position_symbols = candidate
                .private
                .positions()
                .iter()
                .filter(|position| !position.quantity.is_zero())
                .map(|position| position.symbol.clone())
                .collect::<BTreeSet<Symbol>>();
            let receipt = SignedReadbackReceipt::new(
                self.binding.clone(),
                candidate.connection_generation,
                private_generation,
                candidate.observed_ms,
                digest_hex(candidate.evidence_digest),
                vec![
                    FamilyReadbackCoverage::complete(NativeOrderFamily::UmOrder),
                    FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmConditional),
                    FamilyReadbackCoverage::complete(NativeOrderFamily::UmAlgo),
                ],
                Vec::new(),
                nonzero_position_symbols,
                command_results,
            )
            .map_err(|_| BinanceBridgeError::CommandEvidence)?;
            self.latest_private = Some(candidate.private);
            self.issued_readbacks.push(receipt.clone());
            Ok(receipt)
        }

        fn prepare_command(
            &self,
            command: &ExecutionCommand,
            readback_generation: u64,
        ) -> Result<BinancePreparedMutation, BinanceBridgeError> {
            let private = self
                .latest_private
                .as_ref()
                .ok_or(BinanceBridgeError::Recovery)?;
            if private.scope().private_generation() != readback_generation {
                return Err(BinanceBridgeError::ReadbackScope);
            }
            prepare_execution_command(&self.rules, private, command)
                .map_err(|_| BinanceBridgeError::Command)
        }
    }

    impl<B: BinanceCandidateBackend> PhysicalGateway for BinancePhysicalGatewayCandidate<B> {
        type Error = BinanceBridgeError;

        fn binding(&self) -> &GatewayBinding {
            &self.binding
        }

        fn capability_snapshot(&self) -> CapabilitySnapshot {
            CapabilitySnapshot {
                binding: self.binding.clone(),
                version: 0,
                observed_ms: 0,
                expires_ms: 0,
                flags: CapabilityFlags::empty(),
            }
        }

        fn connect_after_recovery(
            &mut self,
            permit: GatewayRecoveryPermit,
        ) -> Result<(), Self::Error> {
            if permit.binding() != &self.binding || permit.config_epoch() == 0 {
                return Err(BinanceBridgeError::Recovery);
            }
            self.backend.connect(BinanceRecoveryScope {
                binding: permit.binding().clone(),
                config_epoch: permit.config_epoch(),
                unresolved_commands: permit.unresolved_commands(),
                predecessor_writer_generation: permit.predecessor_writer_generation(),
                connection_generation_floor: permit.connection_generation_floor(),
                private_generation_floor: permit.private_generation_floor(),
            })
        }

        fn signed_readback(
            &mut self,
            request: &SignedReadbackRequest,
        ) -> Result<SignedReadbackReceipt, Self::Error> {
            let candidate = self.backend.signed_readback(request)?;
            self.convert_readback(
                request.binding(),
                request.after_connection_generation(),
                request.after_private_generation(),
                request.commands(),
                candidate,
            )
        }

        fn verify_signed_readback(
            &self,
            receipt: &SignedReadbackReceipt,
        ) -> Result<(), Self::Error> {
            if self.issued_readbacks.contains(receipt) {
                Ok(())
            } else {
                Err(BinanceBridgeError::ReadbackSignature)
            }
        }

        fn dispatch(&mut self, permit: DispatchPermit) -> GatewayDispatchResult {
            if permit.binding() != &self.binding {
                return GatewayDispatchResult::Rejected {
                    reason_code: REGULAR_REJECTED.to_owned(),
                };
            }
            let request = match self.prepare_command(permit.command(), permit.readback_generation())
            {
                Ok(request) => request,
                Err(_) => {
                    return GatewayDispatchResult::Rejected {
                        reason_code: REGULAR_REJECTED.to_owned(),
                    };
                }
            };
            classify_outcome(self.backend.dispatch_once(request))
        }
    }

    fn classify_outcome(outcome: BinancePhysicalMutationOutcome) -> GatewayDispatchResult {
        match outcome {
            BinancePhysicalMutationOutcome::ReadBack { ack, readback } => {
                let venue_order_id = ack.order_id.clone();
                if settle_mutation_ack(&ack, *readback).is_err() {
                    return GatewayDispatchResult::Unknown;
                }
                match GatewayAcknowledgement::new(venue_order_id) {
                    Ok(acknowledgement) => GatewayDispatchResult::Acknowledged(acknowledgement),
                    Err(_) => GatewayDispatchResult::Unknown,
                }
            }
            BinancePhysicalMutationOutcome::AckedReadbackUnknown { .. }
            | BinancePhysicalMutationOutcome::DispatchUnknown { .. } => {
                GatewayDispatchResult::Unknown
            }
            BinancePhysicalMutationOutcome::DispatchFailed {
                error: BinanceTransportError::HttpStatus(status),
            } if (400..500).contains(&status) => GatewayDispatchResult::Rejected {
                reason_code: VENUE_REJECTED.to_owned(),
            },
            BinancePhysicalMutationOutcome::DispatchFailed { .. } => GatewayDispatchResult::Unknown,
        }
    }

    fn digest_hex(digest: [u8; 32]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in digest {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
    pub enum BinanceBridgeError {
        #[error("Binance candidate bridge binding or instrument scope is invalid")]
        Binding,
        #[error("Binance candidate bridge recovery proof is invalid")]
        Recovery,
        #[error("Binance candidate readback scope or generation is invalid")]
        ReadbackScope,
        #[error("Binance candidate readback has no durable Owner evidence")]
        OwnerEvidenceUnavailable,
        #[error("Binance candidate exact command evidence is incomplete")]
        CommandEvidence,
        #[error("Binance candidate readback was not issued by this bridge")]
        ReadbackSignature,
        #[error("canonical command is not representable by the closed Binance adapter")]
        Command,
    }

    #[cfg(test)]
    mod tests {
        use rust_decimal::Decimal;
        use venue_domain::domain::{
            CancelCommand, CommandId, ExecutionCommand, MarketReduceCommand, NativeOrderFamily,
            OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
        };
        use venue_gateway_api::{GatewayMode, MutationCapability, VenueId};
        use venue_gateway_binance::private::RecentFillsCursor;
        use venue_gateway_binance::{
            BinanceAccountBinding, BinanceMutationAck, BinanceMutationKind,
            BinancePrivateReadRequest, BinancePrivateReadScope, BinanceRawPrivatePage,
            BinanceTimeInForce, build_account_config_request, build_account_request,
            build_algo_orders_request, build_fills_request, build_position_mode_request,
            build_positions_request, build_regular_orders_request, complete_private_readback,
            parse_exact_order_readback, parse_instrument_rules, prepare_place_limit,
        };

        use super::*;

        const ACCOUNT: &[u8] = include_bytes!(
            "../../../../crates/venue-gateway-binance/fixtures/portfolio-account.json"
        );
        const ACCOUNT_CONFIG: &[u8] =
            include_bytes!("../../../../crates/venue-gateway-binance/fixtures/account-config.json");
        const POSITION_MODE: &[u8] = include_bytes!(
            "../../../../crates/venue-gateway-binance/fixtures/position-mode-hedge.json"
        );
        const POSITIONS: &[u8] = include_bytes!(
            "../../../../crates/venue-gateway-binance/fixtures/positions-hedge-long-only.json"
        );
        const FILLS: &[u8] = include_bytes!(
            "../../../../crates/venue-gateway-binance/fixtures/user-trades-page.json"
        );
        const EXACT: &[u8] = include_bytes!(
            "../../../../crates/venue-gateway-binance/fixtures/exact-order-readback.json"
        );
        const EXCHANGE_INFO: &str = include_str!(
            "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        );

        struct NoBackend;

        impl BinanceCandidateBackend for NoBackend {
            fn connect(&mut self, _: BinanceRecoveryScope) -> Result<(), BinanceBridgeError> {
                Err(BinanceBridgeError::Recovery)
            }

            fn signed_readback(
                &mut self,
                _: &SignedReadbackRequest,
            ) -> Result<BinanceCandidateReadback, BinanceBridgeError> {
                Err(BinanceBridgeError::Recovery)
            }

            fn dispatch_once(
                &mut self,
                _: BinancePreparedMutation,
            ) -> BinancePhysicalMutationOutcome {
                BinancePhysicalMutationOutcome::DispatchUnknown {
                    error: BinanceTransportError::Disconnected,
                }
            }
        }

        fn fixture(
            mode: GatewayMode,
            net: bool,
            nonempty_regular: bool,
        ) -> Result<
            (
                BinanceConfig,
                BinanceInstrumentRules,
                BinancePrivateReadbackCandidate,
            ),
            Box<dyn std::error::Error>,
        > {
            let binding = GatewayBinding::new(
                VenueId::Binance,
                mode,
                "00000000-0000-4000-8000-000000000001",
                "BTC/USDT".parse()?,
            )?;
            let config =
                BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
            let rules = parse_instrument_rules(EXCHANGE_INFO, binding.symbol.clone(), 7)?;
            let scope = BinancePrivateReadScope::new(&config, &rules, 17, 11, 900)?;
            let position_mode: &'static [u8] = if net {
                br#"{"dualSidePosition":false}"#
            } else {
                POSITION_MODE
            };
            let positions: &'static [u8] = if net {
                br#"[{"symbol":"BTCUSDT","positionAmt":"-0.010","positionSide":"BOTH","entryPrice":"50000","markPrice":"49000"}]"#
            } else {
                POSITIONS
            };
            let regular: &'static [u8] = if nonempty_regular {
                include_bytes!("../../../../crates/venue-gateway-binance/fixtures/open-orders.json")
            } else {
                br#"[]"#
            };
            let pages = vec![
                page(build_account_request(&scope)?, ACCOUNT)?,
                page(build_account_config_request(&scope)?, ACCOUNT_CONFIG)?,
                page(build_position_mode_request(&scope)?, position_mode)?,
                page(build_positions_request(&scope)?, positions)?,
                page(build_regular_orders_request(&scope)?, regular)?,
                page(build_algo_orders_request(&scope)?, br#"[]"#)?,
                page(
                    build_fills_request(
                        &scope,
                        1,
                        RecentFillsCursor {
                            observed_through_ms: 1_000,
                            last_trade_id: None,
                            last_event_time_ms: None,
                        },
                        1_000,
                        2_000,
                    )?,
                    FILLS,
                )?,
            ];
            let readback = complete_private_readback(
                &config,
                &rules,
                &scope,
                RecentFillsCursor {
                    observed_through_ms: 1_000,
                    last_trade_id: None,
                    last_event_time_ms: None,
                },
                2_000,
                pages,
            )?;
            Ok((config, rules, readback))
        }

        fn page(
            request: BinancePrivateReadRequest,
            payload: &'static [u8],
        ) -> Result<BinanceRawPrivatePage, Box<dyn std::error::Error>> {
            Ok(BinanceRawPrivatePage::new(&request, 1_000, 2_000, payload)?)
        }

        fn owner(purpose: OrderPurpose) -> Result<OrderOwner, Box<dyn std::error::Error>> {
            Ok(OrderOwner {
                strategy_instance_id: "grid_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "portfolio_margin_um".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose,
            })
        }

        #[test]
        fn test_and_live_remain_exact_and_candidate_capability_is_empty()
        -> Result<(), Box<dyn std::error::Error>> {
            let (test_config, rules, _) = fixture(GatewayMode::Test, false, false)?;
            let bridge = BinancePhysicalGatewayCandidate::new(test_config, rules, NoBackend)?;
            assert_eq!(bridge.config.mode(), GatewayMode::Test);
            assert!(
                bridge
                    .config
                    .portfolio_rest_origin()
                    .contains("testnet.binancefuture.com")
            );
            assert!(
                !bridge
                    .config
                    .portfolio_rest_origin()
                    .contains("papi.binance.com")
            );
            let capability = bridge.capability_snapshot();
            assert!(capability.flags.is_empty());
            assert_eq!(
                capability.authorize(bridge.binding(), 1, 1_000, MutationCapability::PlaceLimit,),
                Err(venue_gateway_api::GatewayApiError::CapabilityScope)
            );

            let (live, _, _) = fixture(GatewayMode::Live, false, false)?;
            assert_eq!(live.portfolio_rest_origin(), "https://papi.binance.com");
            Ok(())
        }

        #[test]
        fn readback_converts_net_and_hedge_with_exact_three_family_coverage()
        -> Result<(), Box<dyn std::error::Error>> {
            for net in [false, true] {
                let (config, rules, readback) = fixture(GatewayMode::Test, net, false)?;
                let binding = config.gateway_binding().clone();
                let mut bridge = BinancePhysicalGatewayCandidate::new(config, rules, NoBackend)?;
                let receipt = bridge.convert_readback(
                    &binding,
                    1,
                    16,
                    &[],
                    BinanceCandidateReadback {
                        connection_generation: 2,
                        observed_ms: 2_000,
                        evidence_digest: readback.raw_payload_digest(),
                        private: readback,
                        command_results: Vec::new(),
                    },
                )?;
                assert_eq!(receipt.family_coverage().len(), 3);
                assert_eq!(
                    receipt
                        .family_coverage()
                        .iter()
                        .map(|entry| (entry.family(), entry.supported()))
                        .collect::<Vec<_>>(),
                    vec![
                        (NativeOrderFamily::UmOrder, true),
                        (NativeOrderFamily::UmConditional, false),
                        (NativeOrderFamily::UmAlgo, true),
                    ]
                );
                assert!(receipt.nonzero_position_symbols().contains(&binding.symbol));
                bridge.verify_signed_readback(&receipt)?;
            }
            Ok(())
        }

        #[test]
        fn nonempty_native_orders_fail_without_durable_owner_map()
        -> Result<(), Box<dyn std::error::Error>> {
            let (config, rules, readback) = fixture(GatewayMode::Test, false, true)?;
            let binding = config.gateway_binding().clone();
            let mut bridge = BinancePhysicalGatewayCandidate::new(config, rules, NoBackend)?;
            assert_eq!(
                bridge.convert_readback(
                    &binding,
                    1,
                    16,
                    &[],
                    BinanceCandidateReadback {
                        connection_generation: 2,
                        observed_ms: 2_000,
                        evidence_digest: readback.raw_payload_digest(),
                        private: readback,
                        command_results: Vec::new(),
                    },
                ),
                Err(BinanceBridgeError::OwnerEvidenceUnavailable)
            );
            Ok(())
        }

        #[test]
        fn shared_commands_translate_only_place_cancel_and_reduce_once()
        -> Result<(), Box<dyn std::error::Error>> {
            let (_, rules, readback) = fixture(GatewayMode::Test, false, false)?;
            let place = ExecutionCommand::PlaceLimit(OrderCommand {
                command_id: CommandId::new("place_1")?,
                client_order_id: CommandId::new("venue_place_1")?,
                owner: owner(OrderPurpose::Entry)?,
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quantity: Decimal::new(2, 3),
                limit_price: Price::new(Decimal::new(50_000, 0))?,
                reduce_only: false,
            });
            assert_eq!(
                prepare_execution_command(&rules, &readback, &place)?.kind(),
                BinanceMutationKind::PlaceLimit
            );

            let cancel = ExecutionCommand::Cancel(CancelCommand {
                command_id: CommandId::new("cancel_1")?,
                owner: owner(OrderPurpose::Entry)?,
                target_client_order_id: CommandId::new("venue_place_1")?,
            });
            assert_eq!(
                prepare_execution_command(&rules, &readback, &cancel)?.kind(),
                BinanceMutationKind::Cancel
            );

            let reduce = ExecutionCommand::MarketReduce(MarketReduceCommand {
                command_id: CommandId::new("reduce_1")?,
                client_order_id: CommandId::new("venue_reduce_1")?,
                owner: owner(OrderPurpose::ExposureTakeProfit)?,
                position_side: PositionSide::Long,
                side: OrderSide::Sell,
                quantity: Decimal::new(5, 3),
                risk_episode_id: CommandId::new("episode_1")?,
                position_generation: 17,
            });
            assert_eq!(
                prepare_execution_command(&rules, &readback, &reduce)?.kind(),
                BinanceMutationKind::ReduceOnce
            );
            Ok(())
        }

        #[test]
        fn net_adapter_is_closed_but_shared_command_model_cannot_express_it()
        -> Result<(), Box<dyn std::error::Error>> {
            let (_, rules, readback) = fixture(GatewayMode::Test, true, false)?;
            let direct = venue_gateway_binance::BinancePlaceIntent {
                client_order_id: "venue_net_1".to_owned(),
                side: OrderSide::Buy,
                position_side: PositionSide::Net,
                quantity: Decimal::new(2, 3),
                limit_price: Price::new(Decimal::new(50_000, 0))?,
                time_in_force: BinanceTimeInForce::PostOnly,
                reduce_only: false,
            };
            assert!(prepare_place_limit(&rules, &readback, &direct).is_ok());

            let shared = ExecutionCommand::PlaceLimit(OrderCommand {
                command_id: CommandId::new("net_place_1")?,
                client_order_id: CommandId::new("venue_net_1")?,
                owner: owner(OrderPurpose::Entry)?,
                side: OrderSide::Buy,
                position_side: PositionSide::Net,
                quantity: Decimal::new(2, 3),
                limit_price: Price::new(Decimal::new(50_000, 0))?,
                reduce_only: false,
            });
            assert!(prepare_execution_command(&rules, &readback, &shared).is_err());
            Ok(())
        }

        #[test]
        fn acknowledgement_requires_exact_signed_readback_or_stays_unknown()
        -> Result<(), Box<dyn std::error::Error>> {
            let (config, rules, readback) = fixture(GatewayMode::Test, false, false)?;
            let prepared = prepare_place_limit(
                &rules,
                &readback,
                &venue_gateway_binance::BinancePlaceIntent {
                    client_order_id: "venue_place_1".to_owned(),
                    side: OrderSide::Buy,
                    position_side: PositionSide::Long,
                    quantity: Decimal::new(2, 3),
                    limit_price: Price::new(Decimal::new(50_000, 0))?,
                    time_in_force: BinanceTimeInForce::PostOnly,
                    reduce_only: false,
                },
            )?;
            let ack = BinanceMutationAck {
                binding: config.gateway_binding().clone(),
                instrument_generation: 7,
                private_generation: 17,
                kind: BinanceMutationKind::PlaceLimit,
                order_id: "401".to_owned(),
                client_order_id: "venue_place_1".to_owned(),
                accepted_at_ms: 1_000,
                received_at_ms: 1_100,
            };
            let exact_request = prepared.exact_readback_request(readback.scope())?;
            let exact_page = BinanceRawPrivatePage::new(&exact_request, 1_200, 1_300, EXACT)?;
            let exact = parse_exact_order_readback(&ack, &exact_request, &exact_page)?;
            let accepted = classify_outcome(BinancePhysicalMutationOutcome::ReadBack {
                ack: ack.clone(),
                readback: Box::new(exact),
            });
            assert!(matches!(
                accepted,
                GatewayDispatchResult::Acknowledged(ref acknowledgement)
                    if acknowledgement.venue_order_id() == "401"
            ));
            assert_eq!(
                classify_outcome(BinancePhysicalMutationOutcome::AckedReadbackUnknown {
                    ack,
                    error: BinanceTransportError::Disconnected,
                }),
                GatewayDispatchResult::Unknown
            );
            assert_eq!(
                classify_outcome(BinancePhysicalMutationOutcome::DispatchUnknown {
                    error: BinanceTransportError::Timeout,
                }),
                GatewayDispatchResult::Unknown
            );
            Ok(())
        }
    }
}
