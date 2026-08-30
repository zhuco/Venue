use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use sha2::{Digest, Sha256};
use venue_domain::domain::{CommandId, FieldState, NativeOrderFamily, OrderOwner, OrderState};
use venue_gateway_api::{CapabilitySnapshot, GatewayBinding, GatewayMode, VenueId};
use venue_gateway_bitget::instrument::BitgetInstrumentRules;
#[cfg(test)]
use venue_gateway_bitget::private::{BitgetPrivateGenerationCandidate, BitgetPrivateSurface};
use venue_gateway_bitget::{
    BitgetConfig, BitgetExactOrderReadback, BitgetExactReadbackRequest, BitgetMutationAck,
    BitgetMutationOutcome, BitgetNodePreparedMutation, BitgetNodeReadbackCandidate,
    BitgetPreparedMutation, BitgetReadbackFinality, BitgetUnknownMutation, BitgetUnknownReason,
    build_ack_readback_request, build_unknown_recovery_readback_request, capabilities,
    prepare_node_mutation, settle_ack_readback, settle_unknown_readback,
};
use venue_node::{
    AdapterIsolation, DispatchPermit, FamilyReadbackCoverage, GatewayAcknowledgement,
    GatewayDispatchResult, GatewayRecoveryPermit, NodeError, NodeLaunch, PhysicalGateway,
    ReadbackCommandState, SignedCommandReadback, SignedOwnedOrder, SignedReadbackReceipt,
    SignedReadbackRequest, reject_unintegrated_legacy_test_runtime, report_result,
};
#[cfg(test)]
use venue_runtime::account::{
    PhysicalReadbackReceipt, PhysicalReadbackSurface, PhysicalRecoveryManifestError,
};
use venue_runtime::account::{PhysicalRecoveryReadbackManifest, PhysicalRecoveryScope};

const PROGRAM: &str = "venue-node-bitget";

/// Synchronous boundary consumed by `PhysicalGateway`. A production implementation must own its
/// async runtime inside the Bitget adapter; the fixed node package deliberately has no Tokio
/// dependency and this goal does not broaden that shared dependency surface.
trait BitgetNodeIo {
    fn connect_after_recovery(
        &mut self,
        binding: &GatewayBinding,
        connection_generation_floor: u64,
        private_generation_floor: u64,
    ) -> Result<(), BitgetPhysicalError>;

    fn collect_readback(
        &mut self,
        binding: &GatewayBinding,
        after_connection_generation: u64,
        after_private_generation: u64,
    ) -> Result<BitgetCollectedReadback, BitgetPhysicalError>;

    fn dispatch_once(
        &mut self,
        mutation: BitgetPreparedMutation,
    ) -> Result<BitgetMutationOutcome, BitgetPhysicalError>;

    fn exact_readback(
        &mut self,
        request: BitgetExactReadbackRequest,
    ) -> Result<BitgetExactOrderReadback, BitgetPhysicalError>;
}

struct BitgetCollectedReadback {
    rules: BitgetInstrumentRules,
    candidate: BitgetNodeReadbackCandidate,
    owner_routes: BTreeMap<String, OrderOwner>,
}

/// Rejects production manifest construction until Bitget supplies an opaque, scope-bound fresh
/// collection authority. A replay-checked candidate alone does not bind configuration epoch,
/// Owner/WAL/Unknown roots, collection freshness, or exact Owner coverage.
pub fn bitget_physical_recovery_manifest(
    _scope: PhysicalRecoveryScope,
    _candidate: &BitgetNodeReadbackCandidate,
) -> Result<PhysicalRecoveryReadbackManifest, BitgetPhysicalRecoveryManifestError> {
    Err(BitgetPhysicalRecoveryManifestError::Unavailable)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetPhysicalRecoveryManifestError {
    #[error(
        "Bitget physical recovery manifest is unavailable without scope-bound fresh collection authority"
    )]
    Unavailable,
    #[cfg(test)]
    #[error("Bitget fixture physical recovery scope does not match its candidate")]
    ScopeDrift,
    #[cfg(test)]
    #[error("Bitget fixture physical recovery lacks an exact Owner route")]
    OwnerRoute,
    #[cfg(test)]
    #[error(transparent)]
    Manifest(#[from] PhysicalRecoveryManifestError),
}

#[cfg(test)]
fn fixture_bitget_physical_recovery_manifest(
    scope: PhysicalRecoveryScope,
    candidate: &BitgetNodeReadbackCandidate,
    owner_routes: &BTreeMap<String, OrderOwner>,
) -> Result<PhysicalRecoveryReadbackManifest, BitgetPhysicalRecoveryManifestError> {
    if candidate.private().binding != *scope.binding() {
        return Err(BitgetPhysicalRecoveryManifestError::ScopeDrift);
    }
    let receipts = fixture_bitget_physical_recovery_receipts(&scope, candidate, owner_routes)?;
    Ok(PhysicalRecoveryReadbackManifest::verified(scope, receipts)?)
}

#[cfg(test)]
fn fixture_bitget_physical_recovery_receipts(
    scope: &PhysicalRecoveryScope,
    candidate: &BitgetNodeReadbackCandidate,
    owner_routes: &BTreeMap<String, OrderOwner>,
) -> Result<Vec<PhysicalReadbackReceipt>, BitgetPhysicalRecoveryManifestError> {
    let private = candidate.private();
    validate_fixture_owner_routes(private, owner_routes)?;
    let attempt_id = private.attempt_id;
    let private_generation = candidate.private_generation();
    let account_records = if account_is_empty(private) { 0 } else { 1 };
    let position_records = count_records(
        private
            .positions
            .iter()
            .filter(|position| !position.quantity.is_zero())
            .count(),
    )?;
    let order_records = count_records(private.orders.len())?;
    let fill_records = count_records(private.fills.len())?;
    let profile_version = candidate.families().scope().profile_version;

    Ok(vec![
        PhysicalReadbackReceipt::verified_complete(
            scope,
            PhysicalReadbackSurface::Account,
            attempt_id,
            private_generation,
            bitget_surface_evidence(
                candidate,
                b"account-and-settings",
                &[
                    BitgetPrivateSurface::Account,
                    BitgetPrivateSurface::Settings,
                ],
            ),
            account_records,
        )?,
        PhysicalReadbackReceipt::verified_complete(
            scope,
            PhysicalReadbackSurface::Positions,
            attempt_id,
            private_generation,
            bitget_surface_evidence(candidate, b"positions", &[BitgetPrivateSurface::Positions]),
            position_records,
        )?,
        PhysicalReadbackReceipt::verified_complete(
            scope,
            PhysicalReadbackSurface::UmOrder,
            attempt_id,
            private_generation,
            bitget_surface_evidence(
                candidate,
                b"um-order-normal",
                &[BitgetPrivateSurface::RegularOrders],
            ),
            order_records,
        )?,
        PhysicalReadbackReceipt::verified_unsupported_order_family(
            scope,
            PhysicalReadbackSurface::UmConditional,
            attempt_id,
            private_generation,
            bitget_unsupported_evidence(candidate, b"um-conditional"),
            profile_version,
        )?,
        PhysicalReadbackReceipt::verified_unsupported_order_family(
            scope,
            PhysicalReadbackSurface::UmAlgo,
            attempt_id,
            private_generation,
            bitget_unsupported_evidence(candidate, b"um-algo"),
            profile_version,
        )?,
        PhysicalReadbackReceipt::verified_complete(
            scope,
            PhysicalReadbackSurface::FillsCursor,
            attempt_id,
            private_generation,
            bitget_surface_evidence(candidate, b"fills-cursor", &[BitgetPrivateSurface::Fills]),
            fill_records,
        )?,
    ])
}

#[cfg(test)]
fn validate_fixture_owner_routes(
    private: &BitgetPrivateGenerationCandidate,
    owner_routes: &BTreeMap<String, OrderOwner>,
) -> Result<(), BitgetPhysicalRecoveryManifestError> {
    for order in &private.orders {
        let FieldState::Known(client_id) = &order.client_order_id else {
            return Err(BitgetPhysicalRecoveryManifestError::OwnerRoute);
        };
        let owner = owner_routes
            .get(client_id)
            .ok_or(BitgetPhysicalRecoveryManifestError::OwnerRoute)?;
        if owner.exchange != private.binding.venue.as_str()
            || owner.account != private.binding.trading_account_id
            || owner.symbol != private.binding.symbol
        {
            return Err(BitgetPhysicalRecoveryManifestError::OwnerRoute);
        }
    }
    Ok(())
}

#[cfg(test)]
fn account_is_empty(private: &BitgetPrivateGenerationCandidate) -> bool {
    private.balance.wallet_balance.is_zero()
        && private.balance.available_balance.is_zero()
        && private.balance.initial_margin.is_zero()
        && private.balance.maintenance_margin.is_zero()
}

#[cfg(test)]
fn count_records(value: usize) -> Result<u64, BitgetPhysicalRecoveryManifestError> {
    u64::try_from(value)
        .map_err(|_| PhysicalRecoveryManifestError::Coverage)
        .map_err(Into::into)
}

#[cfg(test)]
fn bitget_surface_evidence(
    candidate: &BitgetNodeReadbackCandidate,
    domain: &[u8],
    surfaces: &[BitgetPrivateSurface],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_evidence_bytes(&mut digest, b"venue-bitget-physical-readback-face-v1");
    commit_evidence_bytes(&mut digest, candidate.commitment_sha256().as_bytes());
    commit_evidence_bytes(&mut digest, domain);
    for raw in candidate
        .private()
        .raw_pages
        .iter()
        .filter(|raw| surfaces.contains(&raw.surface))
    {
        commit_evidence_bytes(&mut digest, &[bitget_private_surface_tag(raw.surface)]);
        commit_evidence_bytes(&mut digest, &raw.attempt_id.to_be_bytes());
        commit_evidence_bytes(&mut digest, &raw.generation.to_be_bytes());
        commit_evidence_bytes(&mut digest, &raw.page_index.to_be_bytes());
        commit_evidence_option(&mut digest, raw.request_cursor.as_deref());
        commit_evidence_option_u64(&mut digest, raw.fill_history_start_ms);
        commit_evidence_bytes(&mut digest, raw.payload_sha256.as_bytes());
    }
    digest.finalize().into()
}

#[cfg(test)]
fn bitget_unsupported_evidence(candidate: &BitgetNodeReadbackCandidate, family: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_evidence_bytes(&mut digest, b"venue-bitget-physical-unsupported-family-v1");
    commit_evidence_bytes(&mut digest, candidate.commitment_sha256().as_bytes());
    commit_evidence_bytes(&mut digest, family);
    commit_evidence_bytes(
        &mut digest,
        &candidate.families().scope().profile_version.to_be_bytes(),
    );
    digest.finalize().into()
}

#[cfg(test)]
const fn bitget_private_surface_tag(surface: BitgetPrivateSurface) -> u8 {
    match surface {
        BitgetPrivateSurface::Account => 1,
        BitgetPrivateSurface::Settings => 2,
        BitgetPrivateSurface::Positions => 3,
        BitgetPrivateSurface::RegularOrders => 4,
        BitgetPrivateSurface::Fills => 5,
    }
}

#[cfg(test)]
fn commit_evidence_option(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            commit_evidence_bytes(digest, &[1]);
            commit_evidence_bytes(digest, value.as_bytes());
        }
        None => commit_evidence_bytes(digest, &[0]),
    }
}

#[cfg(test)]
fn commit_evidence_option_u64(digest: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            commit_evidence_bytes(digest, &[1]);
            commit_evidence_bytes(digest, &value.to_be_bytes());
        }
        None => commit_evidence_bytes(digest, &[0]),
    }
}

#[cfg(test)]
fn commit_evidence_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

/// Local fixed-binary wrapper. It can only operate through a caller-supplied synchronous I/O
/// bridge and always advertises the adapter's current empty capability set.
struct BitgetPhysicalGateway<I> {
    binding: GatewayBinding,
    config: BitgetConfig,
    io: I,
    connected: bool,
    current: Option<BitgetCollectedReadback>,
    unknowns: BTreeMap<CommandId, BitgetUnknownMutation>,
    verified_commitments: BTreeSet<String>,
}

impl<I> BitgetPhysicalGateway<I> {
    fn new(binding: GatewayBinding, io: I) -> Self {
        Self {
            config: BitgetConfig::for_mode(binding.mode),
            binding,
            io,
            connected: false,
            current: None,
            unknowns: BTreeMap::new(),
            verified_commitments: BTreeSet::new(),
        }
    }
}

impl<I: BitgetNodeIo> BitgetPhysicalGateway<I> {
    fn collect_signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, BitgetPhysicalError> {
        if !self.connected || request.binding() != &self.binding {
            return Err(BitgetPhysicalError::Scope);
        }
        let collected = self.io.collect_readback(
            &self.binding,
            request.after_connection_generation(),
            request.after_private_generation(),
        )?;
        self.validate_collected(request, &collected)?;

        let mut exact_payload_digests = Vec::new();
        let mut command_results = Vec::with_capacity(request.commands().len());
        for key in request.commands() {
            if key.family() != NativeOrderFamily::UmOrder {
                return Err(BitgetPhysicalError::UnsupportedFamily);
            }
            let unknown = self
                .unknowns
                .get(key.command_id())
                .ok_or(BitgetPhysicalError::RecoveryIdentity)?;
            if unknown.client_order_id.as_deref() != Some(key.client_id().as_str()) {
                return Err(BitgetPhysicalError::RecoveryIdentity);
            }
            let exact_request = build_unknown_recovery_readback_request(
                unknown,
                collected.candidate.connection_generation(),
            )?;
            let exact = self.io.exact_readback(exact_request)?;
            let settlement = settle_unknown_readback(unknown, &exact)?;
            exact_payload_digests.push(exact.payload_sha256.clone());
            let state = match settlement.order {
                Some(order) if order.state == OrderState::Rejected => {
                    ReadbackCommandState::Rejected {
                        reason_code: "bitget_order_rejected".to_owned(),
                    }
                }
                Some(order) => ReadbackCommandState::Accepted {
                    venue_order_id: order.order_id,
                },
                None if settlement.finality == BitgetReadbackFinality::AbsentAtReadback => {
                    ReadbackCommandState::ProvenAbsent
                }
                None => return Err(BitgetPhysicalError::ExactReadback),
            };
            command_results.push(SignedCommandReadback::new(key.clone(), state)?);
        }

        let mut owned_open_orders = Vec::new();
        for order in &collected.candidate.private().orders {
            let client_id = match &order.client_order_id {
                FieldState::Known(client_id) => client_id,
                FieldState::Missing
                | FieldState::Null
                | FieldState::Unavailable { .. }
                | FieldState::NotApplicable => {
                    return Err(BitgetPhysicalError::OwnerRoute);
                }
            };
            let owner = collected
                .owner_routes
                .get(client_id)
                .ok_or(BitgetPhysicalError::OwnerRoute)?;
            owned_open_orders.push(SignedOwnedOrder::new(
                owner.clone(),
                NativeOrderFamily::UmOrder,
                CommandId::new(client_id.clone()).map_err(|_| BitgetPhysicalError::OwnerRoute)?,
                order.order_id.clone(),
            )?);
        }
        let nonzero_position_symbols = collected
            .candidate
            .private()
            .positions
            .iter()
            .filter(|position| !position.quantity.is_zero())
            .map(|position| position.symbol.clone())
            .collect();
        let commitment = readback_commitment(
            collected.candidate.commitment_sha256(),
            &exact_payload_digests,
        )?;
        let receipt = SignedReadbackReceipt::new(
            self.binding.clone(),
            collected.candidate.connection_generation(),
            collected.candidate.private_generation(),
            collected.candidate.private().observed_at_ms,
            commitment.clone(),
            vec![
                FamilyReadbackCoverage::complete(NativeOrderFamily::UmOrder),
                FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmConditional),
                FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmAlgo),
            ],
            owned_open_orders,
            nonzero_position_symbols,
            command_results,
        )?;
        self.verified_commitments.insert(commitment);
        self.current = Some(collected);
        Ok(receipt)
    }

    fn validate_collected(
        &self,
        request: &SignedReadbackRequest,
        collected: &BitgetCollectedReadback,
    ) -> Result<(), BitgetPhysicalError> {
        let candidate = &collected.candidate;
        if candidate.private().binding != self.binding
            || collected.rules.raw.binding != self.binding
            || candidate.connection_generation() < request.after_connection_generation()
            || candidate.private_generation() <= request.after_private_generation()
            || candidate.connection_generation()
                != collected.rules.snapshot.metadata.instrument.generation
            || !candidate.supports(NativeOrderFamily::UmOrder)
            || candidate.supports(NativeOrderFamily::UmConditional)
            || candidate.supports(NativeOrderFamily::UmAlgo)
        {
            return Err(BitgetPhysicalError::Generation);
        }
        Ok(())
    }

    fn dispatch_command(
        &mut self,
        command: &venue_domain::domain::ExecutionCommand,
        mutation_attempt_id: u64,
    ) -> GatewayDispatchResult {
        let now_ms = match unix_ms() {
            Ok(now_ms) => now_ms,
            Err(_) => {
                return GatewayDispatchResult::Rejected {
                    reason_code: "bitget_clock_invalid".to_owned(),
                };
            }
        };
        let Some(current) = self.current.as_ref() else {
            return GatewayDispatchResult::Rejected {
                reason_code: "bitget_signed_readback_missing".to_owned(),
            };
        };
        let prepared = match prepare_node_mutation(
            &current.candidate,
            &current.rules,
            &self.config,
            command,
            mutation_attempt_id,
            now_ms,
        ) {
            Ok(prepared) => prepared,
            Err(_) => {
                return GatewayDispatchResult::Rejected {
                    reason_code: "bitget_command_rejected".to_owned(),
                };
            }
        };
        self.dispatch_prepared(prepared)
    }

    fn dispatch_prepared(&mut self, prepared: BitgetNodePreparedMutation) -> GatewayDispatchResult {
        let command_id = prepared.command_id().clone();
        match self.io.dispatch_once(prepared.into_mutation()) {
            Ok(BitgetMutationOutcome::Acknowledged(ack)) => {
                self.settle_ack_or_unknown(command_id, ack)
            }
            Ok(BitgetMutationOutcome::Rejected) => GatewayDispatchResult::Rejected {
                reason_code: "bitget_venue_rejected".to_owned(),
            },
            Ok(BitgetMutationOutcome::Unknown(unknown)) => {
                self.unknowns.insert(command_id, unknown);
                GatewayDispatchResult::Unknown
            }
            Err(_) => GatewayDispatchResult::Unknown,
        }
    }

    fn settle_ack_or_unknown(
        &mut self,
        command_id: CommandId,
        ack: BitgetMutationAck,
    ) -> GatewayDispatchResult {
        let request = match build_ack_readback_request(&ack) {
            Ok(request) => request,
            Err(_) => return self.remember_acked_unknown(command_id, ack),
        };
        let exact = match self.io.exact_readback(request) {
            Ok(exact) => exact,
            Err(_) => return self.remember_acked_unknown(command_id, ack),
        };
        let settlement = match settle_ack_readback(&ack, &exact) {
            Ok(settlement) => settlement,
            Err(_) => return self.remember_acked_unknown(command_id, ack),
        };
        let Some(order) = settlement.order else {
            return self.remember_acked_unknown(command_id, ack);
        };
        if order.state == OrderState::Rejected {
            GatewayDispatchResult::Rejected {
                reason_code: "bitget_order_rejected".to_owned(),
            }
        } else {
            GatewayAcknowledgement::new(order.order_id)
                .map(GatewayDispatchResult::Acknowledged)
                .unwrap_or(GatewayDispatchResult::Unknown)
        }
    }

    fn remember_acked_unknown(
        &mut self,
        command_id: CommandId,
        ack: BitgetMutationAck,
    ) -> GatewayDispatchResult {
        self.unknowns.insert(
            command_id,
            BitgetUnknownMutation {
                binding: ack.binding,
                attempt_id: ack.attempt_id,
                generation: ack.generation,
                kind: ack.kind,
                order_id: Some(ack.order_id),
                client_order_id: Some(ack.client_order_id),
                dispatched_at_ms: ack.received_at_ms,
                reason: BitgetUnknownReason::AmbiguousResponse,
            },
        );
        GatewayDispatchResult::Unknown
    }
}

impl<I: BitgetNodeIo> PhysicalGateway for BitgetPhysicalGateway<I> {
    type Error = BitgetPhysicalError;

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        CapabilitySnapshot {
            binding: self.binding.clone(),
            version: 0,
            observed_ms: 0,
            expires_ms: 0,
            flags: capabilities(),
        }
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if permit.binding() != &self.binding {
            return Err(BitgetPhysicalError::Scope);
        }
        self.io.connect_after_recovery(
            &self.binding,
            permit.connection_generation_floor(),
            permit.private_generation_floor(),
        )?;
        self.connected = true;
        Ok(())
    }

    fn signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        self.collect_signed_readback(request)
    }

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        if receipt.binding() != &self.binding
            || !self
                .verified_commitments
                .contains(receipt.commitment_sha256())
        {
            return Err(BitgetPhysicalError::Commitment);
        }
        Ok(())
    }

    fn dispatch(&mut self, permit: DispatchPermit) -> GatewayDispatchResult {
        if permit.binding() != &self.binding {
            return GatewayDispatchResult::Rejected {
                reason_code: "bitget_binding_rejected".to_owned(),
            };
        }
        self.dispatch_command(permit.command(), permit.writer_revision())
    }
}

struct FailClosedBitgetIo;

impl BitgetNodeIo for FailClosedBitgetIo {
    fn connect_after_recovery(
        &mut self,
        _binding: &GatewayBinding,
        _connection_generation_floor: u64,
        _private_generation_floor: u64,
    ) -> Result<(), BitgetPhysicalError> {
        Err(BitgetPhysicalError::SharedRuntimeUnavailable)
    }

    fn collect_readback(
        &mut self,
        _binding: &GatewayBinding,
        _after_connection_generation: u64,
        _after_private_generation: u64,
    ) -> Result<BitgetCollectedReadback, BitgetPhysicalError> {
        Err(BitgetPhysicalError::SharedRuntimeUnavailable)
    }

    fn dispatch_once(
        &mut self,
        _mutation: BitgetPreparedMutation,
    ) -> Result<BitgetMutationOutcome, BitgetPhysicalError> {
        Err(BitgetPhysicalError::SharedRuntimeUnavailable)
    }

    fn exact_readback(
        &mut self,
        _request: BitgetExactReadbackRequest,
    ) -> Result<BitgetExactOrderReadback, BitgetPhysicalError> {
        Err(BitgetPhysicalError::SharedRuntimeUnavailable)
    }
}

#[derive(Debug, thiserror::Error)]
enum BitgetPhysicalError {
    #[error("Bitget PhysicalGateway scope or mode does not match")]
    Scope,
    #[error("Bitget PhysicalGateway readback generation is stale or inconsistent")]
    Generation,
    #[error("Bitget PhysicalGateway cannot resolve an exact durable Owner route")]
    OwnerRoute,
    #[error("Bitget PhysicalGateway cannot reconstruct an UNKNOWN exact-readback identity")]
    RecoveryIdentity,
    #[error("Bitget PhysicalGateway rejects an unsupported canonical order family")]
    UnsupportedFamily,
    #[error("Bitget PhysicalGateway exact readback is absent or inconsistent")]
    ExactReadback,
    #[error("Bitget PhysicalGateway readback commitment is not adapter-produced")]
    Commitment,
    #[error("the shared synchronous node runtime cannot yet drive the async Bitget transport")]
    SharedRuntimeUnavailable,
    #[error(transparent)]
    AdapterExecution(#[from] venue_gateway_bitget::BitgetExecutionError),
    #[error(transparent)]
    Host(#[from] venue_node::SafeHostError),
}

fn readback_commitment(
    candidate_commitment: &str,
    exact_payload_digests: &[String],
) -> Result<String, BitgetPhysicalError> {
    let mut digest = Sha256::new();
    digest.update(candidate_commitment.as_bytes());
    for payload_digest in exact_payload_digests {
        digest.update(payload_digest.as_bytes());
    }
    let bytes: [u8; 32] = digest.finalize().into();
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").map_err(|_| BitgetPhysicalError::Commitment)?;
    }
    Ok(encoded)
}

fn unix_ms() -> Result<u64, BitgetPhysicalError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BitgetPhysicalError::Scope)?
            .as_millis(),
    )
    .map_err(|_| BitgetPhysicalError::Scope)
}

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Bitget)?;
    let arguments = launch.legacy_runtime_arguments(PROGRAM)?;
    let cli = venue::Cli::try_parse_from(arguments)?;
    let config =
        venue::config::Config::load(&cli.config).map_err(|error| NodeError::ExistingRuntime {
            venue: VenueId::Bitget,
            message: error.to_string(),
        })?;
    launch.validate_runtime_scope(&config.trading_account_id, &config.symbol)?;
    let account_binding = config
        .bitget
        .ok_or(NodeError::RuntimeScope)?
        .account_binding;
    account_binding
        .validate_gateway_binding(launch.binding())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Bitget))?;
    let adapter = BitgetConfig::for_mode(launch.binding().mode);
    let candidate_gateway =
        BitgetPhysicalGateway::new(launch.binding().clone(), FailClosedBitgetIo);
    if PhysicalGateway::binding(&candidate_gateway) != launch.binding()
        || candidate_gateway.config != adapter
    {
        return Err(NodeError::AdapterIsolation(VenueId::Bitget));
    }
    AdapterIsolation {
        venue: VenueId::Bitget,
        mode: adapter.mode(),
        endpoints: &[
            adapter.rest_origin(),
            adapter.public_ws(),
            adapter.private_ws(),
        ],
        credential_environment: &[
            "BITGET_API_KEY",
            "BITGET_API_SECRET",
            "BITGET_API_PASSPHRASE",
            "BITGET_PASSPHRASE",
        ],
        credential_prefix: "BITGET_",
        account_binding: account_binding.as_str(),
    }
    .validate(launch.binding())?;
    if launch.binding().mode == GatewayMode::Test {
        return reject_unintegrated_legacy_test_runtime(VenueId::Bitget);
    }
    venue::start_hedged_grid_bitget_deployment(cli).map_err(|error| NodeError::ExistingRuntime {
        venue: VenueId::Bitget,
        message: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde_json::json;
    use venue_domain::domain::{
        ExecutionCommand, OrderCommand, OrderPurpose, OrderSide, PositionSide, Price,
    };
    use venue_gateway_api::{GatewayApiError, MutationCapability};
    use venue_gateway_bitget::{
        BITGET_ORDER_PROFILE_VERSION, BitgetAckStatus, BitgetOrderFamilyEvidence,
        BitgetOrderFamilyScope, BitgetUnsupportedEvidence,
        instrument::{BitgetRawInstrumentPayload, parse_instrument_rules},
        parse_exact_order_readback,
        private::{
            BitgetPrivateError, BitgetPrivateFace, BitgetPrivateSurface, BitgetRawPrivatePage,
            complete_private_turn, parse_account_face, parse_fill_page, parse_positions_face,
            parse_regular_order_page, parse_settings_face,
        },
    };
    use venue_runtime::account::{PhysicalReadbackCoverage, PhysicalRecoveryAuthorityRoots};

    use super::*;

    #[derive(Clone, Copy)]
    enum ExactBehavior {
        Present,
        Fail,
    }

    struct FakeIo {
        exact: ExactBehavior,
        dispatches: usize,
    }

    impl BitgetNodeIo for FakeIo {
        fn connect_after_recovery(
            &mut self,
            _binding: &GatewayBinding,
            _connection_generation_floor: u64,
            _private_generation_floor: u64,
        ) -> Result<(), BitgetPhysicalError> {
            Ok(())
        }

        fn collect_readback(
            &mut self,
            _binding: &GatewayBinding,
            _after_connection_generation: u64,
            _after_private_generation: u64,
        ) -> Result<BitgetCollectedReadback, BitgetPhysicalError> {
            Err(BitgetPhysicalError::SharedRuntimeUnavailable)
        }

        fn dispatch_once(
            &mut self,
            mutation: BitgetPreparedMutation,
        ) -> Result<BitgetMutationOutcome, BitgetPhysicalError> {
            self.dispatches += 1;
            let client_order_id = mutation
                .client_order_id()
                .ok_or(BitgetPhysicalError::RecoveryIdentity)?
                .to_owned();
            let now_ms = unix_ms()?;
            Ok(BitgetMutationOutcome::Acknowledged(BitgetMutationAck {
                binding: mutation.binding,
                attempt_id: mutation.attempt_id,
                generation: mutation.generation,
                kind: mutation.kind,
                order_id: "9001".to_owned(),
                client_order_id,
                accepted_at_ms: now_ms,
                received_at_ms: now_ms,
                status: BitgetAckStatus::AcceptedOnly,
                payload_sha256: "a".repeat(64),
                raw_payload: b"ack".to_vec(),
            }))
        }

        fn exact_readback(
            &mut self,
            request: BitgetExactReadbackRequest,
        ) -> Result<BitgetExactOrderReadback, BitgetPhysicalError> {
            if matches!(self.exact, ExactBehavior::Fail) {
                return Err(BitgetPhysicalError::ExactReadback);
            }
            let now_ms = unix_ms()?;
            let payload = json!({
                "code":"00000",
                "data":{
                    "orderId":"9001", "clientOid":"venue_place_1",
                    "category":"USDT-FUTURES", "symbol":"BTCUSDT",
                    "orderStatus":"new", "side":"buy", "posSide":"long",
                    "holdMode":"hedge_mode", "tradeSide":"open_long",
                    "qty":"0.001", "cumExecQty":"0", "price":"100000",
                    "avgPrice":"0", "delegateType":"normal"
                }
            })
            .to_string()
            .into_bytes();
            parse_exact_order_readback(
                &BitgetConfig::for_mode(request.binding.mode),
                request,
                now_ms,
                now_ms,
                payload,
            )
            .map_err(BitgetPhysicalError::from)
        }
    }

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    fn raw(
        binding: &GatewayBinding,
        surface: BitgetPrivateSurface,
        now_ms: u64,
        data: serde_json::Value,
    ) -> Result<BitgetRawPrivatePage, Box<dyn std::error::Error>> {
        Ok(BitgetRawPrivatePage::new_with_generation(
            surface,
            binding.clone(),
            9,
            7,
            0,
            None,
            (surface == BitgetPrivateSurface::Fills).then_some(now_ms.saturating_sub(1_000)),
            now_ms,
            json!({"code":"00000", "data":data}).to_string(),
        )?)
    }

    fn private_faces(
        binding: &GatewayBinding,
        observed_at_ms: u64,
        balance: &str,
    ) -> Result<Vec<BitgetPrivateFace>, Box<dyn std::error::Error>> {
        private_faces_with_regular_rows(binding, observed_at_ms, balance, json!([]))
    }

    fn private_faces_with_regular_rows(
        binding: &GatewayBinding,
        observed_at_ms: u64,
        balance: &str,
        regular_rows: serde_json::Value,
    ) -> Result<Vec<BitgetPrivateFace>, Box<dyn std::error::Error>> {
        Ok(vec![
            BitgetPrivateFace::Account(parse_account_face(raw(
                binding,
                BitgetPrivateSurface::Account,
                observed_at_ms,
                json!({
                    "imr":"0", "mmr":"0",
                    "assets":[{"coin":"USDT", "balance":balance, "available":balance}]
                }),
            )?)?),
            BitgetPrivateFace::Settings(parse_settings_face(raw(
                binding,
                BitgetPrivateSurface::Settings,
                observed_at_ms,
                json!({"holdMode":"hedge_mode"}),
            )?)?),
            BitgetPrivateFace::Positions(parse_positions_face(raw(
                binding,
                BitgetPrivateSurface::Positions,
                observed_at_ms,
                json!({"list":[]}),
            )?)?),
            BitgetPrivateFace::RegularOrders(vec![parse_regular_order_page(raw(
                binding,
                BitgetPrivateSurface::RegularOrders,
                observed_at_ms,
                json!({"list":regular_rows, "cursor":null}),
            )?)?]),
            BitgetPrivateFace::Fills(vec![parse_fill_page(raw(
                binding,
                BitgetPrivateSurface::Fills,
                observed_at_ms,
                json!({"list":[], "cursor":null}),
            )?)?]),
        ])
    }

    fn collected(mode: GatewayMode) -> Result<BitgetCollectedReadback, Box<dyn std::error::Error>> {
        collected_with_balance(mode, "20")
    }

    fn collected_with_balance(
        mode: GatewayMode,
        balance: &str,
    ) -> Result<BitgetCollectedReadback, Box<dyn std::error::Error>> {
        let now_ms = unix_ms()?;
        let observed_at_ms = now_ms.saturating_sub(1);
        let expires_at_ms = now_ms
            .checked_add(60_000)
            .ok_or(BitgetPhysicalError::Scope)?;
        collected_at(
            mode,
            balance,
            json!([]),
            observed_at_ms,
            expires_at_ms,
            now_ms,
        )
    }

    fn collected_at(
        mode: GatewayMode,
        balance: &str,
        regular_rows: serde_json::Value,
        observed_at_ms: u64,
        expires_at_ms: u64,
        validated_at_ms: u64,
    ) -> Result<BitgetCollectedReadback, Box<dyn std::error::Error>> {
        let binding = binding(mode)?;
        let rules = parse_instrument_rules(
            BitgetRawInstrumentPayload::new(
                binding.clone(),
                7,
                observed_at_ms,
                expires_at_ms,
                include_str!(
                    "../../../../crates/venue-gateway-bitget/tests/fixtures/bitget_uta_btcusdt_instrument.json"
                )
                .to_owned(),
            )?,
            validated_at_ms,
        )?;
        let private = complete_private_turn(private_faces_with_regular_rows(
            &binding,
            observed_at_ms,
            balance,
            regular_rows,
        )?)?;
        let candidate = BitgetNodeReadbackCandidate::validate(
            BitgetOrderFamilyScope {
                binding,
                profile_version: BITGET_ORDER_PROFILE_VERSION,
                attempt_id: 9,
                generation: 7,
                observed_at_ms,
                expires_at_ms,
            },
            &rules,
            validated_at_ms,
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
        Ok(BitgetCollectedReadback {
            rules,
            candidate,
            owner_routes: BTreeMap::new(),
        })
    }

    fn recovery_scope(
        mode: GatewayMode,
        owner_root_seed: u8,
    ) -> Result<PhysicalRecoveryScope, Box<dyn std::error::Error>> {
        Ok(PhysicalRecoveryScope::verified(
            binding(mode)?,
            "bitget_config_1",
            3,
            1,
            6,
            PhysicalRecoveryAuthorityRoots::verified([owner_root_seed; 32], [2; 32], [3; 32])?,
        )?)
    }

    fn place_command() -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            command_id: CommandId::new("place_1")?,
            client_order_id: CommandId::new("venue_place_1")?,
            owner: OrderOwner {
                strategy_instance_id: "grid_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "bitget".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 3),
            limit_price: Price::new(Decimal::from(100_000))?,
            reduce_only: false,
        }))
    }

    fn regular_order_row() -> serde_json::Value {
        json!({
            "orderId":"9001", "clientOid":"venue_open_1",
            "category":"USDT-FUTURES", "symbol":"BTCUSDT",
            "orderStatus":"live", "side":"buy", "posSide":"long",
            "holdMode":"hedge_mode", "tradeSide":"open_long",
            "qty":"0.001", "cumExecQty":"0", "price":"100000",
            "avgPrice":"0", "delegateType":"normal"
        })
    }

    #[test]
    fn fixed_wrapper_binds_demo_and_live_but_grants_no_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        for mode in [GatewayMode::Test, GatewayMode::Live] {
            let binding = binding(mode)?;
            let gateway = BitgetPhysicalGateway::new(binding.clone(), FailClosedBitgetIo);
            assert_eq!(gateway.config.mode(), mode);
            assert_eq!(gateway.config.paper_trading(), mode == GatewayMode::Test);
            assert_eq!(gateway.binding(), &binding);
            let capability = gateway.capability_snapshot();
            assert!(capability.flags.is_empty());
            assert_eq!(
                capability.authorize(&binding, 1, 1, MutationCapability::PlaceLimit),
                Err(GatewayApiError::CapabilityScope)
            );
        }
        Ok(())
    }

    #[test]
    fn production_manifest_is_unavailable_from_a_plain_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let collected = collected(GatewayMode::Live)?;
        assert_eq!(
            bitget_physical_recovery_manifest(
                recovery_scope(GatewayMode::Live, 1)?,
                &collected.candidate,
            ),
            Err(BitgetPhysicalRecoveryManifestError::Unavailable)
        );
        Ok(())
    }

    #[test]
    fn historical_candidate_cannot_be_relabelled_under_new_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let historical = collected_at(GatewayMode::Live, "0", json!([]), 2_000, 3_000, 2_500)?;
        assert!(historical.candidate.families().scope().expires_at_ms < unix_ms()?);
        let relabelled_scope = PhysicalRecoveryScope::verified(
            binding(GatewayMode::Live)?,
            "bitget_config_2",
            4,
            1,
            8,
            PhysicalRecoveryAuthorityRoots::verified([9; 32], [8; 32], [7; 32])?,
        )?;
        assert_eq!(
            bitget_physical_recovery_manifest(relabelled_scope, &historical.candidate),
            Err(BitgetPhysicalRecoveryManifestError::Unavailable)
        );
        Ok(())
    }

    #[test]
    fn fixture_manifest_maps_demo_and_live_without_granting_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        for mode in [GatewayMode::Test, GatewayMode::Live] {
            let collected = collected(mode)?;
            let manifest = fixture_bitget_physical_recovery_manifest(
                recovery_scope(mode, 1)?,
                &collected.candidate,
                &collected.owner_routes,
            )?;
            assert_eq!(manifest.scope().binding().mode, mode);
            assert_eq!(manifest.attempt_id(), 9);
            assert_eq!(manifest.private_generation(), 9);
            assert!(matches!(
                manifest.coverage(PhysicalReadbackSurface::UmOrder),
                PhysicalReadbackCoverage::Complete {
                    record_count: 0,
                    ..
                }
            ));
            for surface in [
                PhysicalReadbackSurface::UmConditional,
                PhysicalReadbackSurface::UmAlgo,
            ] {
                assert!(matches!(
                    manifest.coverage(surface),
                    PhysicalReadbackCoverage::Unsupported {
                        profile_version: BITGET_ORDER_PROFILE_VERSION,
                        ..
                    }
                ));
            }
            let gateway = BitgetPhysicalGateway::new(binding(mode)?, FailClosedBitgetIo);
            assert!(gateway.capability_snapshot().flags.is_empty());
        }
        Ok(())
    }

    #[test]
    fn explicit_empty_account_still_maps_all_six_faces() -> Result<(), Box<dyn std::error::Error>> {
        let collected = collected_with_balance(GatewayMode::Test, "0")?;
        let manifest = fixture_bitget_physical_recovery_manifest(
            recovery_scope(GatewayMode::Test, 1)?,
            &collected.candidate,
            &collected.owner_routes,
        )?;
        for surface in [
            PhysicalReadbackSurface::Account,
            PhysicalReadbackSurface::Positions,
            PhysicalReadbackSurface::UmOrder,
            PhysicalReadbackSurface::FillsCursor,
        ] {
            assert!(matches!(
                manifest.coverage(surface),
                PhysicalReadbackCoverage::Complete {
                    record_count: 0,
                    ..
                }
            ));
        }
        assert!(manifest.commitment_sha256().iter().any(|byte| *byte != 0));
        Ok(())
    }

    #[test]
    fn fixture_nonempty_regular_orders_require_exact_owner_routes()
    -> Result<(), Box<dyn std::error::Error>> {
        let now_ms = unix_ms()?;
        let collected = collected_at(
            GatewayMode::Test,
            "20",
            json!([regular_order_row()]),
            now_ms.saturating_sub(1),
            now_ms
                .checked_add(60_000)
                .ok_or(BitgetPhysicalError::Scope)?,
            now_ms,
        )?;
        assert_eq!(collected.candidate.private().orders.len(), 1);
        assert!(collected.owner_routes.is_empty());
        assert_eq!(
            fixture_bitget_physical_recovery_manifest(
                recovery_scope(GatewayMode::Test, 1)?,
                &collected.candidate,
                &collected.owner_routes,
            ),
            Err(BitgetPhysicalRecoveryManifestError::OwnerRoute)
        );
        Ok(())
    }

    #[test]
    fn mixed_private_attempts_cannot_be_mapped_into_one_manifest()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Test)?;
        let observed_at_ms = unix_ms()?.saturating_sub(1);
        let mut faces = private_faces(&binding, observed_at_ms, "0")?;
        let BitgetPrivateFace::Positions(positions) = &mut faces[2] else {
            return Err(Box::new(BitgetPhysicalError::Scope));
        };
        positions.raw.attempt_id = 10;
        assert_eq!(
            complete_private_turn(faces),
            Err(BitgetPrivateError::MixedAttempt)
        );
        Ok(())
    }

    #[test]
    fn authority_root_drift_cannot_stitch_bitget_receipts() -> Result<(), Box<dyn std::error::Error>>
    {
        let collected = collected_with_balance(GatewayMode::Live, "0")?;
        let expected = recovery_scope(GatewayMode::Live, 1)?;
        let drifted = recovery_scope(GatewayMode::Live, 9)?;
        let drifted_receipts = fixture_bitget_physical_recovery_receipts(
            &drifted,
            &collected.candidate,
            &collected.owner_routes,
        )?;
        assert_eq!(
            PhysicalRecoveryReadbackManifest::verified(expected, drifted_receipts),
            Err(PhysicalRecoveryManifestError::ScopeDrift)
        );
        Ok(())
    }

    #[test]
    fn ack_is_exposed_only_after_exact_readback_and_failure_becomes_unknown()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Test)?;
        let mut exact = BitgetPhysicalGateway::new(
            binding.clone(),
            FakeIo {
                exact: ExactBehavior::Present,
                dispatches: 0,
            },
        );
        exact.connected = true;
        exact.current = Some(collected(GatewayMode::Test)?);
        assert!(matches!(
            exact.dispatch_command(&place_command()?, 11),
            GatewayDispatchResult::Acknowledged(ref ack) if ack.venue_order_id() == "9001"
        ));
        assert_eq!(exact.io.dispatches, 1);
        assert!(exact.unknowns.is_empty());

        let mut unknown = BitgetPhysicalGateway::new(
            binding,
            FakeIo {
                exact: ExactBehavior::Fail,
                dispatches: 0,
            },
        );
        unknown.connected = true;
        unknown.current = Some(collected(GatewayMode::Test)?);
        assert_eq!(
            unknown.dispatch_command(&place_command()?, 12),
            GatewayDispatchResult::Unknown
        );
        assert_eq!(unknown.io.dispatches, 1);
        assert!(unknown.unknowns.contains_key(place_command()?.command_id()));
        Ok(())
    }
}
