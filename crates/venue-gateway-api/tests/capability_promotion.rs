use venue_gateway_api::{
    CanaryAdmissionReceipt, CapabilityFlags, CapabilityProbeCandidate, CapabilityPromotionError,
    CapabilitySnapshot, CompleteOrderFamilyEvidence, ControlAppliedReceipt, ControlState,
    EvidenceCommitment, GatewayBinding, GatewayMode, HostAdmissionEvidence, MutationCapability,
    OrderFamilyEvidence, OrderFamilySupport, OwnerRecoveryReceipt, PromotionScope, VenueId,
    WalRecoveryReceipt, WriterFenceReceipt, promote_capability,
};

const ACCOUNT: &str = "00000000-0000-0000-0000-000000000001";
const NOW_MS: u64 = 10_000;

fn commitment(byte: char) -> Result<EvidenceCommitment, CapabilityPromotionError> {
    EvidenceCommitment::new(byte.to_string().repeat(64))
}

fn binding(
    venue: VenueId,
    mode: GatewayMode,
    account: &str,
    symbol: &str,
) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
    Ok(GatewayBinding::new(venue, mode, account, symbol.parse()?)?)
}

fn families(seed: char) -> Result<CompleteOrderFamilyEvidence, CapabilityPromotionError> {
    Ok(CompleteOrderFamilyEvidence::new(
        OrderFamilyEvidence::new(OrderFamilySupport::Complete, commitment(seed)?),
        OrderFamilyEvidence::new(
            OrderFamilySupport::ExplicitlyUnsupported,
            commitment(
                char::from_u32(u32::from(seed) + 1).ok_or(CapabilityPromotionError::Commitment)?,
            )?,
        ),
        OrderFamilyEvidence::new(
            OrderFamilySupport::ExplicitlyUnsupported,
            commitment(
                char::from_u32(u32::from(seed) + 2).ok_or(CapabilityPromotionError::Commitment)?,
            )?,
        ),
    ))
}

fn flags() -> CapabilityFlags {
    CapabilityFlags::READ_ACCOUNT
        | CapabilityFlags::READ_ORDERS
        | CapabilityFlags::READ_FILLS
        | CapabilityFlags::PRIVATE_STREAM
        | CapabilityFlags::TRADE
        | CapabilityFlags::PLACE_LIMIT
        | CapabilityFlags::CANCEL
}

fn candidate_with(
    binding: GatewayBinding,
    capability_flags: CapabilityFlags,
    order_families: CompleteOrderFamilyEvidence,
) -> Result<CapabilityProbeCandidate, CapabilityPromotionError> {
    CapabilityProbeCandidate::from_snapshot(
        CapabilitySnapshot {
            binding,
            version: 7,
            observed_ms: 9_000,
            expires_ms: 30_000,
            flags: capability_flags,
        },
        11,
        13,
        order_families,
        commitment('a')?,
    )
}

#[allow(clippy::too_many_arguments)]
fn admission_with(
    scope: PromotionScope,
    order_families: CompleteOrderFamilyEvidence,
    expires_ms: u64,
    control_commitment: char,
    owner_revision: u64,
    wal_tail: u64,
    writer_revision: u64,
    canary_sequence: u64,
) -> Result<HostAdmissionEvidence, CapabilityPromotionError> {
    HostAdmissionEvidence::new(
        scope.clone(),
        expires_ms,
        order_families,
        ControlAppliedReceipt::new(
            scope.clone(),
            ControlState::Active,
            17,
            9_100,
            commitment(control_commitment)?,
        )?,
        OwnerRecoveryReceipt::new(scope.clone(), owner_revision, 9_200, commitment('d')?)?,
        WalRecoveryReceipt::new(scope.clone(), wal_tail, 0, 0, 9_300, commitment('e')?)?,
        WriterFenceReceipt::new(scope.clone(), 19, writer_revision, 9_400, commitment('f')?)?,
        CanaryAdmissionReceipt::new(scope, canary_sequence, 7, 9_500, 25_000, commitment('1')?)?,
    )
}

fn exact_scope() -> Result<PromotionScope, Box<dyn std::error::Error>> {
    Ok(PromotionScope::new(
        binding(VenueId::Bybit, GatewayMode::Live, ACCOUNT, "BTC/USDT")?,
        5,
        11,
        13,
    )?)
}

#[test]
fn only_host_promotion_creates_short_lived_mutation_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = exact_scope()?;
    let order_families = families('4')?;
    let candidate = candidate_with(scope.binding().clone(), flags(), order_families.clone())?;
    let evidence = admission_with(scope, order_families, 20_000, 'c', 23, 29, 31, 37)?;

    let admitted = promote_capability(&candidate, evidence.clone(), NOW_MS)?;
    assert_eq!(admitted.capability_version(), 7);
    assert_eq!(admitted.expires_ms(), 20_000);
    assert_eq!(admitted.probe_commitment(), candidate.probe_commitment());
    assert_eq!(
        admitted.authorize(&evidence, 19_999, MutationCapability::PlaceLimit),
        Ok(())
    );
    assert_eq!(
        admitted.authorize(&evidence, 20_000, MutationCapability::PlaceLimit),
        Err(CapabilityPromotionError::Freshness)
    );
    Ok(())
}

#[test]
fn withdrawal_in_probe_and_overlong_promotion_ttl_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = exact_scope()?;
    let order_families = families('4')?;
    let withdrawal_candidate = candidate_with(
        scope.binding().clone(),
        flags() | CapabilityFlags::WITHDRAW,
        order_families.clone(),
    )?;
    let evidence = admission_with(
        scope.clone(),
        order_families.clone(),
        20_000,
        'c',
        23,
        29,
        31,
        37,
    )?;
    assert_eq!(
        promote_capability(&withdrawal_candidate, evidence, NOW_MS),
        Err(CapabilityPromotionError::Denied)
    );

    let candidate = candidate_with(scope.binding().clone(), flags(), order_families.clone())?;
    let overlong = admission_with(scope, order_families, NOW_MS + 30_001, 'c', 23, 29, 31, 37)?;
    assert_eq!(
        promote_capability(&candidate, overlong, NOW_MS),
        Err(CapabilityPromotionError::Freshness)
    );
    Ok(())
}

#[test]
fn every_bound_host_fact_drift_invalidates_the_opaque_token()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = exact_scope()?;
    let order_families = families('4')?;
    let candidate = candidate_with(scope.binding().clone(), flags(), order_families.clone())?;
    let evidence = admission_with(
        scope.clone(),
        order_families.clone(),
        20_000,
        'c',
        23,
        29,
        31,
        37,
    )?;
    let admitted = promote_capability(&candidate, evidence, NOW_MS)?;

    let scope_drifts = [
        PromotionScope::new(
            binding(VenueId::Okx, GatewayMode::Live, ACCOUNT, "BTC/USDT")?,
            5,
            11,
            13,
        )?,
        PromotionScope::new(
            binding(VenueId::Bybit, GatewayMode::Test, ACCOUNT, "BTC/USDT")?,
            5,
            11,
            13,
        )?,
        PromotionScope::new(
            binding(
                VenueId::Bybit,
                GatewayMode::Live,
                "00000000-0000-0000-0000-000000000002",
                "BTC/USDT",
            )?,
            5,
            11,
            13,
        )?,
        PromotionScope::new(
            binding(VenueId::Bybit, GatewayMode::Live, ACCOUNT, "ETH/USDT")?,
            5,
            11,
            13,
        )?,
        PromotionScope::new(scope.binding().clone(), 6, 11, 13)?,
        PromotionScope::new(scope.binding().clone(), 5, 12, 13)?,
        PromotionScope::new(scope.binding().clone(), 5, 11, 14)?,
    ];
    for drifted_scope in scope_drifts {
        let drift = admission_with(
            drifted_scope,
            order_families.clone(),
            20_000,
            'c',
            23,
            29,
            31,
            37,
        )?;
        assert_eq!(
            admitted.authorize(&drift, 15_000, MutationCapability::Cancel),
            Err(CapabilityPromotionError::Drift)
        );
    }

    let receipt_drifts = [
        admission_with(
            scope.clone(),
            order_families.clone(),
            20_000,
            'b',
            23,
            29,
            31,
            37,
        )?,
        admission_with(
            scope.clone(),
            order_families.clone(),
            20_000,
            'c',
            24,
            29,
            31,
            37,
        )?,
        admission_with(
            scope.clone(),
            order_families.clone(),
            20_000,
            'c',
            23,
            30,
            31,
            37,
        )?,
        admission_with(
            scope.clone(),
            order_families.clone(),
            20_000,
            'c',
            23,
            29,
            32,
            37,
        )?,
        admission_with(
            scope.clone(),
            order_families.clone(),
            20_000,
            'c',
            23,
            29,
            31,
            38,
        )?,
        admission_with(scope, families('7')?, 20_000, 'c', 23, 29, 31, 37)?,
    ];
    for drift in receipt_drifts {
        assert_eq!(
            admitted.authorize(&drift, 15_000, MutationCapability::Cancel),
            Err(CapabilityPromotionError::Drift)
        );
    }
    Ok(())
}

#[test]
fn promotion_rejects_probe_scope_family_control_and_wal_gaps()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = exact_scope()?;
    let order_families = families('4')?;
    let candidate = candidate_with(scope.binding().clone(), flags(), order_families.clone())?;
    let wrong_scope = PromotionScope::new(scope.binding().clone(), 5, 11, 14)?;
    let wrong_generation = admission_with(
        wrong_scope,
        order_families.clone(),
        20_000,
        'c',
        23,
        29,
        31,
        37,
    )?;
    assert_eq!(
        promote_capability(&candidate, wrong_generation, NOW_MS),
        Err(CapabilityPromotionError::Scope)
    );

    let family_drift = admission_with(scope.clone(), families('7')?, 20_000, 'c', 23, 29, 31, 37)?;
    assert_eq!(
        promote_capability(&candidate, family_drift, NOW_MS),
        Err(CapabilityPromotionError::Scope)
    );

    let paused = HostAdmissionEvidence::new(
        scope.clone(),
        20_000,
        order_families.clone(),
        ControlAppliedReceipt::new(
            scope.clone(),
            ControlState::Paused,
            17,
            9_100,
            commitment('c')?,
        )?,
        OwnerRecoveryReceipt::new(scope.clone(), 23, 9_200, commitment('d')?)?,
        WalRecoveryReceipt::new(scope.clone(), 29, 0, 0, 9_300, commitment('e')?)?,
        WriterFenceReceipt::new(scope.clone(), 19, 31, 9_400, commitment('f')?)?,
        CanaryAdmissionReceipt::new(scope.clone(), 37, 7, 9_500, 25_000, commitment('1')?)?,
    );
    assert_eq!(paused, Err(CapabilityPromotionError::Control));

    let unsettled_wal = HostAdmissionEvidence::new(
        scope.clone(),
        20_000,
        order_families.clone(),
        ControlAppliedReceipt::new(
            scope.clone(),
            ControlState::Active,
            17,
            9_100,
            commitment('c')?,
        )?,
        OwnerRecoveryReceipt::new(scope.clone(), 23, 9_200, commitment('d')?)?,
        WalRecoveryReceipt::new(scope.clone(), 29, 0, 1, 9_300, commitment('e')?)?,
        WriterFenceReceipt::new(scope.clone(), 19, 31, 9_400, commitment('f')?)?,
        CanaryAdmissionReceipt::new(scope, 37, 7, 9_500, 25_000, commitment('1')?)?,
    );
    assert_eq!(unsettled_wal, Err(CapabilityPromotionError::OwnerWal));
    Ok(())
}
