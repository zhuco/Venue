use venue_gateway_api::{
    CanaryAdmissionReceipt, CapabilityFlags, CapabilityProbeCandidate, CapabilityPromotionError,
    CapabilitySnapshot, CompleteOrderFamilyEvidence, ControlAppliedReceipt, ControlState,
    EvidenceCommitment, GatewayBinding, GatewayMode, HostAdmissionEvidence, OrderFamilyEvidence,
    OrderFamilySupport, OwnerRecoveryReceipt, PromotionScope, VenueId, WalRecoveryReceipt,
    WriterFenceReceipt, promote_capability,
};

const ACCOUNT: &str = "00000000-0000-0000-0000-000000000001";

fn commitment(byte: char) -> Result<EvidenceCommitment, CapabilityPromotionError> {
    EvidenceCommitment::new(byte.to_string().repeat(64))
}

fn fixture() -> Result<(CapabilityProbeCandidate, HostAdmissionEvidence), Box<dyn std::error::Error>>
{
    let binding = GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        ACCOUNT,
        "BTC/USDT".parse()?,
    )?;
    let scope = PromotionScope::new(binding.clone(), 5, 11, 13)?;
    let families = CompleteOrderFamilyEvidence::new(
        OrderFamilyEvidence::new(OrderFamilySupport::Complete, commitment('1')?),
        OrderFamilyEvidence::new(OrderFamilySupport::ExplicitlyUnsupported, commitment('2')?),
        OrderFamilyEvidence::new(OrderFamilySupport::ExplicitlyUnsupported, commitment('3')?),
    );
    let flags = CapabilityFlags::READ_ACCOUNT
        | CapabilityFlags::READ_ORDERS
        | CapabilityFlags::READ_FILLS
        | CapabilityFlags::PRIVATE_STREAM
        | CapabilityFlags::TRADE
        | CapabilityFlags::PLACE_LIMIT
        | CapabilityFlags::CANCEL;
    let candidate = CapabilityProbeCandidate::from_snapshot(
        CapabilitySnapshot {
            binding,
            version: 7,
            observed_ms: 9_000,
            expires_ms: 30_000,
            flags,
        },
        11,
        13,
        families.clone(),
        commitment('4')?,
    )?;
    let evidence = HostAdmissionEvidence::new(
        scope.clone(),
        20_000,
        families,
        ControlAppliedReceipt::new(
            scope.clone(),
            ControlState::Active,
            17,
            9_100,
            commitment('5')?,
        )?,
        OwnerRecoveryReceipt::new(scope.clone(), 23, 9_200, commitment('6')?)?,
        WalRecoveryReceipt::new(scope.clone(), 29, 0, 0, 9_300, commitment('7')?)?,
        WriterFenceReceipt::new(scope.clone(), 19, 31, 9_400, commitment('8')?)?,
        CanaryAdmissionReceipt::new(scope, 37, 7, 9_500, 25_000, commitment('9')?)?,
    )?;
    Ok((candidate, evidence))
}

#[test]
fn caller_constructed_receipts_cannot_promote_a_capability()
-> Result<(), Box<dyn std::error::Error>> {
    let (candidate, evidence) = fixture()?;
    assert_eq!(
        promote_capability(&candidate, evidence, 10_000),
        Err(CapabilityPromotionError::AuthorityUnavailable)
    );
    Ok(())
}

#[test]
fn persisted_probe_json_cannot_be_relabelled_as_authority() -> Result<(), Box<dyn std::error::Error>>
{
    let (candidate, evidence) = fixture()?;
    let persisted = serde_json::to_string(&candidate)?;
    let replayed: CapabilityProbeCandidate = serde_json::from_str(&persisted)?;
    assert_eq!(
        promote_capability(&replayed, evidence, 10_000),
        Err(CapabilityPromotionError::AuthorityUnavailable)
    );
    Ok(())
}
