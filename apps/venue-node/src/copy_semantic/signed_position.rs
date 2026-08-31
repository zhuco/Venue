use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use venue_copy::{
    AuthoritativePositionSnapshot, FollowerDeliveryManifest, MAX_POSITION_SNAPSHOT_TTL_MS,
};
use venue_domain::domain::{Amount, Asset, PositionSide};
use venue_runtime::{SignedAccountPositionMode, SignedAccountSnapshot};

use super::CopySemanticError;

pub(super) fn position(
    manifest: &FollowerDeliveryManifest,
    snapshot: &SignedAccountSnapshot,
    now_ms: u64,
) -> Result<AuthoritativePositionSnapshot, CopySemanticError> {
    let binding = &manifest.binding;
    let expiry = snapshot
        .observed_at_ms()
        .checked_add(MAX_POSITION_SNAPSHOT_TTL_MS)
        .ok_or(CopySemanticError::ExecutionRequest)?;
    if snapshot.binding().trading_account_id != binding.account_id
        || snapshot.observed_at_ms() == 0
        || snapshot.private_generation() == 0
        || snapshot.observed_at_ms() > now_ms
        || now_ms >= expiry
    {
        return Err(CopySemanticError::ExecutionRequest);
    }
    let rows = snapshot
        .positions()
        .iter()
        .filter(|row| row.symbol == binding.instrument.symbol)
        .collect::<Vec<_>>();
    let valid_legs = match snapshot.position_mode() {
        SignedAccountPositionMode::Net => {
            rows.len() == 1 && rows[0].position_side == PositionSide::Net
        }
        SignedAccountPositionMode::Hedge => {
            rows.len() == 2
                && rows
                    .iter()
                    .any(|row| row.position_side == PositionSide::Long)
                && rows
                    .iter()
                    .any(|row| row.position_side == PositionSide::Short)
                && rows.iter().all(|row| row.quantity >= Decimal::ZERO)
        }
    };
    if !valid_legs || rows.iter().filter(|row| !row.quantity.is_zero()).count() > 1 {
        // A target with one signed direction cannot safely net away two nonzero hedge legs.
        return Err(CopySemanticError::ExecutionRequest);
    }
    let mut exposure = Decimal::ZERO;
    for row in &rows {
        if row.quantity.is_zero() {
            continue;
        }
        let mark = row
            .mark_price
            .filter(|mark| *mark > Decimal::ZERO)
            .ok_or(CopySemanticError::ExecutionRequest)?;
        let signed = if row.position_side == PositionSide::Short {
            -row.quantity
        } else {
            row.quantity
        };
        exposure = exposure
            .checked_add(
                signed
                    .checked_mul(mark)
                    .ok_or(CopySemanticError::ExecutionRequest)?,
            )
            .ok_or(CopySemanticError::ExecutionRequest)?;
    }
    let encoded = serde_json::to_vec(&(
        snapshot.binding(),
        snapshot.private_generation(),
        snapshot.observed_at_ms(),
        &rows,
    ))
    .map_err(|_| CopySemanticError::ExecutionRequest)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.copy.signed-position.v1");
    digest.update(encoded);
    Ok(AuthoritativePositionSnapshot {
        binding: binding.clone(),
        generation: snapshot.private_generation(),
        observed_at_ms: snapshot.observed_at_ms(),
        expires_at_ms: expiry,
        exposure: Amount::new(
            Asset::new(binding.instrument.symbol.quote())
                .map_err(|_| CopySemanticError::ExecutionRequest)?,
            exposure,
        ),
        fact_digest: digest.finalize().into(),
    })
}

#[cfg(test)]
mod tests {
    use venue_copy::CopyExecutionPhase;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
    use venue_runtime::SignedAccountPositionFact;

    use super::*;
    use crate::copy_semantic::tests::delivery_and_request;

    fn snapshot(
        manifest: &FollowerDeliveryManifest,
        mode: SignedAccountPositionMode,
        legs: &[(PositionSide, Decimal, Option<Decimal>)],
    ) -> Result<SignedAccountSnapshot, Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            manifest.binding.account_id.clone(),
            manifest.binding.instrument.symbol.clone(),
        )?;
        Ok(SignedAccountSnapshot::complete(
            binding,
            150,
            1,
            2,
            1,
            mode,
            Vec::new(),
            legs.iter()
                .map(|(side, quantity, mark)| SignedAccountPositionFact {
                    symbol: manifest.binding.instrument.symbol.clone(),
                    position_side: *side,
                    quantity: *quantity,
                    entry_price: None,
                    mark_price: *mark,
                })
                .collect(),
            "native-cursor".to_owned(),
            Vec::new(),
        )?)
    }

    #[test]
    fn net_short_preserves_sign_and_real_quote_asset() -> Result<(), Box<dyn std::error::Error>> {
        let (delivery, _) =
            delivery_and_request(Decimal::ZERO, 10.into(), CopyExecutionPhase::Adjust, 1)?;
        let mut manifest = delivery.manifest().clone();
        manifest.binding.instrument.symbol = "DOGE/USDC".parse()?;
        manifest.binding.instrument.settlement_asset = Some(Asset::new("USDC")?);
        let page = snapshot(
            &manifest,
            SignedAccountPositionMode::Net,
            &[(PositionSide::Net, (-3).into(), Some(2.into()))],
        )?;
        let position = position(&manifest, &page, 160)?;
        assert_eq!(position.exposure.value, Decimal::from(-6));
        assert_eq!(position.exposure.asset.as_str(), "USDC");
        Ok(())
    }

    #[test]
    fn hedge_requires_explicit_both_legs_and_does_not_net_two_nonzero_legs()
    -> Result<(), Box<dyn std::error::Error>> {
        let (delivery, _) =
            delivery_and_request(Decimal::ZERO, 10.into(), CopyExecutionPhase::Adjust, 1)?;
        for legs in [
            vec![],
            vec![(PositionSide::Long, Decimal::ZERO, None)],
            vec![
                (PositionSide::Long, 2.into(), Some(Decimal::ONE)),
                (PositionSide::Short, 2.into(), Some(Decimal::ONE)),
            ],
        ] {
            let page = snapshot(delivery.manifest(), SignedAccountPositionMode::Hedge, &legs)?;
            assert!(position(delivery.manifest(), &page, 160).is_err());
        }
        let page = snapshot(
            delivery.manifest(),
            SignedAccountPositionMode::Hedge,
            &[
                (PositionSide::Long, Decimal::ZERO, None),
                (PositionSide::Short, 3.into(), Some(2.into())),
            ],
        )?;
        assert_eq!(
            position(delivery.manifest(), &page, 160)?.exposure.value,
            Decimal::from(-6)
        );
        Ok(())
    }

    #[test]
    fn missing_mark_stale_future_or_wrong_account_cannot_prove_zero_or_exposure()
    -> Result<(), Box<dyn std::error::Error>> {
        let (delivery, _) =
            delivery_and_request(Decimal::ZERO, 10.into(), CopyExecutionPhase::Adjust, 1)?;
        let page = snapshot(
            delivery.manifest(),
            SignedAccountPositionMode::Net,
            &[(PositionSide::Net, Decimal::ONE, None)],
        )?;
        assert!(position(delivery.manifest(), &page, 160).is_err());
        let zero = snapshot(
            delivery.manifest(),
            SignedAccountPositionMode::Net,
            &[(PositionSide::Net, Decimal::ZERO, None)],
        )?;
        assert!(position(delivery.manifest(), &zero, 149).is_err());
        assert!(
            position(
                delivery.manifest(),
                &zero,
                150 + MAX_POSITION_SNAPSHOT_TTL_MS
            )
            .is_err()
        );
        let mut other = delivery.manifest().clone();
        other.binding.account_id = "00000000-0000-4000-8000-000000000002".to_owned();
        assert!(position(&other, &zero, 160).is_err());
        assert_eq!(
            position(delivery.manifest(), &zero, 160)?.exposure.value,
            Decimal::ZERO
        );
        Ok(())
    }
}
