use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use venue_gateway_api::{GatewayMode, VenueId};
use venue_node::NodeLaunch;
use venue_runtime::WriterScope;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000041";
const VENUES: [(VenueId, &str); 6] = [
    (VenueId::Binance, "BTC/USDT"),
    (VenueId::Bitget, "BTC/USDT"),
    (VenueId::Bybit, "BTC/USDT"),
    (VenueId::Gate, "DOGE/USDT"),
    (VenueId::Hyperliquid, "BTC/USDC"),
    (VenueId::Okx, "BTC/USDT"),
];

#[derive(Serialize)]
struct LegacyHandoffFixture<'a> {
    schema_version: u16,
    scope_sha256: &'a str,
    scope: &'a WriterScope,
    canonical_artifacts_root: &'a str,
    canonical_root_sha256: &'a str,
    entry_sha256: &'a str,
}

#[derive(Serialize)]
struct LegacyEntryFixture<'a> {
    schema_version: u16,
    scope_sha256: &'a str,
    scope: &'a WriterScope,
    canonical_artifacts_root: &'a str,
    canonical_root_sha256: &'a str,
}

fn arguments(mode: &str, symbol: &str, handoff: Option<&Path>) -> Vec<OsString> {
    let mut arguments = vec![
        "venue-node-contract".into(),
        "--mode".into(),
        mode.into(),
        "--trading-account-id".into(),
        ACCOUNT.into(),
        "--symbol".into(),
        symbol.into(),
        "--artifacts-base".into(),
        std::env::temp_dir()
            .join("venue-gateway-candidate-contract")
            .into_os_string(),
    ];
    if let Some(handoff) = handoff {
        arguments.push("--legacy-v1-handoff".into());
        arguments.push(handoff.as_os_str().to_os_string());
    }
    arguments
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn legacy_registry_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    #[cfg(windows)]
    let local = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .ok_or("LOCALAPPDATA is required for the Windows Stage-7 registry fixture")?;
    #[cfg(windows)]
    let root = local.join("Venue");
    #[cfg(not(windows))]
    let root = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|value| PathBuf::from(value).join(".local/state")))
        .ok_or("XDG_STATE_HOME or HOME is required for the Stage-7 registry fixture")?
        .join("venue");
    Ok(root.join("stage7_writer_roots").join("v1"))
}

/// Creates the same durable v1 handoff shape that production validates: its scope digest names
/// both registry files, the handoff commits its canonical root, and Node rereads the exact file.
fn legacy_handoff(
    venue: VenueId,
    symbol: &str,
) -> Result<(tempfile::TempDir, PathBuf, PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let legacy_root = temporary.path().join("frozen-stage7");
    fs::create_dir_all(&legacy_root)?;
    let canonical_root = fs::canonicalize(&legacy_root)?;
    let canonical_root = canonical_root
        .to_str()
        .ok_or("legacy root has no UTF-8 representation")?
        .to_owned();
    let product_account = format!("stage7-{}", temporary.path().display());
    let scope = WriterScope {
        exchange: venue.as_str().to_owned(),
        account: product_account,
        symbol: symbol.parse()?,
        owner_scope: "hedged_grid_fixture".to_owned(),
    };
    let scope_sha256 = digest(&serde_json::to_vec(&scope)?);
    let registry = legacy_registry_root()?;
    fs::create_dir_all(&registry)?;
    let lock = registry.join(format!("{scope_sha256}.lock"));
    fs::write(&lock, [])?;
    let canonical_root_sha256 = digest(canonical_root.as_bytes());
    let entry_sha256 = digest(&serde_json::to_vec(&LegacyEntryFixture {
        schema_version: 1,
        scope_sha256: &scope_sha256,
        scope: &scope,
        canonical_artifacts_root: &canonical_root,
        canonical_root_sha256: &canonical_root_sha256,
    })?);
    let handoff = LegacyHandoffFixture {
        schema_version: 1,
        scope_sha256: &scope_sha256,
        scope: &scope,
        canonical_artifacts_root: &canonical_root,
        canonical_root_sha256: &canonical_root_sha256,
        entry_sha256: &entry_sha256,
    };
    let handoff_encoded = serde_json::to_vec(&handoff)?;
    let handoff_sha256 = digest(&handoff_encoded);
    fs::write(
        registry.join(format!("{scope_sha256}.json")),
        handoff_encoded,
    )?;
    let handoff_path = temporary.path().join("legacy-v1-handoff.json");
    let predecessor = venue_runtime::LegacyV1WriterPredecessor {
        exchange: venue,
        legacy_product_account: scope.account.clone(),
        legacy_symbol: scope.symbol.clone(),
        legacy_owner_scope: scope.owner_scope.clone(),
        legacy_artifacts_root: PathBuf::from(canonical_root),
        legacy_lock_sha256: scope_sha256,
        legacy_lock_path: lock.clone(),
        handoff_sha256,
    };
    fs::write(&handoff_path, serde_json::to_vec(&predecessor)?)?;
    Ok((temporary, handoff_path, registry, lock))
}

fn remove_legacy_registry_fixture(
    registry: PathBuf,
    lock: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let handoff = lock.with_extension("json");
    fs::remove_file(lock)?;
    fs::remove_file(handoff)?;
    let _ = fs::remove_dir(registry);
    Ok(())
}

#[test]
fn all_six_bindings_accept_only_exact_live() -> Result<(), Box<dyn std::error::Error>> {
    for (venue, symbol) in VENUES {
        let handoff = if matches!(venue, VenueId::Binance | VenueId::Gate | VenueId::Bitget) {
            Some(legacy_handoff(venue, symbol)?)
        } else {
            None
        };
        let live = NodeLaunch::try_parse_from(
            venue,
            arguments(
                "LIVE",
                symbol,
                handoff.as_ref().map(|value| value.1.as_path()),
            ),
        )?;

        assert_eq!(live.binding().venue, venue);
        assert_eq!(live.binding().mode, GatewayMode::Live);
        assert!(live.artifacts_root().ends_with(Path::new(ACCOUNT)));
        assert_eq!(live.legacy_v1_predecessor().is_some(), handoff.is_some());

        for invalid in ["TEST", "test", "live", "SHADOW", " LIVE "] {
            assert!(
                NodeLaunch::try_parse_from(
                    venue,
                    arguments(
                        invalid,
                        symbol,
                        handoff.as_ref().map(|value| value.1.as_path())
                    ),
                )
                .is_err()
            );
        }
        if let Some((_, _, registry, lock)) = handoff {
            remove_legacy_registry_fixture(registry, lock)?;
        }
    }
    Ok(())
}

#[test]
fn stage7_handoff_is_required_only_for_the_three_legacy_venues()
-> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, binance_handoff, registry, lock) =
        legacy_handoff(VenueId::Binance, "BTC/USDT")?;
    for (venue, symbol) in VENUES {
        let required = matches!(venue, VenueId::Binance | VenueId::Gate | VenueId::Bitget);
        assert_eq!(
            NodeLaunch::try_parse_from(venue, arguments("LIVE", symbol, None)).is_err(),
            required
        );
        assert_eq!(
            NodeLaunch::try_parse_from(venue, arguments("LIVE", symbol, Some(&binance_handoff)))
                .is_err(),
            venue != VenueId::Binance
        );
    }
    remove_legacy_registry_fixture(registry, lock)?;
    Ok(())
}
