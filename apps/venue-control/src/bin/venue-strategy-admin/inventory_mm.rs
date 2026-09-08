use sqlx::PgPool;
use venue_control::{
    accounts::CredentialCipher,
    executor_secret::ExecutorSecretProvider,
    inventory_mm::{InventoryMmStore, signed_gate, signed_resume_gate},
    multi_venue_runtime::now_ms,
    private_projection::BinancePrivateProjectionStore,
};
use venue_control_protocol::inventory_mm::{
    InventoryMmAction, InventoryMmCreateRequest, InventoryMmLifecycleRequest,
    InventoryMmPreflightRequest,
};

/// Trusted operator transport only. Start uses the exact signed gate and locked lifecycle
/// operation used by Control; having database access is not a replacement for admission.
pub(super) async fn run(args: &[String], pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let owner = &args[1];
    let store = InventoryMmStore::new(pool.clone());
    match args[0].as_str() {
        "mm-create" => {
            let request: InventoryMmCreateRequest = super::input()?;
            // Request identity is already a canonical UUID and scoped by owner in the store.
            let instance = store
                .create(owner, &request.request_id, &request, now_ms()?)
                .await?;
            println!("{}", serde_json::to_string(&instance)?);
        }
        "mm-status" => {
            let instance = store.get(owner, &args[2]).await?;
            println!("{}", serde_json::to_string(&instance)?);
        }
        "mm-preflight" => {
            let request: InventoryMmPreflightRequest = super::input()?;
            let instance = store.get(owner, &request.instance_id).await?;
            let projections = BinancePrivateProjectionStore::new(pool.clone());
            projections
                .subscribe(
                    owner,
                    &instance.credential_id,
                    std::slice::from_ref(&instance.config.symbol),
                    now_ms()?,
                )
                .await?;
            let mut report = store
                .preflight(
                    owner,
                    &instance.instance_id,
                    request.expected_revision,
                    now_ms()?,
                )
                .await?;
            if report.ready {
                let secrets = ExecutorSecretProvider::new(
                    pool.clone(),
                    CredentialCipher::from_environment()?,
                );
                if signed_gate(pool, secrets, &instance).await.is_err() {
                    report
                        .blockers
                        .push("signed_leverage_or_margin_unavailable".into());
                }
            }
            report.ready = report.blockers.is_empty();
            report.checked_ms = now_ms()?;
            println!("{}", serde_json::to_string(&report)?);
        }
        "mm-lifecycle" => {
            let request: InventoryMmLifecycleRequest = super::input()?;
            let instance = store.get(owner, &request.instance_id).await?;
            let evidence = if matches!(
                request.action,
                InventoryMmAction::Start | InventoryMmAction::Resume
            ) {
                let secrets = ExecutorSecretProvider::new(
                    pool.clone(),
                    CredentialCipher::from_environment()?,
                );
                Some(if request.action == InventoryMmAction::Resume {
                    signed_resume_gate(pool, secrets, &instance).await?
                } else {
                    signed_gate(pool, secrets, &instance).await?
                })
            } else {
                None
            };
            let instance = store
                .lifecycle(owner, &request, evidence, now_ms()?)
                .await?;
            println!("{}", serde_json::to_string(&instance)?);
        }
        _ => return Err("unsupported inventory MM operation".into()),
    }
    Ok(())
}
