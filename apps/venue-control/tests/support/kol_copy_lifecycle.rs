use super::*;
use venue_control::BinanceCommandLedger;

pub(super) async fn seeded_copy(
    pool: &PgPool,
) -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let kol = id(501);
    let leader = id(502);
    let user = id(504);
    let account = id(505);
    let credential = id(506);
    let invite = id(507);
    let relation = id(508);
    seed_verified_account(pool, &kol, &leader, &id(503), 51).await?;
    seed_verified_account(pool, &user, &account, &credential, 52).await?;
    insert_kol_profile(pool, &kol, &leader, 1).await?;
    insert_invite(pool, &invite, &kol, 51).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)")
        .bind(&user).bind(&kol).bind(&invite).execute(pool).await?;
    insert_follow_relation(
        pool,
        &relation,
        &user,
        &kol,
        &leader,
        &account,
        &credential,
        1,
    )
    .await?;
    let fill = KolSourceFill {
        leader_trading_account_id: leader,
        native_symbol: "BTCUSDT".into(),
        native_trade_id: "lifecycle-1".into(),
        symbol: "BTC/USDT".into(),
        order_side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::new(1, 3),
        price: Decimal::from(50_000),
        occurred_ms: 10,
        observed_ms: 11,
        payload_digest: [51; 32],
    };
    let planned = PgExecutorStore::new(pool.clone())
        .record_source_fill_and_plan(&kol, &fill, 12)
        .await?;
    let command = planned
        .first()
        .ok_or("missing copy command")?
        .command_id
        .clone();
    Ok((relation, account, command))
}

#[tokio::test]
async fn paused_or_revised_copy_is_cancelled_before_either_claim_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    for batch in [false, true] {
        for retired in ["paused", "revision", "kol_disabled"] {
            let fixture = Fixture::create(&url).await?;
            fixture.migrate_twice().await?;
            let (relation, account, command) = seeded_copy(&fixture.pool).await?;
            match retired {
                "paused" => {
                    sqlx::query("UPDATE venue_kol_follow_relations SET relation_state='paused',active_slot=NULL WHERE relation_id=$1").bind(&relation).execute(&fixture.pool).await?;
                }
                "revision" => {
                    sqlx::query("UPDATE venue_kol_follow_relations SET revision=revision+1 WHERE relation_id=$1").bind(&relation).execute(&fixture.pool).await?;
                }
                _ => {
                    sqlx::query(
                        "UPDATE venue_kol_profiles SET profile_state='disabled',active_slot=NULL",
                    )
                    .execute(&fixture.pool)
                    .await?;
                }
            }
            let ledger = BinanceCommandLedger::new(fixture.pool.clone());
            if batch {
                assert!(ledger.claim_next_batch(&account, 20).await?.is_none());
            } else {
                assert!(ledger.claim_next(&account, 20).await?.is_none());
            }
            assert_eq!(command_state(&fixture.pool, &command).await?, "cancelled");
            fixture.cleanup().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn pause_lock_wins_against_a_concurrent_claim() -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let (relation, account, command) = seeded_copy(&fixture.pool).await?;
    let mut pause = fixture.pool.begin().await?;
    sqlx::query(
        "SELECT relation_id FROM venue_kol_follow_relations WHERE relation_id=$1 FOR UPDATE",
    )
    .bind(&relation)
    .fetch_one(&mut *pause)
    .await?;
    let ledger = BinanceCommandLedger::new(fixture.pool.clone());
    let claiming = tokio::spawn(async move { ledger.claim_next_batch(&account, 20).await });
    tokio::task::yield_now().await;
    assert!(!claiming.is_finished());
    sqlx::query("UPDATE venue_kol_follow_relations SET relation_state='paused',active_slot=NULL,revision=revision+1 WHERE relation_id=$1")
        .bind(&relation).execute(&mut *pause).await?;
    pause.commit().await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), claiming)
            .await???
            .is_none()
    );
    assert_eq!(command_state(&fixture.pool, &command).await?, "cancelled");
    fixture.cleanup().await?;
    Ok(())
}
