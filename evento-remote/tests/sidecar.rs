//! `Sidecar`: events on a served store, subscription bookkeeping and snapshots
//! in a local store the client process owns. The conformance suite runs over
//! the combination, then the split itself is checked: nothing lands on the
//! server, a restart with the same local store resumes from its cursor.

use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use bank::account_balance;
use bank::aggregator::AccountOpened;
use bank::{AccountType, OpenAccount};
use evento::metadata::Event;
use evento::subscription::{Context, SubscriptionBuilder};
use evento::{Executor, ProjectionCursor, Sidecar};
use evento_fjall::Fjall;
use evento_remote::{serve, Client, ServerHandle};

type Replica = Sidecar<Client, Fjall>;

/// A served Fjall store, a client of it and a local Fjall store, combined.
struct TestServer {
    addr: std::net::SocketAddr,
    server: Fjall,
    local: Fjall,
    executor: Replica,
    _handle: ServerHandle,
}

async fn setup() -> anyhow::Result<TestServer> {
    let server = Fjall::temporary()?;
    let local = Fjall::temporary()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = serve(listener, server.clone());
    let client = Client::connect(addr).await?;
    Ok(TestServer {
        addr,
        executor: Sidecar::from((client, local.clone())),
        server,
        local,
        _handle: handle,
    })
}

macro_rules! conformance {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() -> anyhow::Result<()> {
            let server = setup().await?;
            evento_test::$name(&server.executor).await
        }
    )*};
}

conformance!(
    load,
    load_with_snapshot,
    subscriber_running,
    subscribe,
    subscribe_low_latency,
    subscribe_default_routing_key,
    subscription_stop_reason,
    subscription_start_from_latest,
    snapshot_projection_scope,
);

/// The accounts opened, as seen by the test subscription.
static OPENED: LazyLock<Mutex<Vec<String>>> = LazyLock::new(Mutex::default);

#[evento::subscription]
async fn on_account_opened<E: Executor>(
    _context: &Context<'_, E>,
    event: Event<AccountOpened>,
) -> anyhow::Result<()> {
    OPENED.lock().unwrap().push(event.aggregate_id.clone());
    Ok(())
}

fn subscription<E: Executor>(key: &str) -> SubscriptionBuilder<E> {
    SubscriptionBuilder::new(key).handler(on_account_opened())
}

async fn open_account(executor: &Replica, owner: &str) -> anyhow::Result<String> {
    Ok(bank::Command(executor.clone())
        .open_account(OpenAccount {
            owner_id: owner.to_owned(),
            owner_name: owner.to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?)
}

async fn eventually(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !ok() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn sidecar_keeps_bookkeeping_local() -> anyhow::Result<()> {
    let t = setup().await?;
    let key = "sidecar-bookkeeping";
    let running = subscription(key).start(&t.executor).await?;
    let id = open_account(&t.executor, "local-owner").await?;
    eventually("the subscription sees the account", || {
        OPENED.lock().unwrap().contains(&id)
    })
    .await;

    // The cursor is in the local store only.
    assert!(t
        .local
        .get_subscriber_cursor(key.to_owned())
        .await?
        .is_some());
    assert!(t
        .server
        .get_subscriber_cursor(key.to_owned())
        .await?
        .is_none());
    assert!(
        t.server
            .read(None, None, evento::cursor::Args::forward(10, None), None)
            .await?
            .edges
            .len()
            == 1,
        "the event itself is on the server"
    );

    // So is a projection's snapshot.
    let view = account_balance::create_projection()
        .load(&id)
        .execute(&t.executor)
        .await?
        .expect("the account is on the server");
    assert_eq!(view.balance, 100);
    async fn snapshot(
        store: &Fjall,
        id: &str,
    ) -> anyhow::Result<Option<(Vec<u8>, evento::cursor::Value)>> {
        store
            .get_snapshot(
                "bank/BankAccount".to_owned(),
                account_balance::AccountBalanceView::projection_name().to_owned(),
                "0".to_owned(),
                id.to_owned(),
            )
            .await
    }
    assert!(
        snapshot(&t.local, &id).await?.is_some(),
        "the snapshot is local"
    );
    assert!(
        snapshot(&t.server, &id).await?.is_none(),
        "nothing is written to the server"
    );

    running.shutdown().await.ok();
    Ok(())
}

#[tokio::test]
async fn sidecar_resumes_from_the_local_cursor() -> anyhow::Result<()> {
    let t = setup().await?;
    let key = "sidecar-resume";
    let first = subscription(key).start(&t.executor).await?;
    let a = open_account(&t.executor, "resume-a").await?;
    let b = open_account(&t.executor, "resume-b").await?;
    eventually("both accounts are seen", || {
        let seen = OPENED.lock().unwrap();
        seen.contains(&a) && seen.contains(&b)
    })
    .await;
    first.shutdown().await.ok();
    drop(t.executor);

    // A fresh client over the same local store: only what came after the
    // cursor is delivered.
    let restarted = Sidecar::from((Client::connect(t.addr).await?, t.local.clone()));
    OPENED.lock().unwrap().clear();
    let second = subscription(key).start(&restarted).await?;
    let c = open_account(&restarted, "resume-c").await?;
    eventually("the new account is seen", || {
        OPENED.lock().unwrap().contains(&c)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let seen = OPENED.lock().unwrap().clone();
    assert_eq!(seen, vec![c], "a and b were not replayed");

    second.shutdown().await.ok();
    Ok(())
}
