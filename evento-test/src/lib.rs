//! Shared conformance test suite for evento [`Executor`] backends.
//!
//! Each public async function is one scenario, generic over the executor, so
//! every backend (`evento-sql`, `evento-fjall`, `evento-remote`, …) runs the
//! exact same behavioral checks. A backend's integration test simply calls
//! each scenario with its executor:
//!
//! ```rust,ignore
//! #[tokio::test]
//! async fn load() -> anyhow::Result<()> {
//!     evento_test::load(&my_executor().into()).await
//! }
//! ```

use std::collections::HashMap;

use bank::aggregator::{BankAccount, Created, MoneyDeposited, NameChanged};
use bank::{
    load_account_details, AccountDetailsView, AccountStatus, AccountType, ChangeOverdraftLimit,
    CloseAccount, DepositMoney, FreezeAccount, OpenAccount, ReceiveMoney, TransferMoney,
    UnfreezeAccount, WithdrawMoney,
};
use evento::cursor::{self, Order, ReadResult};
use evento::Event;
use evento::{cursor::Args, Aggregate, AggregateExt, EventFilter, Executor, ProjectionAggregate};
use rand::seq::IndexedRandom;
use rand::RngExt;
use ulid::Ulid;

async fn last_routing_key<E: Executor>(
    executor: &E,
    id: impl Into<String>,
) -> anyhow::Result<Option<String>> {
    let events = executor
        .read(
            Some([EventFilter::by_id::<BankAccount>(id)].into()),
            None,
            Args::backward(1, None),
            None,
        )
        .await?
        .edges;
    Ok(events.first().unwrap().node.routing_key.clone())
}

/// Scenario: committed events fold into the expected projection state on load.
pub async fn load<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());
    // Create first account (John) with initial balance
    let john_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_john".to_owned(),
            owner_name: "John Doe".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // Create second account (Jane) with different balance
    let jane_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_jane".to_owned(),
            owner_name: "Jane Smith".to_owned(),
            account_type: AccountType::Savings,
            currency: "EUR".to_owned(),
            initial_balance: 500,
        })
        .await?;

    // Load John's account and verify initial state
    let john = cmd
        .load(&john_id)
        .await?
        .expect("john account should exist");

    assert_eq!(john.balance, 1000);
    assert_eq!(john.aggregate_version()?, 1);
    assert!(john.is_active());

    // Load Jane's account and verify initial state
    let jane = cmd
        .load(&jane_id)
        .await?
        .expect("jane account should exist");

    assert_eq!(jane.balance, 500);
    assert_eq!(jane.aggregate_version()?, 1);
    assert!(jane.is_active());

    // Deposit money to John's account
    cmd.deposit_money(
        &john_id,
        DepositMoney {
            amount: 250,
            transaction_id: Ulid::generate().to_string(),
            description: "Salary deposit".to_owned(),
        },
    )
    .await?;

    // Reload John and verify updated balance and version
    let john = cmd
        .load(&john_id)
        .await?
        .expect("john account should exist");

    assert_eq!(john.balance, 1250);
    assert_eq!(john.aggregate_version()?, 2);

    // Transfer money from John to Jane
    let transaction_id = Ulid::generate().to_string();

    cmd.transfer_money(
        &john_id,
        TransferMoney {
            amount: 300,
            to_account_id: jane_id.clone(),
            transaction_id: transaction_id.clone(),
            description: "Payment to Jane".to_owned(),
        },
    )
    .await?;

    // Jane receives the money
    let jane = cmd
        .load(&jane_id)
        .await?
        .expect("jane account should exist");

    cmd.receive_money(
        &jane.id,
        ReceiveMoney {
            amount: 300,
            from_account_id: john_id.clone(),
            transaction_id,
            description: "Payment from John".to_owned(),
        },
    )
    .await?;

    // Verify final balances and versions
    let john = cmd
        .load(&john_id)
        .await?
        .expect("john account should exist");
    let jane = cmd
        .load(&jane_id)
        .await?
        .expect("jane account should exist");

    assert_eq!(john.balance, 950); // 1250 - 300
    assert_eq!(john.aggregate_version()?, 3); // AccountOpened + MoneyDeposited + MoneyTransferred
    assert_eq!(jane.balance, 800); // 500 + 300
    assert_eq!(jane.aggregate_version()?, 2); // AccountOpened + MoneyReceived

    // Verify non-existent account returns None
    let non_existent = cmd.load("non_existent_id").await?;
    assert!(non_existent.is_none());

    Ok(())
}

/// Scenario: a stream's routing key is locked at first write and preserved by
/// later commits.
pub async fn routing_key<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create account WITH routing key "us-east-1"
    let account_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: "owner1".to_owned(),
                owner_name: "Alice".to_owned(),
                account_type: AccountType::Checking,
                currency: "USD".to_owned(),
                initial_balance: 1000,
            },
            "us-east-1",
        )
        .await?;

    // Load and verify routing key and version
    let account = cmd.load(&account_id).await?.expect("account should exist");

    let routing_key = last_routing_key(executor, &account_id).await?;

    assert_eq!(routing_key, Some("us-east-1".to_owned()));
    assert_eq!(account.aggregate_version()?, 1);
    assert_eq!(account.balance, 1000);

    // Deposit money - routing key should be preserved from first event
    cmd.deposit_money(
        &account_id,
        DepositMoney {
            amount: 500,
            transaction_id: Ulid::generate().to_string(),
            description: "Deposit".to_owned(),
        },
    )
    .await?;

    // Reload and verify routing key is preserved and version incremented
    let account = cmd.load(&account_id).await?.expect("account should exist");

    let routing_key = last_routing_key(executor, &account_id).await?;

    assert_eq!(routing_key, Some("us-east-1".to_owned()));
    assert_eq!(account.aggregate_version()?, 2);
    assert_eq!(account.balance, 1500);

    // Create another account with different routing key "eu-west-1"
    let account2_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: "owner2".to_owned(),
                owner_name: "Bob".to_owned(),
                account_type: AccountType::Savings,
                currency: "EUR".to_owned(),
                initial_balance: 2000,
            },
            "eu-west-1",
        )
        .await?;

    let account2 = cmd
        .load(&account2_id)
        .await?
        .expect("account2 should exist");

    let routing_key = last_routing_key(executor, &account2_id).await?;
    assert_eq!(routing_key, Some("eu-west-1".to_owned()));
    assert_eq!(account2.aggregate_version()?, 1);

    // Create account WITHOUT routing key
    let account3_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner3".to_owned(),
            owner_name: "Charlie".to_owned(),
            account_type: AccountType::Business,
            currency: "GBP".to_owned(),
            initial_balance: 3000,
        })
        .await?;

    let account3 = cmd
        .load(&account3_id)
        .await?
        .expect("account3 should exist");

    let routing_key = last_routing_key(executor, &account3_id).await?;
    assert_eq!(routing_key, None);
    assert_eq!(account3.aggregate_version()?, 1);

    // Deposit to account without routing key - should remain None
    cmd.deposit_money(
        &account3_id,
        DepositMoney {
            amount: 100,
            transaction_id: Ulid::generate().to_string(),
            description: "Small deposit".to_owned(),
        },
    )
    .await?;

    let account3 = cmd
        .load(&account3_id)
        .await?
        .expect("account3 should exist");

    let routing_key = last_routing_key(executor, &account3_id).await?;
    assert_eq!(routing_key, None);
    assert_eq!(account3.aggregate_version()?, 2);
    assert_eq!(account3.balance, 3100);

    Ok(())
}

/// Scenario: loading a projection whose handlers span multiple aggregate types.
pub async fn load_multiple_aggregator<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create an Owner aggregate
    let owner_id = evento::create()
        .event(&Created {
            name: "John Doe".to_owned(),
        })
        .commit(executor)
        .await?;

    // Create a bank account with this owner
    let account_id = cmd
        .open_account(OpenAccount {
            owner_id: owner_id.clone(),
            owner_name: "John Doe".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // Load account details (should include owner info)
    let account = load_account_details(executor, &account_id, &owner_id)
        .await?
        .expect("account should exist");

    assert_eq!(account.balance, 1000);
    assert_eq!(account.owner_id, owner_id);
    assert_eq!(account.owner_name, "John Doe");

    // Deposit money
    cmd.deposit_money(
        &account_id,
        DepositMoney {
            amount: 500,
            transaction_id: Ulid::generate().to_string(),
            description: "Deposit".to_owned(),
        },
    )
    .await?;

    // Update owner name
    evento::append(&owner_id)
        .original_version(1)
        .event(&NameChanged {
            value: "John Smith".to_owned(),
        })
        .commit(executor)
        .await?;

    // Load account details again - should reflect both changes
    let account = load_account_details(executor, &account_id, &owner_id)
        .await?
        .expect("account should exist");

    // Verify BankAccount events were applied
    assert_eq!(account.balance, 1500); // 1000 + 500
    assert_eq!(account.available_balance, 1500);

    // Verify Owner::NameChanged was applied
    assert_eq!(account.owner_name, "John Smith");

    Ok(())
}

/// Scenario: loading a projection resumes from a stored snapshot plus trailing
/// events.
pub async fn load_with_snapshot<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create an account
    let account_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner1".to_owned(),
            owner_name: "John".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // Deposit money twice (version 2 and 3)
    cmd.deposit_money(
        &account_id,
        DepositMoney {
            amount: 200,
            transaction_id: Ulid::generate().to_string(),
            description: "Deposit 1".to_owned(),
        },
    )
    .await?;

    let data1 = cmd.load(&account_id).await?.unwrap();

    cmd.deposit_money(
        &account_id,
        DepositMoney {
            amount: 300,
            transaction_id: Ulid::generate().to_string(),
            description: "Deposit 2".to_owned(),
        },
    )
    .await?;

    let data2 = cmd.load(&account_id).await?.unwrap();

    // Now we have events: AccountOpened(v1), MoneyDeposited(v2), MoneyDeposited(v3)
    // Real balance should be: 1000 + 200 + 300 = 1500

    // Manually insert a "snapshot" at version 1 with balance 1000
    // This simulates a snapshot taken after AccountOpened
    {
        let mut rows = bank::BankAccount::snapshot_rows().write().unwrap();
        rows.insert(account_id.clone(), data1);
    }

    // Load - should restore from snapshot (version 1, balance 1000)
    // and apply events v2 and v3 (+200 +300)
    let account = cmd.load(&account_id).await?.unwrap();

    assert_eq!(account.balance, 1500); // 1000 (snapshot) + 200 + 300
    assert_eq!(account.aggregate_version()?, 3);

    // Test with a snapshot at version 2
    {
        let mut rows = bank::BankAccount::snapshot_rows().write().unwrap();
        rows.insert(account_id.clone(), data2);
    }

    // Load - should restore from snapshot (version 2, balance 1200)
    // and apply only event v3 (+300)
    let account = cmd.load(&account_id).await?.unwrap();

    assert_eq!(account.balance, 1500); // 1200 (snapshot) + 300
    assert_eq!(account.aggregate_version()?, 3);

    // Test with snapshot at latest version (no events to apply)
    {
        let mut rows = bank::BankAccount::snapshot_rows().write().unwrap();
        rows.insert(account_id.clone(), account.clone());
    }

    // Load - should restore from snapshot (version 3), no events to apply
    let account = cmd.load(&account_id).await?.unwrap();

    assert_eq!(account.balance, 1500);
    assert_eq!(account.aggregate_version()?, 3);

    Ok(())
}

/// Scenario: optimistic concurrency — a commit with a stale `original_version`
/// is rejected.
pub async fn invalid_original_version<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create an account
    let account_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner1".to_owned(),
            owner_name: "Alice".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // Load and verify initial version
    let account = cmd.load(&account_id).await?.expect("account should exist");
    assert_eq!(account.aggregate_version()?, 1);

    // First deposit commits successfully (version 1 -> 2)
    cmd.deposit_money(
        &account_id,
        DepositMoney {
            amount: 100,
            transaction_id: Ulid::generate().to_string(),
            description: "First deposit".to_owned(),
        },
    )
    .await?;

    // Verify first commit succeeded
    let account_after_first = cmd.load(&account_id).await?.expect("account should exist");
    assert_eq!(account_after_first.aggregate_version()?, 2);
    assert_eq!(account_after_first.balance, 1100);

    // Simulate a stale client trying to commit with version 1
    // This should fail because version is now 2
    let result = evento::append(&account_id)
        .original_version(1) // stale version
        .event(&MoneyDeposited {
            amount: 200,
            transaction_id: Ulid::generate().to_string(),
            description: "Second deposit (should fail)".to_owned(),
        })
        .commit(executor)
        .await;

    // Should get InvalidOriginalVersion error
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("invalid original version"),
        "Expected InvalidOriginalVersion error, got: {:?}",
        err
    );

    // Verify the second commit didn't go through - balance unchanged
    let account_final = cmd.load(&account_id).await?.expect("account should exist");
    assert_eq!(account_final.aggregate_version()?, 2);
    assert_eq!(account_final.balance, 1100); // Only first deposit counted

    Ok(())
}

/// Scenario: subscriber registration and worker-ownership tracking.
pub async fn subscriber_running<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let sub1 = simple::subscription().start(executor).await?;
    let sub2 = simple::subscription().start(executor).await?;

    assert!(
        !executor
            .is_subscriber_running("simple".to_owned(), sub1.id)
            .await?
    );
    assert!(
        executor
            .is_subscriber_running("simple".to_owned(), sub2.id)
            .await?
    );

    Ok(())
}

/// Scenario: a running subscription processes existing backlog and live writes.
pub async fn subscribe<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create first account (Alice)
    let alice_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_alice".to_owned(),
            owner_name: "Alice".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // Create second account (Bob)
    let bob_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_bob".to_owned(),
            owner_name: "Bob".to_owned(),
            account_type: AccountType::Savings,
            currency: "EUR".to_owned(),
            initial_balance: 500,
        })
        .await?;

    // Perform some operations
    cmd.deposit_money(
        &alice_id,
        DepositMoney {
            amount: 200,
            transaction_id: Ulid::generate().to_string(),
            description: "Deposit".to_owned(),
        },
    )
    .await?;

    let transaction_id = Ulid::generate().to_string();
    cmd.transfer_money(
        &alice_id,
        TransferMoney {
            amount: 300,
            to_account_id: bob_id.clone(),
            transaction_id: transaction_id.clone(),
            description: "Transfer to Bob".to_owned(),
        },
    )
    .await?;

    cmd.receive_money(
        &bob_id,
        ReceiveMoney {
            amount: 300,
            from_account_id: alice_id.clone(),
            transaction_id,
            description: "From Alice".to_owned(),
        },
    )
    .await?;

    // Remove only this test's accounts to simulate fresh projection state
    {
        let mut rows = simple::ROWS.write().unwrap();
        rows.remove(&alice_id);
        rows.remove(&bob_id);
    }

    // Verify our accounts are not in projection
    {
        let rows = simple::ROWS.read().unwrap();
        assert!(!rows.contains_key(&alice_id));
        assert!(!rows.contains_key(&bob_id));
    }

    // Run subscription to rebuild projection from events
    simple::subscription().no_retry().run_once(executor).await?;

    // Verify projection was rebuilt correctly
    let rows = simple::ROWS.read().unwrap();

    // Check Alice's account
    let alice_row = rows
        .get(&alice_id)
        .expect("Alice should exist in projection");
    assert_eq!(alice_row.status, AccountStatus::Active); // 1000 + 200 - 300

    // Check Bob's account
    let bob_row = rows.get(&bob_id).expect("Bob should exist in projection");
    assert_eq!(bob_row.status, AccountStatus::Active); // 500 + 300

    Ok(())
}

/// A running subscription must pick up a freshly written event almost
/// immediately, driven by the executor's in-process write signal rather than by
/// the poll interval.
///
/// The poll interval is set deliberately high (5s) so that polling alone cannot
/// explain a fast pickup — if the handler runs well under that, it is the
/// `write_watch` signal waking the loop. A regression to pure polling would make
/// this take ~5s and time out.
/// Scenario: writes wake a subscription without waiting out the poll interval.
pub async fn subscribe_low_latency<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    use std::time::{Duration, Instant};

    let subscription = simple::subscription()
        .poll_interval(Duration::from_secs(5))
        .start(executor)
        .await?;

    // Let the subscription reach its idle wait (first tick drains any backlog).
    tokio::time::sleep(Duration::from_millis(50)).await;

    let cmd = bank::Command(executor.clone());
    let started = Instant::now();
    let account_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_lowlat".to_owned(),
            owner_name: "Low Latency".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?;

    // Poll the projection until the handler has processed the new event.
    let elapsed = loop {
        if simple::ROWS.read().unwrap().contains_key(&account_id) {
            break started.elapsed();
        }
        if started.elapsed() >= Duration::from_secs(2) {
            subscription.shutdown().await.ok();
            anyhow::bail!(
                "event not processed within 2s despite a 5s poll interval: \
                 write signal did not wake the subscription"
            );
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    };

    // Far below the 5s poll interval — proves the wakeup came from the signal.
    assert!(
        elapsed < Duration::from_millis(500),
        "expected sub-500ms latency from the write signal, got {elapsed:?}"
    );

    {
        let rows = simple::ROWS.read().unwrap();
        assert_eq!(
            rows.get(&account_id)
                .expect("account should be in projection")
                .status,
            AccountStatus::Active
        );
    }

    subscription.shutdown().await.ok();

    Ok(())
}

/// Scenario: a subscription only receives events matching its routing key.
pub async fn subscribe_routing_key<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create account with routing key "us-east-1"
    let us_account_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: "owner_us".to_owned(),
                owner_name: "US User".to_owned(),
                account_type: AccountType::Checking,
                currency: "USD".to_owned(),
                initial_balance: 1000,
            },
            "us-east-1",
        )
        .await?;

    // Create account with routing key "eu-west-1"
    let eu_account_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: "owner_eu".to_owned(),
                owner_name: "EU User".to_owned(),
                account_type: AccountType::Checking,
                currency: "EUR".to_owned(),
                initial_balance: 2000,
            },
            "eu-west-1",
        )
        .await?;

    // Deposit to both accounts
    cmd.deposit_money(
        &us_account_id,
        DepositMoney {
            amount: 500,
            transaction_id: Ulid::generate().to_string(),
            description: "US deposit".to_owned(),
        },
    )
    .await?;

    cmd.deposit_money(
        &eu_account_id,
        DepositMoney {
            amount: 300,
            transaction_id: Ulid::generate().to_string(),
            description: "EU deposit".to_owned(),
        },
    )
    .await?;

    // Remove only this test's accounts from projection
    {
        let mut rows = simple::ROWS.write().unwrap();
        rows.remove(&us_account_id);
        rows.remove(&eu_account_id);
    }

    // Run subscription filtered by "us-east-1" routing key
    simple::subscription()
        .routing_key("us-east-1")
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify only US account was processed
    {
        let rows = simple::ROWS.read().unwrap();

        // US account should exist with correct balance
        let us_row = rows
            .get(&us_account_id)
            .expect("US account should exist in projection");
        assert_eq!(us_row.status, AccountStatus::Active); // 1000 + 500

        // EU account should NOT exist (not processed by this subscription)
        assert!(
            !rows.contains_key(&eu_account_id),
            "EU account should NOT be in projection (different routing key)"
        );
    }

    // Now run subscription filtered by "eu-west-1" routing key
    simple::subscription()
        .routing_key("eu-west-1")
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify EU account was now processed
    {
        let rows = simple::ROWS.read().unwrap();

        // EU account should now exist
        let eu_row = rows
            .get(&eu_account_id)
            .expect("EU account should exist in projection");
        assert_eq!(eu_row.status, AccountStatus::Active); // 2000 + 300
    }

    Ok(())
}

/// Scenario: subscription behavior when no explicit routing key is configured.
pub async fn subscribe_default<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create account WITHOUT routing key (default/None)
    let default_account_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_default".to_owned(),
            owner_name: "Default User".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // Create account WITH routing key
    let routed_account_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: "owner_routed".to_owned(),
                owner_name: "Routed User".to_owned(),
                account_type: AccountType::Checking,
                currency: "EUR".to_owned(),
                initial_balance: 2000,
            },
            "eu-west-1",
        )
        .await?;

    // Deposit to both accounts
    cmd.deposit_money(
        &default_account_id,
        DepositMoney {
            amount: 500,
            transaction_id: Ulid::generate().to_string(),
            description: "Default deposit".to_owned(),
        },
    )
    .await?;

    cmd.deposit_money(
        &routed_account_id,
        DepositMoney {
            amount: 300,
            transaction_id: Ulid::generate().to_string(),
            description: "Routed deposit".to_owned(),
        },
    )
    .await?;

    // Remove only this test's accounts from projection
    {
        let mut rows = simple::ROWS.write().unwrap();
        rows.remove(&default_account_id);
        rows.remove(&routed_account_id);
    }

    // Run default subscription (no routing key = processes events with routing_key IS NULL)
    simple::subscription().no_retry().run_once(executor).await?;

    // Verify only default (no routing key) account was processed
    {
        let rows = simple::ROWS.read().unwrap();

        // Default account should exist with correct balance
        let default_row = rows
            .get(&default_account_id)
            .expect("Default account should exist in projection");
        assert_eq!(default_row.status, AccountStatus::Active); // 1000 + 500

        // Routed account should NOT exist (has routing key, not processed by default subscription)
        assert!(
            !rows.contains_key(&routed_account_id),
            "Routed account should NOT be in projection (has routing key)"
        );
    }

    // Now run subscription with specific routing key
    simple::subscription()
        .routing_key("eu-west-1")
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify routed account was now processed
    {
        let rows = simple::ROWS.read().unwrap();

        // Routed account should now exist
        let routed_row = rows
            .get(&routed_account_id)
            .expect("Routed account should exist in projection");
        assert_eq!(routed_row.status, AccountStatus::Active); // 2000 + 300
    }

    Ok(())
}

/// Scenario: an executor-level default routing key is inherited by writes and
/// subscriptions.
pub async fn subscribe_default_routing_key<E: Executor + Clone>(
    executor: &E,
) -> anyhow::Result<()> {
    // Wrap the underlying executor in an Evento with a global default routing key.
    let evento = evento::Evento::new(executor.clone()).default_routing_key("default-region");
    let cmd = bank::Command(evento.clone());

    // (1) Writing without an explicit routing key should pick up the executor default.
    let default_account_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_default".to_owned(),
            owner_name: "Default User".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // (2) Writing with an explicit routing key should still win over the default.
    let other_account_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: "owner_other".to_owned(),
                owner_name: "Other User".to_owned(),
                account_type: AccountType::Checking,
                currency: "EUR".to_owned(),
                initial_balance: 2000,
            },
            "other-region",
        )
        .await?;

    assert_eq!(
        last_routing_key(&evento, &default_account_id).await?,
        Some("default-region".to_owned()),
        "events without explicit routing key should inherit the executor default"
    );
    assert_eq!(
        last_routing_key(&evento, &other_account_id).await?,
        Some("other-region".to_owned()),
        "explicit per-aggregator routing key must win over the executor default"
    );

    // Drop any state these accounts left behind so we can observe the subscription pass.
    {
        let mut rows = simple::ROWS.write().unwrap();
        rows.remove(&default_account_id);
        rows.remove(&other_account_id);
    }

    // (3) A subscription with neither .routing_key() nor .any_routing_key()
    // should inherit the executor default when started against the Evento
    // wrapper.
    simple::subscription().no_retry().run_once(&evento).await?;

    {
        let rows = simple::ROWS.read().unwrap();
        assert!(
            rows.contains_key(&default_account_id),
            "default-region account should be processed by the inherited subscription"
        );
        assert!(
            !rows.contains_key(&other_account_id),
            "other-region account should NOT be processed by the inherited subscription"
        );
    }

    // (4) An explicit .routing_key("other-region") on the SubscriptionBuilder still
    // overrides the executor default. Use a separate subscription key to keep its
    // cursor independent from the one above.
    simple_explicit::subscription("simple_other")
        .routing_key("other-region")
        .no_retry()
        .run_once(&evento)
        .await?;

    {
        let rows = simple_explicit::ROWS.read().unwrap();
        let other_row = rows.get(&other_account_id).expect(
            "other-region account should be processed when subscription explicitly opts in",
        );
        assert_eq!(other_row.status, AccountStatus::Active);
        assert!(
            !rows.contains_key(&default_account_id),
            "default-region account should NOT be processed by an explicit other-region subscription"
        );
    }

    Ok(())
}

/// Regression test for the bug where two Evento wrappers with different
/// `default_routing_key` values shared the same row in the subscriber table
/// when subscriptions called `.any_routing_key()`, so the second tenant
/// inherited the first tenant's cursor and never replayed.
///
/// The storage key for `.any_routing_key()` must include the executor's default
/// routing key as a prefix; otherwise tenant-b reads from where tenant-a
/// stopped.
/// Scenario: isolation between default-routing-key subscriptions and
/// all-events subscriptions.
pub async fn subscribe_default_routing_key_all_isolation<E: Executor + Clone>(
    executor: &E,
) -> anyhow::Result<()> {
    let evento_a = evento::Evento::new(executor.clone()).default_routing_key("tenant-a");
    let evento_b = evento::Evento::new(executor.clone()).default_routing_key("tenant-b");
    let cmd_a = bank::Command(evento_a.clone());
    let cmd_b = bank::Command(evento_b.clone());

    let acc_a = cmd_a
        .open_account(OpenAccount {
            owner_id: "owner_a".to_owned(),
            owner_name: "A".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?;
    let acc_b = cmd_b
        .open_account(OpenAccount {
            owner_id: "owner_b".to_owned(),
            owner_name: "B".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 200,
        })
        .await?;

    // Tenant-a runs .any_routing_key() — should process both events (it reads
    // every routing key).
    simple::subscription()
        .any_routing_key()
        .no_retry()
        .run_once(&evento_a)
        .await?;
    {
        let rows = simple::ROWS.read().unwrap();
        assert!(
            rows.contains_key(&acc_a),
            "tenant-a .any_routing_key() should process acc_a"
        );
        assert!(
            rows.contains_key(&acc_b),
            "tenant-a .any_routing_key() should process acc_b"
        );
    }

    // Wipe the projection so we can observe whether tenant-b actually re-reads
    // the events from its own cursor (which should start fresh).
    {
        let mut rows = simple::ROWS.write().unwrap();
        rows.remove(&acc_a);
        rows.remove(&acc_b);
    }

    // Tenant-b runs .any_routing_key() — with the fix, it has its own cursor
    // (storage key "tenant-b.simple" vs tenant-a's "tenant-a.simple") and
    // replays both events from the beginning.
    simple::subscription()
        .any_routing_key()
        .no_retry()
        .run_once(&evento_b)
        .await?;
    {
        let rows = simple::ROWS.read().unwrap();
        assert!(
            rows.contains_key(&acc_a),
            "tenant-b .any_routing_key() must replay acc_a — without per-tenant \
             cursor scoping it would inherit tenant-a's position"
        );
        assert!(
            rows.contains_key(&acc_b),
            "tenant-b .any_routing_key() must replay acc_b"
        );
    }

    Ok(())
}

/// Scenario: one subscription handling events from multiple aggregate types.
pub async fn subscribe_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create an Owner aggregate using evento::create()
    let owner_id = evento::create()
        .event(&Created {
            name: "John Doe".to_owned(),
        })
        .commit(executor)
        .await?;

    // Create a bank account with this owner
    let account_id = cmd
        .open_account(OpenAccount {
            owner_id: owner_id.clone(),
            owner_name: "John Doe".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // Deposit some money
    cmd.deposit_money(
        &account_id,
        DepositMoney {
            amount: 500,
            transaction_id: Ulid::generate().to_string(),
            description: "Deposit".to_owned(),
        },
    )
    .await?;

    // Update owner name using evento::append()
    evento::append(&owner_id)
        .original_version(1)
        .event(&NameChanged {
            value: "John Smith".to_owned(),
        })
        .commit(executor)
        .await?;

    // Remove this test's account from projection
    {
        let mut rows = AccountDetailsView::snapshot_rows().write().unwrap();
        rows.remove(&account_id);
    }

    // Run account_details subscription (handles both BankAccount and Owner events)
    multiple::subscription()
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify projection was rebuilt correctly with both aggregator types processed
    let rows = multiple::ROWS.read().unwrap();

    let account_row = rows
        .get(&account_id)
        .expect("Account should exist in projection");

    // Verify Owner::NameChanged event was processed (updates owner_name)
    assert_eq!(account_row.owner_name, "John Smith");

    Ok(())
}

/// A `Projection` whose handlers span the primary (`BankAccount`) and a
/// **co-keyed** secondary (`Owner`) aggregate — one whose events share the
/// primary's id. In subscription mode the worker must wake on the secondary's
/// events and scope them to `event.aggregate_id`; without that, an `Owner`
/// handler would read every `NameChanged` in the store and smear the last one
/// into every row.
mod co_keyed {
    use std::{collections::HashMap, sync::RwLock};

    use bank::aggregator::{AccountOpened, BankAccount, NameChanged};
    use evento::{metadata::Event, projection::Projection, Executor};
    use once_cell::sync::Lazy;

    // aggregate_id -> owner_name, written by the Owner handler as a subscription
    // applies co-keyed `NameChanged` events. Keyed by the unique account id so
    // concurrent backend tests don't collide.
    pub static ROWS: Lazy<RwLock<HashMap<String, String>>> = Lazy::new(Default::default);

    #[evento::projection(bitcode::Encode, bitcode::Decode)]
    pub struct View {
        pub id: String,
        pub owner_name: String,
    }

    // Sets identity only — deliberately does not touch `owner_name`, so the
    // co-keyed `NameChanged` is its sole writer and the result is independent of
    // the order the two co-keyed events (same id, both version 1) replay in.
    #[evento::handler]
    async fn on_account_opened(event: Event<AccountOpened>, view: &mut View) -> anyhow::Result<()> {
        view.id = event.aggregate_id.to_owned();
        Ok(())
    }

    #[evento::handler]
    async fn on_owner_name_changed(
        event: Event<NameChanged>,
        view: &mut View,
    ) -> anyhow::Result<()> {
        view.owner_name = event.data.value.to_owned();
        ROWS.write()
            .unwrap()
            .insert(event.aggregate_id.to_owned(), event.data.value);
        Ok(())
    }

    pub fn projection<E: Executor>() -> Projection<E, View> {
        Projection::new::<BankAccount>()
            .handler(on_account_opened())
            .handler(on_owner_name_changed())
    }
}

/// A projection subscription over a co-keyed secondary aggregate must scope that
/// aggregate to each event's id — not read every instance of its type.
/// Scenario: subscribing over a secondary aggregate co-keyed with the
/// projection id.
pub async fn subscribe_co_keyed_aggregator<E: Executor + Clone>(
    executor: &E,
) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Two accounts, each with an `Owner` stream co-keyed under the account's own
    // id (append the Owner `NameChanged` to the account id). Versions are tracked
    // per (aggregate_type, id), so this is independent of the account's version.
    let account_a = cmd
        .open_account(OpenAccount {
            owner_id: Ulid::generate().to_string(),
            owner_name: "A-at-open".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;
    evento::append(&account_a)
        .event(&NameChanged {
            value: "Alice".to_owned(),
        })
        .commit(executor)
        .await?;

    let account_b = cmd
        .open_account(OpenAccount {
            owner_id: Ulid::generate().to_string(),
            owner_name: "B-at-open".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 2000,
        })
        .await?;
    evento::append(&account_b)
        .event(&NameChanged {
            value: "Bob".to_owned(),
        })
        .commit(executor)
        .await?;

    // Co-keying is automatic: no `.aggregate::<Owner>()` call needed.
    co_keyed::projection()
        .subscription("co-keyed")
        .any_routing_key()
        .no_retry()
        .run_once(executor)
        .await?;

    // Copy the values out and drop the lock before asserting, so a failing
    // assert can't poison the shared lock for other concurrent tests.
    let (a, b) = {
        let rows = co_keyed::ROWS.read().unwrap();
        (rows.get(&account_a).cloned(), rows.get(&account_b).cloned())
    };
    assert_eq!(
        a.as_deref(),
        Some("Alice"),
        "account A must get its own co-keyed owner name"
    );
    assert_eq!(
        b.as_deref(),
        Some("Bob"),
        "co-keyed Owner must be scoped per id, not smeared across every account"
    );

    Ok(())
}

/// `.load()` auto-keys a secondary aggregate to the loaded id when it is not
/// registered with `.aggregate::<S>(id)` — scoping its events to that id rather
/// than reading every instance of the type.
/// Scenario: loading a projection over a secondary aggregate co-keyed with the
/// projection id.
pub async fn load_co_keyed_aggregator<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    let account_a = cmd
        .open_account(OpenAccount {
            owner_id: Ulid::generate().to_string(),
            owner_name: "A-at-open".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;
    evento::append(&account_a)
        .event(&NameChanged {
            value: "Alice".to_owned(),
        })
        .commit(executor)
        .await?;

    // A second account with its own co-keyed Owner rename — it must not leak
    // into account A's view.
    let account_b = cmd
        .open_account(OpenAccount {
            owner_id: Ulid::generate().to_string(),
            owner_name: "B-at-open".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 2000,
        })
        .await?;
    evento::append(&account_b)
        .event(&NameChanged {
            value: "Bob".to_owned(),
        })
        .commit(executor)
        .await?;

    // No `.aggregate::<Owner>(..)`: the Owner handler auto-keys to `account_a`.
    let view = co_keyed::projection()
        .load(&account_a)
        .execute(executor)
        .await?
        .expect("account A view");
    assert_eq!(
        view.owner_name, "Alice",
        "unregistered secondary must auto-key to the loaded id, not read every instance"
    );

    Ok(())
}

/// Scenario: routing-key filtering combined with handlers over multiple
/// aggregate types.
pub async fn subscribe_routing_key_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create Owner with routing key "us-east-1"
    let us_owner_id = evento::create()
        .routing_key("us-east-1")
        .event(&Created {
            name: "US Owner".to_owned(),
        })
        .commit(executor)
        .await?;

    // Create Owner with routing key "eu-west-1"
    let eu_owner_id = evento::create()
        .routing_key("eu-west-1")
        .event(&Created {
            name: "EU Owner".to_owned(),
        })
        .commit(executor)
        .await?;

    // Create bank account with routing key "us-east-1"
    let us_account_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: us_owner_id.clone(),
                owner_name: "US Owner".to_owned(),
                account_type: AccountType::Checking,
                currency: "USD".to_owned(),
                initial_balance: 1000,
            },
            "us-east-1",
        )
        .await?;

    // Create bank account with routing key "eu-west-1"
    let eu_account_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: eu_owner_id.clone(),
                owner_name: "EU Owner".to_owned(),
                account_type: AccountType::Checking,
                currency: "EUR".to_owned(),
                initial_balance: 2000,
            },
            "eu-west-1",
        )
        .await?;

    // Update US owner name
    evento::append(&us_owner_id)
        .original_version(1)
        .routing_key("us-east-1")
        .event(&NameChanged {
            value: "US Owner Updated".to_owned(),
        })
        .commit(executor)
        .await?;

    // Update EU owner name
    evento::append(&eu_owner_id)
        .original_version(1)
        .routing_key("eu-west-1")
        .event(&NameChanged {
            value: "EU Owner Updated".to_owned(),
        })
        .commit(executor)
        .await?;

    // Remove this test's accounts from projection
    {
        let mut rows = multiple::ROWS.write().unwrap();
        rows.remove(&us_account_id);
        rows.remove(&eu_account_id);
    }

    // Run subscription filtered by "us-east-1" routing key
    multiple::subscription()
        .routing_key("us-east-1")
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify only US account was processed
    {
        let rows = multiple::ROWS.read().unwrap();

        // US account should exist with updated owner name
        let us_row = rows
            .get(&us_account_id)
            .expect("US account should exist in projection");
        assert_eq!(us_row.owner_name, "US Owner Updated"); // NameChanged was processed

        // EU account should NOT exist (different routing key)
        assert!(
            !rows.contains_key(&eu_account_id),
            "EU account should NOT be in projection (different routing key)"
        );
    }

    // Now run subscription filtered by "eu-west-1" routing key
    multiple::subscription()
        .routing_key("eu-west-1")
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify EU account was now processed
    {
        let rows = multiple::ROWS.read().unwrap();

        let eu_row = rows
            .get(&eu_account_id)
            .expect("EU account should exist in projection");
        assert_eq!(eu_row.owner_name, "EU Owner Updated"); // NameChanged was processed
    }

    Ok(())
}

/// Scenario: default-key subscription with handlers over multiple aggregate
/// types.
pub async fn subscribe_default_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // Create Owner WITHOUT routing key (default)
    let default_owner_id = evento::create()
        .event(&Created {
            name: "Default Owner".to_owned(),
        })
        .commit(executor)
        .await?;

    // Create Owner WITH routing key
    let routed_owner_id = evento::create()
        .routing_key("eu-west-1")
        .event(&Created {
            name: "Routed Owner".to_owned(),
        })
        .commit(executor)
        .await?;

    // Create bank account WITHOUT routing key (default)
    let default_account_id = cmd
        .open_account(OpenAccount {
            owner_id: default_owner_id.clone(),
            owner_name: "Default Owner".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        })
        .await?;

    // Create bank account WITH routing key
    let routed_account_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: routed_owner_id.clone(),
                owner_name: "Routed Owner".to_owned(),
                account_type: AccountType::Checking,
                currency: "EUR".to_owned(),
                initial_balance: 2000,
            },
            "eu-west-1",
        )
        .await?;

    // Update default owner name (no routing key)
    evento::append(&default_owner_id)
        .original_version(1)
        .event(&NameChanged {
            value: "Default Owner Updated".to_owned(),
        })
        .commit(executor)
        .await?;

    // Update routed owner name (with routing key)
    evento::append(&routed_owner_id)
        .original_version(1)
        .routing_key("eu-west-1")
        .event(&NameChanged {
            value: "Routed Owner Updated".to_owned(),
        })
        .commit(executor)
        .await?;

    // Remove this test's accounts from projection
    {
        let mut rows = multiple::ROWS.write().unwrap();
        rows.remove(&default_account_id);
        rows.remove(&routed_account_id);
    }

    // Run default subscription (no routing key = processes events with routing_key IS NULL)
    multiple::subscription()
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify only default (no routing key) account was processed
    {
        let rows = multiple::ROWS.read().unwrap();

        // Default account should exist with updated owner name
        let default_row = rows
            .get(&default_account_id)
            .expect("Default account should exist in projection");
        assert_eq!(default_row.owner_name, "Default Owner Updated"); // NameChanged was processed

        // Routed account should NOT exist (has routing key)
        assert!(
            !rows.contains_key(&routed_account_id),
            "Routed account should NOT be in projection (has routing key)"
        );
    }

    // Now run subscription with specific routing key
    multiple::subscription()
        .routing_key("eu-west-1")
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify routed account was now processed
    {
        let rows = multiple::ROWS.read().unwrap();

        let routed_row = rows
            .get(&routed_account_id)
            .expect("Routed account should exist in projection");
        assert_eq!(routed_row.owner_name, "Routed Owner Updated"); // NameChanged was processed
    }

    Ok(())
}

/// Comprehensive test that exercises all Command operations, loads state, and runs subscription.
///
/// This test covers:
/// - OpenAccount (with and without routing key)
/// - DepositMoney
/// - WithdrawMoney
/// - TransferMoney / ReceiveMoney
/// - ChangeOverdraftLimit
/// - FreezeAccount / UnfreezeAccount
/// - CloseAccount
pub async fn all_commands<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    // =========================================================================
    // 1. Open two accounts
    // =========================================================================

    // Account A: Primary test account
    let account_a_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_a".to_owned(),
            owner_name: "Alice".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 5000,
        })
        .await?;

    // Account B: Secondary account for transfers (with routing key)
    let account_b_id = cmd
        .open_account_with_routing(
            OpenAccount {
                owner_id: "owner_b".to_owned(),
                owner_name: "Bob".to_owned(),
                account_type: AccountType::Savings,
                currency: "USD".to_owned(),
                initial_balance: 1000,
            },
            "region-1",
        )
        .await?;

    // Verify initial state
    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.balance, 5000);
    assert_eq!(account_a.aggregate_version()?, 1);
    assert!(account_a.is_active());

    let account_b = cmd
        .load(&account_b_id)
        .await?
        .expect("Account B should exist");
    assert_eq!(account_b.balance, 1000);

    // =========================================================================
    // 2. DepositMoney
    // =========================================================================

    cmd.deposit_money(
        &account_a_id,
        DepositMoney {
            amount: 2500,
            transaction_id: Ulid::generate().to_string(),
            description: "Salary deposit".to_owned(),
        },
    )
    .await?;

    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.balance, 7500); // 5000 + 2500
    assert_eq!(account_a.aggregate_version()?, 2);

    // =========================================================================
    // 3. WithdrawMoney
    // =========================================================================

    cmd.withdraw_money(
        &account_a_id,
        WithdrawMoney {
            amount: 500,
            transaction_id: Ulid::generate().to_string(),
            description: "ATM withdrawal".to_owned(),
        },
    )
    .await?;

    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.balance, 7000); // 7500 - 500
    assert_eq!(account_a.aggregate_version()?, 3);

    // =========================================================================
    // 4. ChangeOverdraftLimit
    // =========================================================================

    cmd.change_overdraft_limit(&account_a_id, ChangeOverdraftLimit { new_limit: 1000 })
        .await?;

    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.overdraft_limit, 1000);
    assert_eq!(account_a.aggregate_version()?, 4);

    // =========================================================================
    // 5. TransferMoney / ReceiveMoney
    // =========================================================================

    let transfer_tx_id = Ulid::generate().to_string();

    // Alice transfers to Bob
    cmd.transfer_money(
        &account_a_id,
        TransferMoney {
            amount: 2000,
            to_account_id: account_b_id.clone(),
            transaction_id: transfer_tx_id.clone(),
            description: "Payment to Bob".to_owned(),
        },
    )
    .await?;

    // Bob receives from Alice
    cmd.receive_money(
        &account_b_id,
        ReceiveMoney {
            amount: 2000,
            from_account_id: account_a_id.clone(),
            transaction_id: transfer_tx_id,
            description: "Payment from Alice".to_owned(),
        },
    )
    .await?;

    // Verify balances after transfer
    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    let account_b = cmd
        .load(&account_b_id)
        .await?
        .expect("Account B should exist");

    assert_eq!(account_a.balance, 5000); // 7000 - 2000
    assert_eq!(account_a.aggregate_version()?, 5);
    assert_eq!(account_b.balance, 3000); // 1000 + 2000
    assert_eq!(account_b.aggregate_version()?, 2);

    // =========================================================================
    // 6. FreezeAccount / UnfreezeAccount
    // =========================================================================

    cmd.freeze_account(
        &account_a_id,
        FreezeAccount {
            reason: "Suspicious activity detected".to_owned(),
        },
    )
    .await?;

    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    assert!(account_a.is_frozen());
    assert_eq!(account_a.aggregate_version()?, 6);

    // Try to withdraw while frozen - should fail
    let withdraw_result = cmd
        .withdraw_money(
            &account_a_id,
            WithdrawMoney {
                amount: 100,
                transaction_id: Ulid::generate().to_string(),
                description: "Should fail".to_owned(),
            },
        )
        .await;
    assert!(withdraw_result.is_err());

    // Unfreeze the account
    cmd.unfreeze_account(
        &account_a_id,
        UnfreezeAccount {
            reason: "Investigation complete".to_owned(),
        },
    )
    .await?;

    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    assert!(account_a.is_active());
    assert_eq!(account_a.aggregate_version()?, 7);

    // =========================================================================
    // 7. CloseAccount
    // =========================================================================

    // First, withdraw remaining balance to prepare for closure
    cmd.withdraw_money(
        &account_a_id,
        WithdrawMoney {
            amount: 5000,
            transaction_id: Ulid::generate().to_string(),
            description: "Final withdrawal before closure".to_owned(),
        },
    )
    .await?;

    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.balance, 0);
    assert_eq!(account_a.aggregate_version()?, 8);

    // Close the account
    cmd.close_account(
        &account_a_id,
        CloseAccount {
            reason: "Customer request".to_owned(),
        },
    )
    .await?;

    let account_a = cmd
        .load(&account_a_id)
        .await?
        .expect("Account A should exist");
    assert!(account_a.is_closed());
    assert_eq!(account_a.aggregate_version()?, 9);

    // Try operations on closed account - should fail
    let deposit_result = cmd
        .deposit_money(
            &account_a_id,
            DepositMoney {
                amount: 100,
                transaction_id: Ulid::generate().to_string(),
                description: "Should fail".to_owned(),
            },
        )
        .await;
    assert!(deposit_result.is_err());

    // =========================================================================
    // 8. Verify final state via subscription
    // =========================================================================

    // Clear projection state for our test accounts
    {
        let mut rows = simple::ROWS.write().unwrap();
        rows.remove(&account_a_id);
        rows.remove(&account_b_id);
    }

    // Run subscription to rebuild projection from events
    simple::subscription().no_retry().run_once(executor).await?;

    // Verify Account A projection
    {
        let rows = simple::ROWS.read().unwrap();

        let account_a_row = rows
            .get(&account_a_id)
            .expect("Account A should exist in projection");
        assert_eq!(account_a_row.status, AccountStatus::Closed);
    }

    // Run subscription with routing key for Account B
    simple::subscription()
        .routing_key("region-1")
        .no_retry()
        .run_once(executor)
        .await?;

    // Verify Account B projection
    {
        let rows = simple::ROWS.read().unwrap();

        let account_b_row = rows
            .get(&account_b_id)
            .expect("Account B should exist in projection");
        assert_eq!(account_b_row.status, AccountStatus::Active);
    }

    Ok(())
}

mod simple {
    use std::{collections::HashMap, sync::RwLock};

    use bank::aggregator::{AccountClosed, AccountFrozen, AccountOpened};
    use bank::AccountStatus;
    use evento::{
        metadata::Event,
        subscription::{Context, SubscriptionBuilder},
        Executor,
    };
    use once_cell::sync::Lazy;

    pub static ROWS: Lazy<RwLock<HashMap<String, Row>>> = Lazy::new(Default::default);

    #[derive(Default)]
    pub struct Row {
        pub status: AccountStatus,
    }

    pub fn subscription<E: Executor>() -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new("simple")
            .handler(handle_account_opened())
            .handler(handle_account_frozen())
            .handler(handle_account_closed())
    }

    #[evento::subscription]
    async fn handle_account_opened<E: Executor>(
        _context: &Context<'_, E>,
        event: Event<AccountOpened>,
    ) -> anyhow::Result<()> {
        let mut rows = ROWS.write().unwrap();
        rows.insert(
            event.aggregate_id.to_owned(),
            Row {
                status: AccountStatus::Active,
            },
        );

        Ok(())
    }

    #[evento::subscription]
    async fn handle_account_frozen<E: Executor>(
        _context: &Context<'_, E>,
        event: Event<AccountFrozen>,
    ) -> anyhow::Result<()> {
        let mut rows = ROWS.write().unwrap();
        rows.insert(
            event.aggregate_id.to_owned(),
            Row {
                status: AccountStatus::Frozen,
            },
        );

        Ok(())
    }

    #[evento::subscription]
    async fn handle_account_closed<E: Executor>(
        _context: &Context<'_, E>,
        event: Event<AccountClosed>,
    ) -> anyhow::Result<()> {
        let mut rows = ROWS.write().unwrap();
        rows.insert(
            event.aggregate_id.to_owned(),
            Row {
                status: AccountStatus::Closed,
            },
        );

        Ok(())
    }
}

mod simple_explicit {
    use std::{collections::HashMap, sync::RwLock};

    use bank::aggregator::AccountOpened;
    use bank::AccountStatus;
    use evento::{
        metadata::Event,
        subscription::{Context, SubscriptionBuilder},
        Executor,
    };
    use once_cell::sync::Lazy;

    pub static ROWS: Lazy<RwLock<HashMap<String, Row>>> = Lazy::new(Default::default);

    #[derive(Default)]
    pub struct Row {
        pub status: AccountStatus,
    }

    pub fn subscription<E: Executor>(key: impl Into<String>) -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new(key).handler(handle_account_opened())
    }

    #[evento::subscription]
    async fn handle_account_opened<E: Executor>(
        _context: &Context<'_, E>,
        event: Event<AccountOpened>,
    ) -> anyhow::Result<()> {
        let mut rows = ROWS.write().unwrap();
        rows.insert(
            event.aggregate_id.to_owned(),
            Row {
                status: AccountStatus::Active,
            },
        );

        Ok(())
    }
}

mod multiple {
    use std::{collections::HashMap, sync::RwLock};

    use bank::aggregator::{AccountFrozen, AccountOpened, NameChanged};
    use bank::AccountStatus;
    use evento::{
        metadata::Event,
        subscription::{Context, SubscriptionBuilder},
        Executor,
    };
    use once_cell::sync::Lazy;

    pub static ROWS: Lazy<RwLock<HashMap<String, Row>>> = Lazy::new(Default::default);

    #[derive(Default)]
    pub struct Row {
        pub owner_id: String,
        pub owner_name: String,
        pub status: AccountStatus,
    }

    pub fn subscription<E: Executor>() -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new("multiple")
            .handler(handle_account_opened())
            .handler(handle_account_frozen())
            .handler(handle_owned_name_chaged())
    }

    #[evento::subscription]
    async fn handle_account_opened<E: Executor>(
        _context: &Context<'_, E>,
        event: Event<AccountOpened>,
    ) -> anyhow::Result<()> {
        let mut rows = ROWS.write().unwrap();
        rows.insert(
            event.aggregate_id.to_owned(),
            Row {
                status: AccountStatus::Active,
                owner_name: event.data.owner_name,
                owner_id: event.data.owner_id,
            },
        );

        Ok(())
    }

    #[evento::subscription]
    async fn handle_account_frozen<E: Executor>(
        _context: &Context<'_, E>,
        event: Event<AccountFrozen>,
    ) -> anyhow::Result<()> {
        let mut rows = ROWS.write().unwrap();
        let row = rows.get_mut(&event.aggregate_id).unwrap();
        row.status = AccountStatus::Frozen;

        Ok(())
    }

    #[evento::subscription]
    async fn handle_owned_name_chaged<E: Executor>(
        _context: &Context<'_, E>,
        event: Event<NameChanged>,
    ) -> anyhow::Result<()> {
        let mut rows = ROWS.write().unwrap();
        for row in rows.values_mut() {
            if row.owner_id == event.aggregate_id {
                row.owner_name = event.data.value.to_owned();
            }
        }

        Ok(())
    }
}

/// Events must order by whole seconds, THEN sub-seconds, THEN version, THEN id —
/// matching the SQL `ORDER BY timestamp, timestamp_subsec, version, id`. This is a
/// regression guard against ordering by `timestamp_subsec` first (which reorders
/// events whose larger whole-second carries a smaller sub-second). The existing
/// suite cannot catch it because it always uses `timestamp_subsec: 0`.
/// Scenario: events read back in commit (timestamp/cursor) order.
pub async fn read_order_timestamp<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let agg_type = "evento/OrderTest";
    let mk = |t: u64, s: u32| Event {
        id: Ulid::generate(),
        aggregate_id: Ulid::generate().to_string(),
        aggregate_type: agg_type.to_owned(),
        version: 1,
        name: "Tick".to_owned(),
        routing_key: None,
        data: vec![],
        metadata: Default::default(),
        timestamp: t,
        timestamp_subsec: s,
    };
    // (100,500) < (101,100) < (101,900) < (102,0) by (timestamp, subsec).
    let e1 = mk(100, 500);
    let e2 = mk(101, 100);
    let e3 = mk(101, 900);
    let e4 = mk(102, 0);
    let expected = vec![e1.id, e2.id, e3.id, e4.id];
    // `replicate` persists the hand-crafted timestamps verbatim; `write` would
    // re-stamp them with the store's commit clock.
    executor
        .replicate(vec![e1.clone(), e2.clone(), e3.clone(), e4.clone()])
        .await?;

    let ids = |r: &ReadResult<Event>| r.edges.iter().map(|e| e.node.id).collect::<Vec<_>>();
    let read = |args| {
        let f = EventFilter::by_type_raw(agg_type);
        async move { executor.read(Some([f].into()), None, args, None).await }
    };

    // Full forward read.
    let all = read(Args::forward(10, None)).await?;
    assert_eq!(ids(&all), expected, "forward order must be timestamp-major");

    // Forward pagination across the boundary.
    let p1 = read(Args::forward(2, None)).await?;
    assert_eq!(ids(&p1), vec![expected[0], expected[1]]);
    assert!(p1.page_info.has_next_page);
    let p2 = read(Args::forward(2, p1.page_info.end_cursor.clone())).await?;
    assert_eq!(ids(&p2), vec![expected[2], expected[3]]);

    // Backward selects the last window but still yields ascending order (Relay-style).
    let back = read(Args::backward(2, None)).await?;
    assert_eq!(
        ids(&back),
        vec![expected[2], expected[3]],
        "backward(last=2) must return the last two events in ascending order"
    );

    Ok(())
}

/// `write` must replace client-supplied timestamps with the store's commit
/// clock (so cursor order tracks commit order), while `replicate` persists
/// them verbatim (for replication layers that own ordering).
/// Scenario: `write` re-stamps client-supplied timestamps with the store's
/// commit clock.
pub async fn write_restamps_client_clock<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let agg_type = "evento/RestampTest";
    let mk = |id: &str| Event {
        id: Ulid::generate(),
        aggregate_id: id.to_owned(),
        aggregate_type: agg_type.to_owned(),
        version: 1,
        name: "Stamped".to_owned(),
        routing_key: None,
        data: vec![],
        metadata: Default::default(),
        // A stale client stamp, long in the past.
        timestamp: 42,
        timestamp_subsec: 7,
    };

    executor.write(vec![mk("restamped")]).await?;
    executor.replicate(vec![mk("verbatim")]).await?;

    let restamped = executor
        .read(
            Some([EventFilter::by_id_raw(agg_type, "restamped")].into()),
            None,
            Args::forward(1, None),
            None,
        )
        .await?;
    assert!(
        restamped.edges[0].node.timestamp > 42,
        "write must replace the stale client stamp with the store's commit clock, got {}",
        restamped.edges[0].node.timestamp
    );

    let verbatim = executor
        .read(
            Some([EventFilter::by_id_raw(agg_type, "verbatim")].into()),
            None,
            Args::forward(1, None),
            None,
        )
        .await?;
    assert_eq!(verbatim.edges[0].node.timestamp, 42);
    assert_eq!(verbatim.edges[0].node.timestamp_subsec, 7);

    Ok(())
}

/// `EventFilter::exact_raw(type, id, name)` must return only events of that exact name
/// for the given aggregate. This exercises Fjall's `agg_name_index` fast path.
/// Scenario: `EventFilter::exact_raw` matches a single aggregate/event pair only.
pub async fn exact_filter<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let agg_type = "evento/FilterTest";
    let id = Ulid::generate().to_string();
    let mk = |v: u16, name: &str| Event {
        id: Ulid::generate(),
        aggregate_id: id.clone(),
        aggregate_type: agg_type.to_owned(),
        version: v,
        name: name.to_owned(),
        routing_key: None,
        data: vec![],
        metadata: Default::default(),
        timestamp: 100 + v as u64,
        timestamp_subsec: 0,
    };
    executor
        .write(vec![mk(1, "Alpha"), mk(2, "Beta"), mk(3, "Alpha")])
        .await?;

    let alpha = executor
        .read(
            Some([EventFilter::exact_raw(agg_type, &id, "Alpha")].into()),
            None,
            Args::forward(10, None),
            None,
        )
        .await?;
    assert_eq!(
        alpha.edges.len(),
        2,
        "exact filter must return both Alpha events"
    );
    assert!(alpha.edges.iter().all(|e| e.node.name == "Alpha"));

    let beta = executor
        .read(
            Some([EventFilter::exact_raw(agg_type, &id, "Beta")].into()),
            None,
            Args::forward(10, None),
            None,
        )
        .await?;
    assert_eq!(
        beta.edges.len(),
        1,
        "exact filter must return the single Beta event"
    );

    Ok(())
}

/// Concurrent appends at the same `original_version` must conflict: exactly one
/// wins, the rest get `InvalidOriginalVersion`. Guards optimistic concurrency under
/// real parallelism (the existing suite only tests it sequentially).
/// Scenario: concurrent appends to one stream serialize without losing events.
pub async fn concurrent_append<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create()
        .event(&MoneyDeposited {
            amount: 1,
            transaction_id: Ulid::generate().to_string(),
            description: "seed".to_owned(),
        })
        .commit(executor)
        .await?;

    let mut handles = Vec::new();
    for i in 0..8u32 {
        let ex = executor.clone();
        let id = id.clone();
        handles.push(tokio::spawn(async move {
            evento::append(&id)
                .original_version(1)
                .event(&MoneyDeposited {
                    amount: 10,
                    transaction_id: Ulid::generate().to_string(),
                    description: format!("concurrent-{i}"),
                })
                .commit(&ex)
                .await
        }));
    }

    let mut winners = 0;
    for h in handles {
        if h.await?.is_ok() {
            winners += 1;
        }
    }
    assert_eq!(
        winners, 1,
        "exactly one concurrent append at the same version must win"
    );

    assert_eq!(
        executor.original_version::<MoneyDeposited>(&id).await?,
        Some(2),
        "the aggregate must advance by exactly one version"
    );

    Ok(())
}

/// A `.strict()` subscription must fail when it encounters an event with no handler.
/// Scenario: a `strict()` subscription fails on an event without a handler.
pub async fn strict_unhandled<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());
    let id = cmd
        .open_account(OpenAccount {
            owner_id: "owner".to_owned(),
            owner_name: "Strict".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?;
    // MoneyDeposited has no handler in `simple` -> strict must reject it.
    cmd.deposit_money(
        &id,
        DepositMoney {
            amount: 10,
            transaction_id: Ulid::generate().to_string(),
            description: "d".to_owned(),
        },
    )
    .await?;

    let result = simple::subscription()
        .strict()
        .no_retry()
        .run_once(executor)
        .await;
    assert!(
        result.is_err(),
        "strict subscription must fail on an unhandled event"
    );

    Ok(())
}

/// A registered tombstone event must make projection load return `None`.
/// Scenario: tombstone handling for deleted aggregates.
pub async fn tombstone<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    use bank::aggregator::AccountClosed;

    let cmd = bank::Command(executor.clone());
    let id = cmd
        .open_account(OpenAccount {
            owner_id: "owner".to_owned(),
            owner_name: "Tomb".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?;

    // Without a tombstone the projection loads normally.
    let alive = feature::balance().load(&id).execute(executor).await?;
    assert!(alive.is_some(), "account should load before it is closed");

    cmd.close_account(
        &id,
        CloseAccount {
            reason: "closing".to_owned(),
        },
    )
    .await?; // emits AccountClosed

    // With a tombstone on AccountClosed, load returns None once the event exists.
    let dead = feature::balance()
        .tombstone::<AccountClosed>()
        .load(&id)
        .execute(executor)
        .await?;
    assert!(dead.is_none(), "tombstone event must make load return None");

    Ok(())
}

/// `#[subscription_all]` must observe every event of the aggregate regardless of type.
/// Scenario: an all-events subscription observes every event exactly once.
pub async fn subscription_all_counts<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::SeqCst;

    feature::ALL_COUNT.store(0, SeqCst);
    let cmd = bank::Command(executor.clone());
    let id = cmd
        .open_account(OpenAccount {
            owner_id: "owner".to_owned(),
            owner_name: "All".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?; // AccountOpened
    cmd.deposit_money(
        &id,
        DepositMoney {
            amount: 10,
            transaction_id: Ulid::generate().to_string(),
            description: "d".to_owned(),
        },
    )
    .await?; // MoneyDeposited

    feature::all_subscription()
        .no_retry()
        .run_once(executor)
        .await?;

    assert_eq!(
        feature::ALL_COUNT.load(SeqCst),
        2,
        "subscription_all must see every event regardless of type"
    );

    Ok(())
}

/// The executor snapshot contract: snapshots are scoped by revision (a revision
/// bump invalidates old snapshots), and delete removes them.
/// Scenario: stored snapshots are scoped to the projection revision that wrote
/// them.
pub async fn snapshot_revision_scope<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    use evento::cursor::Value;

    let agg = "evento/SnapTest";
    let view = "evento_test::SnapView";
    let id = "snap-1";
    let data = vec![1u8, 2, 3];

    executor
        .save_snapshot(
            agg.to_owned(),
            view.to_owned(),
            "0".to_owned(),
            id.to_owned(),
            data.clone(),
            Value(String::new()),
        )
        .await?;

    let same = executor
        .get_snapshot(
            agg.to_owned(),
            view.to_owned(),
            "0".to_owned(),
            id.to_owned(),
        )
        .await?;
    assert!(same.is_some(), "same revision must return the snapshot");
    assert_eq!(same.unwrap().0, data);

    let other = executor
        .get_snapshot(
            agg.to_owned(),
            view.to_owned(),
            "1".to_owned(),
            id.to_owned(),
        )
        .await?;
    assert!(
        other.is_none(),
        "a snapshot stored under a different revision must not be returned"
    );

    executor
        .delete_snapshot(agg.to_owned(), view.to_owned(), id.to_owned())
        .await?;
    assert!(
        executor
            .get_snapshot(
                agg.to_owned(),
                view.to_owned(),
                "0".to_owned(),
                id.to_owned()
            )
            .await?
            .is_none(),
        "delete_snapshot must remove the snapshot"
    );

    Ok(())
}

/// The executor snapshot contract: snapshots are keyed by projection name, so
/// several snapshotted projections of one aggregate never share a slot.
/// Scenario: stored snapshots are scoped to the projection that wrote them.
pub async fn snapshot_projection_scope<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    use evento::{cursor::Value, ProjectionCursor};

    // Executor level: same aggregate type, id and revision, two projections.
    let agg = "evento/SnapScopeTest";
    let id = "snap-scope-1";
    for (view, data) in [("view::A", vec![1u8]), ("view::B", vec![2u8, 2])] {
        executor
            .save_snapshot(
                agg.to_owned(),
                view.to_owned(),
                "0".to_owned(),
                id.to_owned(),
                data,
                Value(String::new()),
            )
            .await?;
    }

    let get = |view: &'static str| {
        executor.get_snapshot(
            agg.to_owned(),
            view.to_owned(),
            "0".to_owned(),
            id.to_owned(),
        )
    };
    assert_eq!(get("view::A").await?.map(|s| s.0), Some(vec![1u8]));
    assert_eq!(get("view::B").await?.map(|s| s.0), Some(vec![2u8, 2]));
    assert!(
        get("view::C").await?.is_none(),
        "a snapshot stored by another projection must not be returned"
    );

    executor
        .delete_snapshot(agg.to_owned(), "view::A".to_owned(), id.to_owned())
        .await?;
    assert!(get("view::A").await?.is_none());
    assert_eq!(
        get("view::B").await?.map(|s| s.0),
        Some(vec![2u8, 2]),
        "deleting one projection's snapshot must leave the others"
    );

    // End to end: two differently-shaped snapshotted views of one account.
    assert_ne!(
        proj_scope_a::View::projection_name(),
        proj_scope_b::View::projection_name(),
        "same-named structs in different modules must get different names"
    );

    let cmd = bank::Command(executor.clone());
    let id = cmd
        .open_account(OpenAccount {
            owner_id: "owner".to_owned(),
            owner_name: "Scope".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?;
    cmd.deposit_money(
        &id,
        DepositMoney {
            amount: 10,
            transaction_id: Ulid::generate().to_string(),
            description: "d".to_owned(),
        },
    )
    .await?;

    let mut a = proj_scope_a::view()
        .load(&id)
        .execute(executor)
        .await?
        .unwrap();
    let mut b = proj_scope_b::view()
        .load(&id)
        .execute(executor)
        .await?
        .unwrap();
    assert_eq!(a.amount, 110);
    assert_eq!((b.owner_name.as_str(), b.deposits), ("Scope", 1));

    // Store a marked snapshot for each at the cursor they reached: a load that
    // restores its own snapshot returns the mark, a rebuild would not. (Seeded
    // by hand because a backend with a stability watermark does not persist
    // fresh events yet.)
    let bank_account = BankAccount::aggregate_type().to_owned();
    a.amount = 9_999;
    b.owner_name = "Marked".to_owned();
    executor
        .save_snapshot(
            bank_account.clone(),
            proj_scope_a::View::projection_name().to_owned(),
            "0".to_owned(),
            id.clone(),
            bitcode::encode(&a),
            a.get_cursor(),
        )
        .await?;
    executor
        .save_snapshot(
            bank_account.clone(),
            proj_scope_b::View::projection_name().to_owned(),
            "0".to_owned(),
            id.clone(),
            bitcode::encode(&b),
            b.get_cursor(),
        )
        .await?;

    for _ in 0..2 {
        let a = proj_scope_a::view()
            .load(&id)
            .execute(executor)
            .await?
            .unwrap();
        let b = proj_scope_b::view()
            .load(&id)
            .execute(executor)
            .await?
            .unwrap();
        assert_eq!(a.amount, 9_999, "view A must restore its own snapshot");
        assert_eq!(
            (b.owner_name.as_str(), b.deposits),
            ("Marked", 1),
            "view B must restore its own snapshot"
        );
    }

    // A stored snapshot that does not decode is a cache miss, not an error.
    executor
        .save_snapshot(
            bank_account,
            proj_scope_a::View::projection_name().to_owned(),
            "0".to_owned(),
            id.clone(),
            vec![0xFF],
            a.get_cursor(),
        )
        .await?;
    let a = proj_scope_a::view()
        .load(&id)
        .execute(executor)
        .await?
        .unwrap();
    assert_eq!(
        a.amount, 110,
        "an unreadable snapshot must be rebuilt from events"
    );

    Ok(())
}

/// First of two same-named, differently-shaped snapshotted views of
/// `BankAccount` for [`snapshot_projection_scope`].
mod proj_scope_a {
    use bank::aggregator::{AccountOpened, BankAccount, MoneyDeposited};
    use evento::{metadata::Event, projection::Projection, Executor};

    #[evento::projection(bitcode::Encode, bitcode::Decode)]
    pub struct View {
        pub amount: i64,
    }

    #[evento::handler]
    async fn on_opened(event: Event<AccountOpened>, view: &mut View) -> anyhow::Result<()> {
        view.amount = event.data.initial_balance;
        Ok(())
    }

    #[evento::handler]
    async fn on_deposited(event: Event<MoneyDeposited>, view: &mut View) -> anyhow::Result<()> {
        view.amount += event.data.amount;
        Ok(())
    }

    pub fn view<E: Executor>() -> Projection<E, View> {
        Projection::new::<BankAccount>()
            .handler(on_opened())
            .handler(on_deposited())
    }
}

/// Second view for [`snapshot_projection_scope`]; see [`proj_scope_a`].
mod proj_scope_b {
    use bank::aggregator::{AccountOpened, BankAccount, MoneyDeposited};
    use evento::{metadata::Event, projection::Projection, Executor};

    #[evento::projection(bitcode::Encode, bitcode::Decode)]
    pub struct View {
        pub owner_name: String,
        pub deposits: u32,
    }

    #[evento::handler]
    async fn on_opened(event: Event<AccountOpened>, view: &mut View) -> anyhow::Result<()> {
        view.owner_name = event.data.owner_name.to_owned();
        Ok(())
    }

    #[evento::handler]
    async fn on_deposited(_event: Event<MoneyDeposited>, view: &mut View) -> anyhow::Result<()> {
        view.deposits += 1;
        Ok(())
    }

    pub fn view<E: Executor>() -> Projection<E, View> {
        Projection::new::<BankAccount>()
            .handler(on_opened())
            .handler(on_deposited())
    }
}

/// Commits one refund stream holding every generation of the event:
/// `Refunded` (10), `RefundedV2` (20, "v2"), `RefundedV3` (30, "v3", "ref").
async fn upcast_refund_stream<E: Executor>(executor: &E) -> anyhow::Result<String> {
    let id = evento::create()
        .event(&upcast::Refunded { amount: 10 })
        .event(&upcast::RefundedV2 {
            amount: 20,
            reason: "v2".to_owned(),
        })
        .event(&upcast::RefundedV3 {
            amount: 30,
            reason: "v3".to_owned(),
            reference: Some("ref".to_owned()),
        })
        .commit(executor)
        .await?;

    Ok(id)
}

/// Scenario: a projection with only the newest event's handler folds every
/// older generation, converted through the whole `upcast_to` chain — in lenient
/// mode (older names are added to the read filter) and in `strict()` mode.
pub async fn upcast_load<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = upcast_refund_stream(executor).await?;
    evento::append(&id)
        .original_version(3)
        .event(&upcast::Noted {
            text: "hello".to_owned(),
        })
        .commit(executor)
        .await?;

    for strict in [false, true] {
        let projection = upcast::latest_only();
        let projection = if strict {
            projection.strict()
        } else {
            projection
        };
        let view = projection.load(&id).execute(executor).await?.unwrap();

        assert_eq!(view.total, 60, "strict={strict}");
        assert_eq!(view.reasons, ["unknown", "v2", "v3"], "strict={strict}");
        assert_eq!(
            view.references,
            [None, None, Some("ref".to_owned())],
            "strict={strict}"
        );
        // Handlers see the newer event: name and payload are a consistent pair.
        assert_eq!(
            view.names,
            ["RefundedV3", "RefundedV3", "RefundedV3", "NotedV2"],
            "strict={strict}"
        );
        // `Noted` pins its stored name with `#[evento(name = "legacy.noted")]`.
        assert_eq!(view.notes, ["hello (anonymous)"], "strict={strict}");
        assert_eq!(view.aggregate_version, 4, "strict={strict}");
    }

    Ok(())
}

/// Scenario: with handlers for `V2` and `V3`, a `V1` event goes to the nearest
/// one (`V2`), whatever the registration order.
pub async fn upcast_nearest_target<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = upcast_refund_stream(executor).await?;

    for projection in [upcast::v2_then_v3(), upcast::v3_then_v2()] {
        let view = projection
            .strict()
            .load(&id)
            .execute(executor)
            .await?
            .unwrap();
        assert_eq!(view.total, 60);
        assert_eq!(view.names, ["RefundedV2", "RefundedV2", "RefundedV3"]);
        assert_eq!(view.reasons, ["unknown", "v2", "v3"]);
    }

    Ok(())
}

/// Scenario: a handler registered for the older event itself wins over the
/// upcast, so consumers can migrate one at a time.
pub async fn upcast_explicit_wins<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = upcast_refund_stream(executor).await?;

    let view = upcast::with_v1_handler()
        .strict()
        .load(&id)
        .execute(executor)
        .await?
        .unwrap();
    assert_eq!(view.names, ["Refunded", "RefundedV3", "RefundedV3"]);
    assert_eq!(view.reasons, ["v1-handler", "v2", "v3"]);

    Ok(())
}

/// Scenario: `.skip::<New>()` also skips the older events, without decoding
/// them (the stream below holds a `Refunded` whose bytes are not a `Refunded`).
pub async fn upcast_skip<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create()
        .event(&upcast::NotedV2 {
            text: "kept".to_owned(),
            author: "me".to_owned(),
        })
        .commit(executor)
        .await?;
    // Bytes of another shape under the old name: converting them would fail.
    let mut broken = executor
        .read(
            Some([EventFilter::by_id::<upcast::Refund>(&id)].into()),
            None,
            Args::forward(1, None),
            None,
        )
        .await?
        .edges
        .remove(0)
        .node;
    broken.id = Ulid::generate();
    broken.version = 2;
    broken.name = "Refunded".to_owned();
    broken.data = vec![0xFF];
    executor.replicate(vec![broken]).await?;

    let view = upcast::skip_refunds()
        .strict()
        .load(&id)
        .execute(executor)
        .await?
        .unwrap();
    assert_eq!(view.notes, ["kept (me)"]);
    assert_eq!(view.total, 0);

    // The same stream through a real handler surfaces the failed conversion.
    let err = upcast::latest_only().load(&id).execute(executor).await;
    assert!(
        err.is_err_and(|e| e.to_string().contains("failed to upcast `Refunded`")),
        "a payload that does not decode as the older event must fail the load"
    );

    Ok(())
}

/// Scenario: in `strict()` mode an older event whose target is neither
/// handled nor skipped still fails — upcasting never hides a missing consumer.
pub async fn upcast_strict_unhandled_target<E: Executor + Clone>(
    executor: &E,
) -> anyhow::Result<()> {
    let id = evento::create()
        .event(&upcast::Refunded { amount: 1 })
        .commit(executor)
        .await?;

    let result = upcast::notes_only()
        .strict()
        .load(&id)
        .execute(executor)
        .await;
    assert!(
        result.is_err_and(|e| e.to_string().contains("no handler")),
        "strict must reject an old event whose upcast target has no handler"
    );
    // Lenient: the unrelated event is simply not read.
    assert!(upcast::notes_only()
        .load(&id)
        .execute(executor)
        .await?
        .is_none());

    Ok(())
}

/// Scenario: a `SubscriptionBuilder` with only the newest handler receives the
/// older events upcast (`name` and `data` rewritten, everything else as
/// stored), while a `#[subscription_all]` handler sees them as stored.
pub async fn upcast_subscription<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create()
        .metadata("trace", &"abc".to_owned())
        .event(&upcast::Refunded { amount: 10 })
        .event(&upcast::RefundedV2 {
            amount: 20,
            reason: "v2".to_owned(),
        })
        .commit(executor)
        .await?;

    upcast::subscription("upcast-sub")
        .any_routing_key()
        .no_retry()
        .run_once(executor)
        .await?;
    // Strict: older names resolve to the registered handler, nothing is unhandled.
    upcast::subscription("upcast-sub-strict")
        .strict()
        .any_routing_key()
        .no_retry()
        .run_once(executor)
        .await?;

    let seen = upcast::SEEN
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .unwrap_or_default();
    assert_eq!(seen.len(), 4, "two events, two subscriptions");
    for row in &seen {
        assert_eq!(row.name, "RefundedV3");
        assert_eq!(
            row.trace.as_deref(),
            Some("abc"),
            "metadata must be preserved"
        );
    }
    let mut versions: Vec<_> = seen
        .iter()
        .map(|r| (r.version, r.amount, r.reason.clone()))
        .collect();
    versions.sort();
    versions.dedup();
    assert_eq!(
        versions,
        [(1, 10, "unknown".to_owned()), (2, 20, "v2".to_owned())],
        "payload upcast, version preserved"
    );

    upcast::raw_subscription("upcast-sub-raw")
        .any_routing_key()
        .no_retry()
        .run_once(executor)
        .await?;
    let raw = upcast::RAW
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        raw,
        ["Refunded", "RefundedV2"],
        "subscription_all must see stored events, not upcast ones"
    );

    Ok(())
}

/// Scenario: a projection subscription whose projection only handles the
/// newest event is woken by the older events too.
pub async fn upcast_projection_subscription<E: Executor + Clone>(
    executor: &E,
) -> anyhow::Result<()> {
    let id = evento::create()
        .event(&upcast::Refunded { amount: 7 })
        .commit(executor)
        .await?;

    upcast::recording()
        .subscription("upcast-projection")
        .any_routing_key()
        .no_retry()
        .run_once(executor)
        .await?;

    let total = upcast::TOTALS.read().unwrap().get(&id).copied();
    assert_eq!(
        total,
        Some(7),
        "an older event alone must trigger the projection's auto handler"
    );

    Ok(())
}

/// Scenario: `tombstone::<New>()` also fires on the older events that upcast
/// to it — on load, and in a projection subscription.
pub async fn upcast_tombstone<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create()
        .event(&upcast::Refunded { amount: 5 })
        .commit(executor)
        .await?;

    let alive = upcast::recording()
        .tombstone::<upcast::ClosedV2>()
        .load(&id)
        .execute(executor)
        .await?;
    assert!(alive.is_some());

    evento::append(&id)
        .original_version(1)
        .event(&upcast::Closed {
            by: "admin".to_owned(),
        })
        .commit(executor)
        .await?;

    let dead = upcast::recording()
        .tombstone::<upcast::ClosedV2>()
        .load(&id)
        .execute(executor)
        .await?;
    assert!(
        dead.is_none(),
        "an older tombstone event must make load return None"
    );

    // The subscription registers a tombstone handler per name without
    // colliding with the auto handlers.
    upcast::recording()
        .tombstone::<upcast::ClosedV2>()
        .subscription("upcast-tombstone")
        .any_routing_key()
        .no_retry()
        .run_once(executor)
        .await?;

    Ok(())
}

/// Scenario: `has_event::<New>()` is true for a stream that only holds an
/// older event upcasting to it.
pub async fn upcast_has_event<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = evento::create()
        .event(&upcast::Refunded { amount: 1 })
        .commit(executor)
        .await?;

    assert!(executor.has_event::<upcast::Refunded>(&id).await?);
    assert!(executor.has_event::<upcast::RefundedV2>(&id).await?);
    assert!(executor.has_event::<upcast::RefundedV3>(&id).await?);
    assert!(!executor.has_event::<upcast::NotedV2>(&id).await?);

    Ok(())
}

/// Aggregate with several generations of its events, plus the projections and
/// subscriptions used by the `upcast_*` scenarios.
mod upcast {
    use std::{collections::HashMap, sync::RwLock};

    use evento::{
        metadata::{Event, RawEvent},
        projection::Projection,
        subscription::{Context, SubscriptionBuilder},
        Executor,
    };
    use once_cell::sync::Lazy;

    #[evento::aggregate(name = "evento-test/Refund")]
    pub enum Refund {
        #[evento(upcast_to = RefundedV2)]
        Refunded {
            amount: i64,
        },
        #[evento(upcast_to = RefundedV3)]
        RefundedV2 {
            amount: i64,
            reason: String,
        },
        RefundedV3 {
            amount: i64,
            reason: String,
            reference: Option<String>,
        },

        #[evento(name = "legacy.noted", upcast_to = NotedV2)]
        Noted {
            text: String,
        },
        NotedV2 {
            text: String,
            author: String,
        },

        #[evento(upcast_to = ClosedV2)]
        Closed {
            by: String,
        },
        ClosedV2 {
            by: String,
            reason: String,
        },
    }

    impl From<Refunded> for RefundedV2 {
        fn from(old: Refunded) -> Self {
            Self {
                amount: old.amount,
                reason: "unknown".to_owned(),
            }
        }
    }

    impl From<RefundedV2> for RefundedV3 {
        fn from(old: RefundedV2) -> Self {
            Self {
                amount: old.amount,
                reason: old.reason,
                reference: None,
            }
        }
    }

    impl From<Noted> for NotedV2 {
        fn from(old: Noted) -> Self {
            Self {
                text: old.text,
                author: "anonymous".to_owned(),
            }
        }
    }

    impl From<Closed> for ClosedV2 {
        fn from(old: Closed) -> Self {
            Self {
                by: old.by,
                reason: "unspecified".to_owned(),
            }
        }
    }

    #[evento::projection]
    #[evento::snapshot(none)]
    pub struct View {
        pub total: i64,
        pub reasons: Vec<String>,
        pub references: Vec<Option<String>>,
        /// `event.name` as each handler saw it.
        pub names: Vec<String>,
        pub notes: Vec<String>,
    }

    #[evento::handler]
    async fn on_refunded(event: Event<Refunded>, view: &mut View) -> anyhow::Result<()> {
        view.total += event.data.amount;
        view.reasons.push("v1-handler".to_owned());
        view.names.push(event.name.to_owned());
        Ok(())
    }

    #[evento::handler]
    async fn on_refunded_v2(event: Event<RefundedV2>, view: &mut View) -> anyhow::Result<()> {
        view.total += event.data.amount;
        view.reasons.push(event.data.reason.to_owned());
        view.names.push(event.name.to_owned());
        Ok(())
    }

    #[evento::handler]
    async fn on_refunded_v3(event: Event<RefundedV3>, view: &mut View) -> anyhow::Result<()> {
        view.total += event.data.amount;
        view.reasons.push(event.data.reason.to_owned());
        view.references.push(event.data.reference.to_owned());
        view.names.push(event.name.to_owned());
        Ok(())
    }

    #[evento::handler]
    async fn on_noted_v2(event: Event<NotedV2>, view: &mut View) -> anyhow::Result<()> {
        view.notes
            .push(format!("{} ({})", event.data.text, event.data.author));
        view.names.push(event.name.to_owned());
        Ok(())
    }

    /// Only the newest generation of each event is handled.
    pub fn latest_only<E: Executor>() -> Projection<E, View> {
        Projection::new::<Refund>()
            .handler(on_refunded_v3())
            .handler(on_noted_v2())
    }

    pub fn v2_then_v3<E: Executor>() -> Projection<E, View> {
        Projection::new::<Refund>()
            .handler(on_refunded_v2())
            .handler(on_refunded_v3())
    }

    pub fn v3_then_v2<E: Executor>() -> Projection<E, View> {
        Projection::new::<Refund>()
            .handler(on_refunded_v3())
            .handler(on_refunded_v2())
    }

    pub fn with_v1_handler<E: Executor>() -> Projection<E, View> {
        Projection::new::<Refund>()
            .handler(on_refunded())
            .handler(on_refunded_v3())
    }

    pub fn skip_refunds<E: Executor>() -> Projection<E, View> {
        Projection::new::<Refund>()
            .skip::<RefundedV3>()
            .handler(on_noted_v2())
    }

    pub fn notes_only<E: Executor>() -> Projection<E, View> {
        Projection::new::<Refund>().handler(on_noted_v2())
    }

    // aggregate id -> total, written as a projection subscription folds events.
    pub static TOTALS: Lazy<RwLock<HashMap<String, i64>>> = Lazy::new(Default::default);

    #[evento::projection]
    #[evento::snapshot(none)]
    pub struct Recorded {
        pub total: i64,
    }

    #[evento::handler]
    async fn record_refunded_v3(
        event: Event<RefundedV3>,
        view: &mut Recorded,
    ) -> anyhow::Result<()> {
        view.total += event.data.amount;
        TOTALS
            .write()
            .unwrap()
            .insert(event.aggregate_id.to_owned(), view.total);
        Ok(())
    }

    pub fn recording<E: Executor>() -> Projection<E, Recorded> {
        Projection::new::<Refund>().handler(record_refunded_v3())
    }

    #[derive(Clone, Debug)]
    pub struct Seen {
        pub name: String,
        pub version: u16,
        pub amount: i64,
        pub reason: String,
        pub trace: Option<String>,
    }

    // aggregate id -> what the subscription handler received.
    pub static SEEN: Lazy<RwLock<HashMap<String, Vec<Seen>>>> = Lazy::new(Default::default);
    // aggregate id -> stored names a `subscription_all` handler received.
    pub static RAW: Lazy<RwLock<HashMap<String, Vec<String>>>> = Lazy::new(Default::default);

    #[evento::subscription]
    async fn see_refunded_v3<E: Executor>(
        _context: &Context<'_, E>,
        event: Event<RefundedV3>,
    ) -> anyhow::Result<()> {
        SEEN.write()
            .unwrap()
            .entry(event.aggregate_id.to_owned())
            .or_default()
            .push(Seen {
                name: event.name.to_owned(),
                version: event.version,
                amount: event.data.amount,
                reason: event.data.reason.to_owned(),
                trace: event.metadata.try_get::<String>("trace").ok(),
            });
        Ok(())
    }

    pub fn subscription<E: Executor>(key: &str) -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new(key)
            .handler(see_refunded_v3())
            .skip::<NotedV2>()
            .skip::<ClosedV2>()
    }

    #[evento::subscription_all]
    async fn see_raw<E: Executor>(
        _context: &Context<'_, E>,
        event: RawEvent<Refund>,
    ) -> anyhow::Result<()> {
        // `decode()` reconstructs the typed enum from the raw event. It is
        // verbatim: an old stored event decodes to its own variant, so the
        // name it reports must equal the name that was stored.
        let decoded = event.decode()?;
        assert_eq!(decoded.event_name(), event.name);

        RAW.write()
            .unwrap()
            .entry(event.aggregate_id.to_owned())
            .or_default()
            .push(event.name.to_owned());
        Ok(())
    }

    pub fn raw_subscription<E: Executor>(key: &str) -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new(key).handler(see_raw())
    }
}

/// Supporting projection + subscription_all handler for the feature tests above.
mod feature {
    use std::sync::atomic::AtomicU32;

    use bank::aggregator::{AccountOpened, BankAccount, MoneyDeposited};
    use evento::{
        metadata::{Event, RawEvent},
        projection::Projection,
        subscription::{Context, SubscriptionBuilder},
        Executor,
    };

    pub static ALL_COUNT: AtomicU32 = AtomicU32::new(0);

    #[evento::projection(bitcode::Encode, bitcode::Decode)]
    pub struct Balance {
        pub amount: i64,
    }

    #[evento::handler]
    async fn on_opened(event: Event<AccountOpened>, view: &mut Balance) -> anyhow::Result<()> {
        view.amount = event.data.initial_balance;
        Ok(())
    }

    #[evento::handler]
    async fn on_deposited(event: Event<MoneyDeposited>, view: &mut Balance) -> anyhow::Result<()> {
        view.amount += event.data.amount;
        Ok(())
    }

    pub fn balance<E: Executor>() -> Projection<E, Balance> {
        Projection::new::<BankAccount>()
            .handler(on_opened())
            .handler(on_deposited())
    }

    #[evento::subscription_all]
    async fn count_all<E: Executor>(
        _ctx: &Context<'_, E>,
        _event: RawEvent<BankAccount>,
    ) -> anyhow::Result<()> {
        ALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    pub fn all_subscription<E: Executor>() -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new("feature_all").handler(count_all())
    }
}

/// Asserts that a backend's paginated read matches the in-memory
/// [`cursor::Reader`] reference implementation for the same data and args.
pub fn assert_read_result(
    args: Args,
    order: Order,
    data: Vec<Event>,
    result: ReadResult<Event>,
) -> anyhow::Result<()> {
    let data = cursor::Reader::new(data)
        .args(args)
        .order(order)
        .execute()?;

    assert_eq!(result.page_info, data.page_info);
    assert_eq!(result.edges, data.edges);

    Ok(())
}

/// Generates a randomized set of events across several aggregates, types, and
/// routing keys for pagination tests.
pub fn get_data() -> Vec<Event> {
    let aggregator_ids = [
        Ulid::generate().to_string(),
        Ulid::generate().to_string(),
        Ulid::generate().to_string(),
        Ulid::generate().to_string(),
        Ulid::generate().to_string(),
    ];

    let aggregator_types = ["evento/Calcul", "evento/MyCalcul"];

    let routing_keys = [
        Some("us-east-1".to_owned()),
        Some("eu-west-3".to_owned()),
        None,
    ];

    let timestamps: Vec<u16> = vec![rand::random(), rand::random(), rand::random()];
    let mut versions: HashMap<String, u16> = HashMap::new();
    let mut data = vec![];

    for _ in 0..10 {
        let mut rng = rand::rng();
        let aggregate_id = aggregator_ids
            .choose(&mut rng)
            .cloned()
            .unwrap_or_else(|| Ulid::generate().to_string());

        let routing_key = routing_keys.choose(&mut rng).cloned().unwrap_or(None);
        let aggregate_type = aggregator_types
            .choose(&mut rng)
            .cloned()
            .unwrap_or("Calcul");
        let version = versions.entry(aggregate_id.to_owned()).or_default();
        let timestamp = if rng.random_range(0..100) < 20 {
            timestamps.choose(&mut rng).cloned()
        } else {
            None
        }
        .unwrap_or_else(|| rng.random()) as u64;

        let event = Event {
            id: Ulid::generate(),
            name: "MessageSent".to_owned(),
            aggregate_id,
            aggregate_type: aggregate_type.to_owned(),
            version: *version,
            routing_key,
            timestamp: timestamp as u64,
            timestamp_subsec: 0,
            data: Default::default(),
            metadata: Default::default(),
        };

        data.push(event);

        *version += 1;
    }

    data
}

/// Commits a stream of `count` deposits after an opening event, returning its id.
async fn seed_stream<E: Executor>(executor: &E, count: usize) -> anyhow::Result<String> {
    let id = evento::create()
        .event(&bank::aggregator::AccountOpened {
            owner_id: "owner-1".to_owned(),
            owner_name: "Alice".to_owned(),
            account_type: AccountType::Checking,
            currency: "EUR".to_owned(),
            initial_balance: 0,
        })
        .commit(executor)
        .await?;

    for i in 0..count {
        evento::append(&id)
            .original_version((i + 1) as u16)
            .event(&MoneyDeposited {
                amount: i as i64 + 1,
                transaction_id: format!("tx-{i}"),
                description: format!("deposit {i}"),
            })
            .commit(executor)
            .await?;
    }

    Ok(id)
}

/// `evento::read::<A>(id)` must return the aggregate's whole stream, and an
/// empty `Vec` — not an error — for an id that was never written.
///
/// Scenario: the reader returns every event of one aggregate instance.
pub async fn read_stream<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = seed_stream(executor, 3).await?;

    let events = evento::read::<BankAccount>(&id).execute(executor).await?;
    assert_eq!(events.len(), 4, "opening event plus three deposits");
    assert_eq!(
        events.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "a single stream must come back in version order"
    );
    assert_eq!(events[0].name, "AccountOpened");
    assert!(
        events.iter().all(|e| e.aggregate_id == id),
        "the reader must not leak events of other instances"
    );

    let missing = evento::read::<BankAccount>("does-not-exist")
        .execute(executor)
        .await?;
    assert!(
        missing.is_empty(),
        "an unknown aggregate is an empty stream, not an error"
    );

    // Narrowing to one event type keeps only that name.
    let deposits = evento::read::<BankAccount>(&id)
        .event::<MoneyDeposited>()
        .execute(executor)
        .await?;
    assert_eq!(deposits.len(), 3);
    assert!(deposits.iter().all(|e| e.name == "MoneyDeposited"));

    Ok(())
}

/// `execute()` must page through the whole stream, not stop at one page — the
/// regression guard for the internal drain loop (page size 100).
///
/// Scenario: the reader drains past its internal page size.
pub async fn read_drains_pages<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    // 1 opening event + 249 deposits = 250, comfortably over two pages of 100.
    let id = seed_stream(executor, 249).await?;

    let events = evento::read::<BankAccount>(&id).execute(executor).await?;
    assert_eq!(
        events.len(),
        250,
        "execute() must drain every page, not return only the first"
    );

    let versions = events.iter().map(|e| e.version).collect::<Vec<_>>();
    assert_eq!(
        versions,
        (1..=250).collect::<Vec<u16>>(),
        "draining must neither drop nor duplicate an event across page boundaries"
    );

    Ok(())
}

/// `limit(n)` caps the total number of events returned across all pages, and is
/// a no-op when it exceeds the stream length.
///
/// Scenario: `limit` bounds the total, not the page size.
pub async fn read_limit<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = seed_stream(executor, 4).await?;

    let capped = evento::read::<BankAccount>(&id)
        .limit(3)
        .execute(executor)
        .await?;
    assert_eq!(capped.len(), 3, "limit must cap the total");
    assert_eq!(
        capped.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let over = evento::read::<BankAccount>(&id)
        .limit(500)
        .execute(executor)
        .await?;
    assert_eq!(
        over.len(),
        5,
        "a limit above the stream length returns it all"
    );

    // A limit larger than one internal page still caps correctly.
    let long = seed_stream(executor, 149).await?;
    let partial = evento::read::<BankAccount>(&long)
        .limit(120)
        .execute(executor)
        .await?;
    assert_eq!(partial.len(), 120, "limit must cap across page boundaries");

    // `backward()` takes the tail of the stream, still oldest-first (GraphQL
    // `last:` semantics).
    let newest = evento::read::<BankAccount>(&id)
        .backward()
        .limit(2)
        .execute(executor)
        .await?;
    assert_eq!(
        newest.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![4, 5],
        "backward() must take the last events, in chronological order"
    );

    // Draining backward across several pages must stitch them back in order,
    // not emit page-sized blocks out of sequence.
    let tail = evento::read::<BankAccount>(&long)
        .backward()
        .limit(150)
        .execute(executor)
        .await?;
    assert_eq!(tail.len(), 150);
    assert_eq!(
        tail.iter().map(|e| e.version).collect::<Vec<_>>(),
        (1..=150).collect::<Vec<u16>>(),
        "a multi-page backward drain must stay chronological"
    );

    Ok(())
}

/// `page()` exposes the raw cursor surface: one page plus the `page_info` needed
/// to fetch the next, and `after(..)` must resume exactly where it left off.
///
/// Scenario: `page`/`after` paginate without gaps or duplicates.
pub async fn read_page_cursor<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let id = seed_stream(executor, 4).await?;

    let first = evento::read::<BankAccount>(&id)
        .limit(2)
        .page(executor)
        .await?;
    assert_eq!(first.edges.len(), 2, "limit is the page size for page()");
    assert!(first.page_info.has_next_page, "two of five events remain");

    let cursor = first
        .page_info
        .end_cursor
        .clone()
        .expect("a non-empty page must carry an end cursor");

    let second = evento::read::<BankAccount>(&id)
        .limit(2)
        .after(cursor.clone())
        .page(executor)
        .await?;
    assert_eq!(
        second
            .edges
            .iter()
            .map(|e| e.node.version)
            .collect::<Vec<_>>(),
        vec![3, 4],
        "after(end_cursor) must resume with no gap and no repeat"
    );

    // The same cursor also seeds a drain of the remainder.
    let rest = evento::read::<BankAccount>(&id)
        .after(cursor)
        .execute(executor)
        .await?;
    assert_eq!(
        rest.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![3, 4, 5],
        "execute() must honour a starting cursor"
    );

    Ok(())
}

/// `decode()` must yield the generated events enum, in stream order.
///
/// Scenario: the reader decodes into `{Enum}Event`.
pub async fn read_decode<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    use bank::aggregator::BankAccountEvent;

    let id = seed_stream(executor, 2).await?;

    let events = evento::read::<BankAccount>(&id).decode(executor).await?;
    assert_eq!(events.len(), 3);

    assert!(
        matches!(events[0], BankAccountEvent::AccountOpened(_)),
        "first event must decode to the opening variant"
    );

    let amounts = events
        .iter()
        .filter_map(|e| match e {
            BankAccountEvent::MoneyDeposited(d) => Some(d.amount),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(amounts, vec![1, 2], "payloads must decode in stream order");

    Ok(())
}

/// The reader's routing-key default is the inverse of a subscription's: with no
/// filter it reads **every** routing key, and `routing_key(..)` narrows it.
///
/// Scenario: routing keys filter the reader only when asked.
pub async fn read_routing_key<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let open = |owner: &str| bank::aggregator::AccountOpened {
        owner_id: owner.to_owned(),
        owner_name: owner.to_owned(),
        account_type: AccountType::Checking,
        currency: "EUR".to_owned(),
        initial_balance: 0,
    };

    let eu = evento::create()
        .routing_key("read-eu")
        .event(&open("eu"))
        .commit(executor)
        .await?;
    let us = evento::create()
        .routing_key("read-us")
        .event(&open("us"))
        .commit(executor)
        .await?;

    // Unset: every routing key is visible.
    assert_eq!(
        evento::read::<BankAccount>(&eu)
            .execute(executor)
            .await?
            .len(),
        1
    );
    assert_eq!(
        evento::read::<BankAccount>(&us)
            .execute(executor)
            .await?
            .len(),
        1
    );

    // Narrowed: the stream is invisible under the wrong key.
    assert_eq!(
        evento::read::<BankAccount>(&eu)
            .routing_key("read-eu")
            .execute(executor)
            .await?
            .len(),
        1,
        "the matching routing key must return the stream"
    );
    assert!(
        evento::read::<BankAccount>(&eu)
            .routing_key("read-us")
            .execute(executor)
            .await?
            .is_empty(),
        "a non-matching routing key must exclude the stream"
    );
    assert!(
        evento::read::<BankAccount>(&eu)
            .no_routing_key()
            .execute(executor)
            .await?
            .is_empty(),
        "no_routing_key() must match only events committed without a key"
    );

    Ok(())
}

/// Scenario: data registered with `.data(..)` is extractable in a handler under
/// the type it was registered with.
///
/// Guards the documented idiom (see `SubscriptionBuilder::data`): a bare value
/// comes back as `T`, a value registered as `Data<T>` comes back as `Data<T>`,
/// and a type that was never registered is a `try_extract` error rather than a
/// panic in the worker task.
pub async fn subscription_data<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let cmd = bank::Command(executor.clone());

    let account_id = cmd
        .open_account(OpenAccount {
            owner_id: "owner_ctx_data".to_owned(),
            owner_name: "Context Data".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 100,
        })
        .await?;

    context_data::subscription()
        .data(context_data::Smtp {
            host: "smtp.example.com".to_owned(),
        })
        .data(evento::context::Data::new(context_data::Templates {
            welcome: "welcome!".to_owned(),
        }))
        .no_retry()
        .run_once(executor)
        .await?;

    let rows = context_data::ROWS.read().unwrap();
    let row = rows
        .get(&account_id)
        .expect("the handler should have run for this account");
    assert_eq!(row, "smtp.example.com/welcome!");

    Ok(())
}

mod context_data {
    use std::{collections::HashMap, sync::RwLock};

    use bank::aggregator::AccountOpened;
    use evento::{
        context::Data,
        metadata::Event,
        subscription::{Context, SubscriptionBuilder},
        Executor,
    };
    use once_cell::sync::Lazy;

    pub static ROWS: Lazy<RwLock<HashMap<String, String>>> = Lazy::new(Default::default);

    /// Cheap to clone, so it is registered as itself.
    #[derive(Clone)]
    pub struct Smtp {
        pub host: String,
    }

    /// Not `Clone`, so it is registered wrapped in `Data`.
    pub struct Templates {
        pub welcome: String,
    }

    pub fn subscription<E: Executor>() -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new("context-data").handler(handle_account_opened())
    }

    #[evento::subscription]
    async fn handle_account_opened<E: Executor>(
        context: &Context<'_, E>,
        event: Event<AccountOpened>,
    ) -> anyhow::Result<()> {
        // A bare value is extracted under its own type.
        let smtp: Smtp = context.extract();

        // A value registered as `Data<T>` is extracted as `Data<T>`.
        let templates: Data<Templates> = context.try_extract()?;

        // A type that was never registered is an error, not a panic.
        assert!(
            context.try_extract::<u64>().is_err(),
            "an unregistered type must not extract"
        );

        ROWS.write().unwrap().insert(
            event.aggregate_id.to_owned(),
            format!("{}/{}", smtp.host, templates.welcome),
        );

        Ok(())
    }
}

/// Scenario: a subscription that dies takes the reason with it.
///
/// Guards issue #246: with `continue_on_error` unset a failing handler stops
/// the worker for good, and the handle is the only way to notice. The worker
/// also logs, but `tracing` is a no-op until the application installs a
/// subscriber — so `stopped()` is what a supervisor can actually rely on.
pub async fn subscription_stop_reason<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    use std::time::Duration;

    use evento::subscription::StopReason;

    let cmd = bank::Command(executor.clone());

    cmd.open_account(OpenAccount {
        owner_id: "owner_stop_reason".to_owned(),
        owner_name: "Stop Reason".to_owned(),
        account_type: AccountType::Checking,
        currency: "USD".to_owned(),
        initial_balance: 100,
    })
    .await?;

    // A handler that always fails, with no retries: the worker must stop and
    // report why.
    let failing = stop_reason::subscription(format!("stop-reason-failing-{}", Ulid::generate()))
        .no_retry()
        .start(executor)
        .await?;

    // Bounded so a regression fails instead of hanging the suite.
    let reason = tokio::time::timeout(Duration::from_secs(10), failing.stopped()).await?;

    assert!(
        matches!(reason, StopReason::Failed(_)),
        "expected the handler error to stop the worker, got {reason:?}"
    );
    assert!(reason.is_failure(), "{reason}");
    assert!(
        reason.to_string().contains(stop_reason::FAILURE),
        "the handler's message should survive: {reason}"
    );
    assert!(reason.error().is_some(), "{reason}");

    // The same reason is readable without blocking, and the handle now reports
    // itself as finished.
    assert!(failing.is_finished());
    assert!(matches!(failing.stop_reason(), Some(StopReason::Failed(_))));

    // A healthy subscription is not finished, and a requested stop is reported
    // as a shutdown rather than a failure.
    let healthy = stop_reason::healthy(format!("stop-reason-healthy-{}", Ulid::generate()))
        .start(executor)
        .await?;

    assert!(!healthy.is_finished());
    assert!(healthy.stop_reason().is_none());

    healthy.stop();
    let reason = tokio::time::timeout(Duration::from_secs(10), healthy.stopped()).await?;

    assert!(
        matches!(reason, StopReason::Shutdown),
        "expected a graceful shutdown, got {reason:?}"
    );
    assert!(!reason.is_failure(), "{reason}");

    healthy.shutdown().await?;

    Ok(())
}

mod stop_reason {
    use bank::aggregator::AccountOpened;
    use evento::{
        metadata::Event,
        subscription::{Context, SubscriptionBuilder},
        Executor,
    };

    /// The handler's error message, asserted on by the scenario.
    pub const FAILURE: &str = "boom, the handler failed";

    pub fn subscription<E: Executor>(key: impl Into<String>) -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new(key).handler(always_fails())
    }

    pub fn healthy<E: Executor>(key: impl Into<String>) -> SubscriptionBuilder<E> {
        SubscriptionBuilder::new(key).handler(never_fails())
    }

    #[evento::subscription]
    async fn always_fails<E: Executor>(
        _context: &Context<'_, E>,
        _event: Event<AccountOpened>,
    ) -> anyhow::Result<()> {
        anyhow::bail!(FAILURE)
    }

    #[evento::subscription]
    async fn never_fails<E: Executor>(
        _context: &Context<'_, E>,
        _event: Event<AccountOpened>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
