use bank::{
    account_details_subscription, load_account_details, AccountStatus, AccountType,
    ChangeOverdraftLimit, CloseAccount, Command, Created, DepositMoney, FreezeAccount, NameChanged,
    OpenAccount, ReceiveMoney, TransferMoney, UnfreezeAccount, WithdrawMoney, ACCOUNT_DETAILS_ROWS,
    COMMAND_ROWS,
};
use evento::{metadata::Metadata, Event, Executor};
use ulid::Ulid;

pub async fn load<E: Executor>(executor: &E) -> anyhow::Result<()> {
    // Create first account (John) with initial balance
    let john_id = Command::open_account(
        OpenAccount {
            owner_id: "owner_john".to_owned(),
            owner_name: "John Doe".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    // Create second account (Jane) with different balance
    let jane_id = Command::open_account(
        OpenAccount {
            owner_id: "owner_jane".to_owned(),
            owner_name: "Jane Smith".to_owned(),
            account_type: AccountType::Savings,
            currency: "EUR".to_owned(),
            initial_balance: 500,
        },
        executor,
    )
    .await?;

    // Load John's account and verify initial state
    let john = bank::load(executor, &john_id)
        .await?
        .expect("john account should exist");

    assert_eq!(john.balance, 1000);
    assert_eq!(john.event_version, 1);
    assert!(john.is_active());

    // Load Jane's account and verify initial state
    let jane = bank::load(executor, &jane_id)
        .await?
        .expect("jane account should exist");

    assert_eq!(jane.balance, 500);
    assert_eq!(jane.event_version, 1);
    assert!(jane.is_active());

    // Deposit money to John's account
    john.deposit_money(DepositMoney {
        amount: 250,
        transaction_id: Ulid::new().to_string(),
        description: "Salary deposit".to_owned(),
    })
    .await?;

    // Reload John and verify updated balance and version
    let john = bank::load(executor, &john_id)
        .await?
        .expect("john account should exist");

    assert_eq!(john.balance, 1250);
    assert_eq!(john.event_version, 2);

    // Transfer money from John to Jane
    let transaction_id = Ulid::new().to_string();

    john.transfer_money(TransferMoney {
        amount: 300,
        to_account_id: jane_id.clone(),
        transaction_id: transaction_id.clone(),
        description: "Payment to Jane".to_owned(),
    })
    .await?;

    // Jane receives the money
    let jane = bank::load(executor, &jane_id)
        .await?
        .expect("jane account should exist");

    jane.receive_money(ReceiveMoney {
        amount: 300,
        from_account_id: john_id.clone(),
        transaction_id,
        description: "Payment from John".to_owned(),
    })
    .await?;

    // Verify final balances and versions
    let john = bank::load(executor, &john_id)
        .await?
        .expect("john account should exist");
    let jane = bank::load(executor, &jane_id)
        .await?
        .expect("jane account should exist");

    assert_eq!(john.balance, 950); // 1250 - 300
    assert_eq!(john.event_version, 3); // AccountOpened + MoneyDeposited + MoneyTransferred
    assert_eq!(jane.balance, 800); // 500 + 300
    assert_eq!(jane.event_version, 2); // AccountOpened + MoneyReceived

    // Verify non-existent account returns None
    let non_existent = bank::load(executor, "non_existent_id").await?;
    assert!(non_existent.is_none());

    Ok(())
}

pub async fn routing_key<E: Executor>(executor: &E) -> anyhow::Result<()> {
    // Create account WITH routing key "us-east-1"
    let account_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: "owner1".to_owned(),
            owner_name: "Alice".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
        "us-east-1",
    )
    .await?;

    // Load and verify routing key and version
    let account = bank::load(executor, &account_id)
        .await?
        .expect("account should exist");

    assert_eq!(account.event_routing_key, Some("us-east-1".to_owned()));
    assert_eq!(account.event_version, 1);
    assert_eq!(account.balance, 1000);

    // Deposit money - routing key should be preserved from first event
    account
        .deposit_money(DepositMoney {
            amount: 500,
            transaction_id: Ulid::new().to_string(),
            description: "Deposit".to_owned(),
        })
        .await?;

    // Reload and verify routing key is preserved and version incremented
    let account = bank::load(executor, &account_id)
        .await?
        .expect("account should exist");

    assert_eq!(account.event_routing_key, Some("us-east-1".to_owned()));
    assert_eq!(account.event_version, 2);
    assert_eq!(account.balance, 1500);

    // Create another account with different routing key "eu-west-1"
    let account2_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: "owner2".to_owned(),
            owner_name: "Bob".to_owned(),
            account_type: AccountType::Savings,
            currency: "EUR".to_owned(),
            initial_balance: 2000,
        },
        executor,
        "eu-west-1",
    )
    .await?;

    let account2 = bank::load(executor, &account2_id)
        .await?
        .expect("account2 should exist");

    assert_eq!(account2.event_routing_key, Some("eu-west-1".to_owned()));
    assert_eq!(account2.event_version, 1);

    // Create account WITHOUT routing key
    let account3_id = Command::open_account(
        OpenAccount {
            owner_id: "owner3".to_owned(),
            owner_name: "Charlie".to_owned(),
            account_type: AccountType::Business,
            currency: "GBP".to_owned(),
            initial_balance: 3000,
        },
        executor,
    )
    .await?;

    let account3 = bank::load(executor, &account3_id)
        .await?
        .expect("account3 should exist");

    assert_eq!(account3.event_routing_key, None);
    assert_eq!(account3.event_version, 1);

    // Deposit to account without routing key - should remain None
    account3
        .deposit_money(DepositMoney {
            amount: 100,
            transaction_id: Ulid::new().to_string(),
            description: "Small deposit".to_owned(),
        })
        .await?;

    let account3 = bank::load(executor, &account3_id)
        .await?
        .expect("account3 should exist");

    assert_eq!(account3.event_routing_key, None);
    assert_eq!(account3.event_version, 2);
    assert_eq!(account3.balance, 3100);

    Ok(())
}

pub async fn load_multiple_aggregator<E: Executor>(executor: &E) -> anyhow::Result<()> {
    // Create an Owner aggregate
    let owner_id = evento::create()
        .event(&Created {
            name: "John Doe".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Create a bank account with this owner
    let account_id = Command::open_account(
        OpenAccount {
            owner_id: owner_id.clone(),
            owner_name: "John Doe".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    // Load account details (should include owner info)
    let account = load_account_details(executor, &account_id, &owner_id)
        .await?
        .expect("account should exist");

    assert_eq!(account.item.balance, 1000);
    assert_eq!(account.item.owner_id, owner_id);
    assert_eq!(account.item.owner_name, "John Doe");
    assert_eq!(account.version, 1);

    // Deposit money
    let bank_account = bank::load(executor, &account_id).await?.unwrap();
    bank_account
        .deposit_money(DepositMoney {
            amount: 500,
            transaction_id: Ulid::new().to_string(),
            description: "Deposit".to_owned(),
        })
        .await?;

    // Update owner name
    evento::aggregator(&owner_id)
        .original_version(1)
        .event(&NameChanged {
            value: "John Smith".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Load account details again - should reflect both changes
    let account = load_account_details(executor, &account_id, &owner_id)
        .await?
        .expect("account should exist");

    // Verify BankAccount events were applied
    assert_eq!(account.item.balance, 1500); // 1000 + 500
    assert_eq!(account.item.available_balance, 1500);

    // Verify Owner::NameChanged was applied
    assert_eq!(account.item.owner_name, "John Smith");

    // Version should be max of both aggregators' versions seen
    // BankAccount: AccountOpened(1) + MoneyDeposited(2) = version 2
    // Owner: Created(1) + NameChanged(2) = version 2
    // The projection tracks the last event's version it processed
    assert_eq!(account.version, 2);

    Ok(())
}

pub async fn load_with_snapshot<E: Executor>(executor: &E) -> anyhow::Result<()> {
    use bank::{AccountStatus, CommandData};

    // Create an account
    let account_id = Command::open_account(
        OpenAccount {
            owner_id: "owner1".to_owned(),
            owner_name: "John".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    // Deposit money twice (version 2 and 3)
    let account = bank::load(executor, &account_id).await?.unwrap();
    account
        .deposit_money(DepositMoney {
            amount: 200,
            transaction_id: Ulid::new().to_string(),
            description: "Deposit 1".to_owned(),
        })
        .await?;

    let account = bank::load(executor, &account_id).await?.unwrap();
    account
        .deposit_money(DepositMoney {
            amount: 300,
            transaction_id: Ulid::new().to_string(),
            description: "Deposit 2".to_owned(),
        })
        .await?;

    // Now we have events: AccountOpened(v1), MoneyDeposited(v2), MoneyDeposited(v3)
    // Real balance should be: 1000 + 200 + 300 = 1500

    // Manually insert a "snapshot" at version 1 with balance 1000
    // This simulates a snapshot taken after AccountOpened
    {
        let mut rows = COMMAND_ROWS.write().unwrap();
        rows.insert(
            account_id.clone(),
            (
                CommandData {
                    balance: 1000, // snapshot at version 1
                    status: AccountStatus::Active,
                    overdraft_limit: 0,
                },
                1,    // version 1
                None, // no routing key
            ),
        );
    }

    // Load - should restore from snapshot (version 1, balance 1000)
    // and apply events v2 and v3 (+200 +300)
    let account = bank::load(executor, &account_id).await?.unwrap();

    assert_eq!(account.balance, 1500); // 1000 (snapshot) + 200 + 300
    assert_eq!(account.event_version, 3);

    // Test with a snapshot at version 2
    {
        let mut rows = COMMAND_ROWS.write().unwrap();
        rows.insert(
            account_id.clone(),
            (
                CommandData {
                    balance: 1200, // snapshot at version 2 (1000 + 200)
                    status: AccountStatus::Active,
                    overdraft_limit: 0,
                },
                2,    // version 2
                None, // no routing key
            ),
        );
    }

    // Load - should restore from snapshot (version 2, balance 1200)
    // and apply only event v3 (+300)
    let account = bank::load(executor, &account_id).await?.unwrap();

    assert_eq!(account.balance, 1500); // 1200 (snapshot) + 300
    assert_eq!(account.event_version, 3);

    // Test with snapshot at latest version (no events to apply)
    {
        let mut rows = COMMAND_ROWS.write().unwrap();
        rows.insert(
            account_id.clone(),
            (
                CommandData {
                    balance: 1500, // snapshot at version 3 (full state)
                    status: AccountStatus::Active,
                    overdraft_limit: 0,
                },
                3,    // version 3
                None, // no routing key
            ),
        );
    }

    // Load - should restore from snapshot (version 3), no events to apply
    let account = bank::load(executor, &account_id).await?.unwrap();

    assert_eq!(account.balance, 1500);
    assert_eq!(account.event_version, 3);

    Ok(())
}

pub async fn invalid_original_version<E: Executor>(executor: &E) -> anyhow::Result<()> {
    // Create an account
    let account_id = Command::open_account(
        OpenAccount {
            owner_id: "owner1".to_owned(),
            owner_name: "Alice".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    // Load the account twice (simulating concurrent access)
    let account_v1_first = bank::load(executor, &account_id)
        .await?
        .expect("account should exist");

    let account_v1_second = bank::load(executor, &account_id)
        .await?
        .expect("account should exist");

    // Both have version 1
    assert_eq!(account_v1_first.event_version, 1);
    assert_eq!(account_v1_second.event_version, 1);

    // First load commits successfully (version 1 -> 2)
    account_v1_first
        .deposit_money(DepositMoney {
            amount: 100,
            transaction_id: Ulid::new().to_string(),
            description: "First deposit".to_owned(),
        })
        .await?;

    // Verify first commit succeeded
    let account_after_first = bank::load(executor, &account_id)
        .await?
        .expect("account should exist");
    assert_eq!(account_after_first.event_version, 2);
    assert_eq!(account_after_first.balance, 1100);

    // Second load tries to commit with stale version 1
    // This should fail because version is now 2
    let result = account_v1_second
        .deposit_money(DepositMoney {
            amount: 200,
            transaction_id: Ulid::new().to_string(),
            description: "Second deposit (should fail)".to_owned(),
        })
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
    let account_final = bank::load(executor, &account_id)
        .await?
        .expect("account should exist");
    assert_eq!(account_final.event_version, 2);
    assert_eq!(account_final.balance, 1100); // Only first deposit counted

    Ok(())
}

pub async fn subscriber_running<E: Executor + Clone>(executor: &E) -> anyhow::Result<()> {
    let sub1 = bank::subscription().start(executor).await?;
    let sub2 = bank::subscription().start(executor).await?;

    assert!(
        !executor
            .is_subscriber_running("command".to_owned(), sub1.id)
            .await?
    );
    assert!(
        executor
            .is_subscriber_running("command".to_owned(), sub2.id)
            .await?
    );

    Ok(())
}

pub async fn subscribe<E: Executor + Clone>(
    executor: &E,
    _events: Vec<Event>,
) -> anyhow::Result<()> {
    // Create first account (Alice)
    let alice_id = Command::open_account(
        OpenAccount {
            owner_id: "owner_alice".to_owned(),
            owner_name: "Alice".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    // Create second account (Bob)
    let bob_id = Command::open_account(
        OpenAccount {
            owner_id: "owner_bob".to_owned(),
            owner_name: "Bob".to_owned(),
            account_type: AccountType::Savings,
            currency: "EUR".to_owned(),
            initial_balance: 500,
        },
        executor,
    )
    .await?;

    // Perform some operations
    let alice = bank::load(executor, &alice_id).await?.unwrap();
    alice
        .deposit_money(DepositMoney {
            amount: 200,
            transaction_id: Ulid::new().to_string(),
            description: "Deposit".to_owned(),
        })
        .await?;

    let alice = bank::load(executor, &alice_id).await?.unwrap();
    let transaction_id = Ulid::new().to_string();
    alice
        .transfer_money(TransferMoney {
            amount: 300,
            to_account_id: bob_id.clone(),
            transaction_id: transaction_id.clone(),
            description: "Transfer to Bob".to_owned(),
        })
        .await?;

    let bob = bank::load(executor, &bob_id).await?.unwrap();
    bob.receive_money(ReceiveMoney {
        amount: 300,
        from_account_id: alice_id.clone(),
        transaction_id,
        description: "From Alice".to_owned(),
    })
    .await?;

    // Remove only this test's accounts to simulate fresh projection state
    {
        let mut rows = COMMAND_ROWS.write().unwrap();
        rows.remove(&alice_id);
        rows.remove(&bob_id);
    }

    // Verify our accounts are not in projection
    {
        let rows = COMMAND_ROWS.read().unwrap();
        assert!(!rows.contains_key(&alice_id));
        assert!(!rows.contains_key(&bob_id));
    }

    // Run subscription to rebuild projection from events
    bank::subscription().execute(executor).await?;

    // Verify projection was rebuilt correctly
    let rows = COMMAND_ROWS.read().unwrap();

    // Check Alice's account
    let alice_row = rows
        .get(&alice_id)
        .expect("Alice should exist in projection");
    assert_eq!(alice_row.0.balance, 900); // 1000 + 200 - 300
    assert_eq!(alice_row.1, 3); // version: AccountOpened + MoneyDeposited + MoneyTransferred

    // Check Bob's account
    let bob_row = rows.get(&bob_id).expect("Bob should exist in projection");
    assert_eq!(bob_row.0.balance, 800); // 500 + 300
    assert_eq!(bob_row.1, 2); // version: AccountOpened + MoneyReceived

    Ok(())
}

pub async fn subscribe_routing_key<E: Executor + Clone>(
    executor: &E,
    _events: Vec<Event>,
) -> anyhow::Result<()> {
    // Create account with routing key "us-east-1"
    let us_account_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: "owner_us".to_owned(),
            owner_name: "US User".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
        "us-east-1",
    )
    .await?;

    // Create account with routing key "eu-west-1"
    let eu_account_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: "owner_eu".to_owned(),
            owner_name: "EU User".to_owned(),
            account_type: AccountType::Checking,
            currency: "EUR".to_owned(),
            initial_balance: 2000,
        },
        executor,
        "eu-west-1",
    )
    .await?;

    // Deposit to both accounts
    let us_account = bank::load(executor, &us_account_id).await?.unwrap();
    us_account
        .deposit_money(DepositMoney {
            amount: 500,
            transaction_id: Ulid::new().to_string(),
            description: "US deposit".to_owned(),
        })
        .await?;

    let eu_account = bank::load(executor, &eu_account_id).await?.unwrap();
    eu_account
        .deposit_money(DepositMoney {
            amount: 300,
            transaction_id: Ulid::new().to_string(),
            description: "EU deposit".to_owned(),
        })
        .await?;

    // Remove only this test's accounts from projection
    {
        let mut rows = COMMAND_ROWS.write().unwrap();
        rows.remove(&us_account_id);
        rows.remove(&eu_account_id);
    }

    // Run subscription filtered by "us-east-1" routing key
    bank::subscription()
        .routing_key("us-east-1")
        .execute(executor)
        .await?;

    // Verify only US account was processed
    {
        let rows = COMMAND_ROWS.read().unwrap();

        // US account should exist with correct balance
        let us_row = rows
            .get(&us_account_id)
            .expect("US account should exist in projection");
        assert_eq!(us_row.0.balance, 1500); // 1000 + 500
        assert_eq!(us_row.1, 2); // version: AccountOpened + MoneyDeposited
        assert_eq!(us_row.2, Some("us-east-1".to_owned()));

        // EU account should NOT exist (not processed by this subscription)
        assert!(
            !rows.contains_key(&eu_account_id),
            "EU account should NOT be in projection (different routing key)"
        );
    }

    // Now run subscription filtered by "eu-west-1" routing key
    bank::subscription()
        .routing_key("eu-west-1")
        .execute(executor)
        .await?;

    // Verify EU account was now processed
    {
        let rows = COMMAND_ROWS.read().unwrap();

        // EU account should now exist
        let eu_row = rows
            .get(&eu_account_id)
            .expect("EU account should exist in projection");
        assert_eq!(eu_row.0.balance, 2300); // 2000 + 300
        assert_eq!(eu_row.1, 2); // version: AccountOpened + MoneyDeposited
        assert_eq!(eu_row.2, Some("eu-west-1".to_owned()));
    }

    Ok(())
}

pub async fn subscribe_default<E: Executor + Clone>(
    executor: &E,
    _events: Vec<Event>,
) -> anyhow::Result<()> {
    // Create account WITHOUT routing key (default/None)
    let default_account_id = Command::open_account(
        OpenAccount {
            owner_id: "owner_default".to_owned(),
            owner_name: "Default User".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    // Create account WITH routing key
    let routed_account_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: "owner_routed".to_owned(),
            owner_name: "Routed User".to_owned(),
            account_type: AccountType::Checking,
            currency: "EUR".to_owned(),
            initial_balance: 2000,
        },
        executor,
        "eu-west-1",
    )
    .await?;

    // Deposit to both accounts
    let default_account = bank::load(executor, &default_account_id).await?.unwrap();
    default_account
        .deposit_money(DepositMoney {
            amount: 500,
            transaction_id: Ulid::new().to_string(),
            description: "Default deposit".to_owned(),
        })
        .await?;

    let routed_account = bank::load(executor, &routed_account_id).await?.unwrap();
    routed_account
        .deposit_money(DepositMoney {
            amount: 300,
            transaction_id: Ulid::new().to_string(),
            description: "Routed deposit".to_owned(),
        })
        .await?;

    // Remove only this test's accounts from projection
    {
        let mut rows = COMMAND_ROWS.write().unwrap();
        rows.remove(&default_account_id);
        rows.remove(&routed_account_id);
    }

    // Run default subscription (no routing key = processes events with routing_key IS NULL)
    bank::subscription().execute(executor).await?;

    // Verify only default (no routing key) account was processed
    {
        let rows = COMMAND_ROWS.read().unwrap();

        // Default account should exist with correct balance
        let default_row = rows
            .get(&default_account_id)
            .expect("Default account should exist in projection");
        assert_eq!(default_row.0.balance, 1500); // 1000 + 500
        assert_eq!(default_row.1, 2); // version: AccountOpened + MoneyDeposited
        assert_eq!(default_row.2, None); // no routing key

        // Routed account should NOT exist (has routing key, not processed by default subscription)
        assert!(
            !rows.contains_key(&routed_account_id),
            "Routed account should NOT be in projection (has routing key)"
        );
    }

    // Now run subscription with specific routing key
    bank::subscription()
        .routing_key("eu-west-1")
        .execute(executor)
        .await?;

    // Verify routed account was now processed
    {
        let rows = COMMAND_ROWS.read().unwrap();

        // Routed account should now exist
        let routed_row = rows
            .get(&routed_account_id)
            .expect("Routed account should exist in projection");
        assert_eq!(routed_row.0.balance, 2300); // 2000 + 300
        assert_eq!(routed_row.1, 2); // version: AccountOpened + MoneyDeposited
        assert_eq!(routed_row.2, Some("eu-west-1".to_owned()));
    }

    Ok(())
}

pub async fn subscribe_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    _events: Vec<Event>,
) -> anyhow::Result<()> {
    // Create an Owner aggregate using evento::create()
    let owner_id = evento::create()
        .event(&Created {
            name: "John Doe".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Create a bank account with this owner
    let account_id = Command::open_account(
        OpenAccount {
            owner_id: owner_id.clone(),
            owner_name: "John Doe".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    // Deposit some money
    let account = bank::load(executor, &account_id).await?.unwrap();
    account
        .deposit_money(DepositMoney {
            amount: 500,
            transaction_id: Ulid::new().to_string(),
            description: "Deposit".to_owned(),
        })
        .await?;

    // Update owner name using evento::aggregator()
    evento::aggregator(&owner_id)
        .original_version(1)
        .event(&NameChanged {
            value: "John Smith".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Remove this test's account from projection
    {
        let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
        rows.remove(&account_id);
    }

    // Run account_details subscription (handles both BankAccount and Owner events)
    account_details_subscription().execute(executor).await?;

    // Verify projection was rebuilt correctly with both aggregator types processed
    let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();

    let account_row = rows
        .get(&account_id)
        .expect("Account should exist in projection");

    // Verify BankAccount events were processed
    assert_eq!(account_row.0.balance, 1500); // 1000 + 500
    assert_eq!(account_row.0.available_balance, 1500);
    assert_eq!(account_row.0.owner_id, owner_id);

    // Verify Owner::NameChanged event was processed (updates owner_name)
    assert_eq!(account_row.0.owner_name, "John Smith");

    Ok(())
}

pub async fn subscribe_routing_key_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    _events: Vec<Event>,
) -> anyhow::Result<()> {
    // Create Owner with routing key "us-east-1"
    let us_owner_id = evento::create()
        .routing_key("us-east-1")
        .event(&Created {
            name: "US Owner".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Create Owner with routing key "eu-west-1"
    let eu_owner_id = evento::create()
        .routing_key("eu-west-1")
        .event(&Created {
            name: "EU Owner".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Create bank account with routing key "us-east-1"
    let us_account_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: us_owner_id.clone(),
            owner_name: "US Owner".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
        "us-east-1",
    )
    .await?;

    // Create bank account with routing key "eu-west-1"
    let eu_account_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: eu_owner_id.clone(),
            owner_name: "EU Owner".to_owned(),
            account_type: AccountType::Checking,
            currency: "EUR".to_owned(),
            initial_balance: 2000,
        },
        executor,
        "eu-west-1",
    )
    .await?;

    // Update US owner name
    evento::aggregator(&us_owner_id)
        .original_version(1)
        .routing_key("us-east-1")
        .event(&NameChanged {
            value: "US Owner Updated".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Update EU owner name
    evento::aggregator(&eu_owner_id)
        .original_version(1)
        .routing_key("eu-west-1")
        .event(&NameChanged {
            value: "EU Owner Updated".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Remove this test's accounts from projection
    {
        let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
        rows.remove(&us_account_id);
        rows.remove(&eu_account_id);
    }

    // Run subscription filtered by "us-east-1" routing key
    account_details_subscription()
        .routing_key("us-east-1")
        .execute(executor)
        .await?;

    // Verify only US account was processed
    {
        let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();

        // US account should exist with updated owner name
        let us_row = rows
            .get(&us_account_id)
            .expect("US account should exist in projection");
        assert_eq!(us_row.0.balance, 1000);
        assert_eq!(us_row.0.owner_id, us_owner_id);
        assert_eq!(us_row.0.owner_name, "US Owner Updated"); // NameChanged was processed
        assert_eq!(us_row.2, Some("us-east-1".to_owned()));

        // EU account should NOT exist (different routing key)
        assert!(
            !rows.contains_key(&eu_account_id),
            "EU account should NOT be in projection (different routing key)"
        );
    }

    // Now run subscription filtered by "eu-west-1" routing key
    account_details_subscription()
        .routing_key("eu-west-1")
        .execute(executor)
        .await?;

    // Verify EU account was now processed
    {
        let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();

        let eu_row = rows
            .get(&eu_account_id)
            .expect("EU account should exist in projection");
        assert_eq!(eu_row.0.balance, 2000);
        assert_eq!(eu_row.0.owner_id, eu_owner_id);
        assert_eq!(eu_row.0.owner_name, "EU Owner Updated"); // NameChanged was processed
        assert_eq!(eu_row.2, Some("eu-west-1".to_owned()));
    }

    Ok(())
}

pub async fn subscribe_default_multiple_aggregator<E: Executor + Clone>(
    executor: &E,
    _events: Vec<Event>,
) -> anyhow::Result<()> {
    // Create Owner WITHOUT routing key (default)
    let default_owner_id = evento::create()
        .event(&Created {
            name: "Default Owner".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Create Owner WITH routing key
    let routed_owner_id = evento::create()
        .routing_key("eu-west-1")
        .event(&Created {
            name: "Routed Owner".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Create bank account WITHOUT routing key (default)
    let default_account_id = Command::open_account(
        OpenAccount {
            owner_id: default_owner_id.clone(),
            owner_name: "Default Owner".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
    )
    .await?;

    // Create bank account WITH routing key
    let routed_account_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: routed_owner_id.clone(),
            owner_name: "Routed Owner".to_owned(),
            account_type: AccountType::Checking,
            currency: "EUR".to_owned(),
            initial_balance: 2000,
        },
        executor,
        "eu-west-1",
    )
    .await?;

    // Update default owner name (no routing key)
    evento::aggregator(&default_owner_id)
        .original_version(1)
        .event(&NameChanged {
            value: "Default Owner Updated".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Update routed owner name (with routing key)
    evento::aggregator(&routed_owner_id)
        .original_version(1)
        .routing_key("eu-west-1")
        .event(&NameChanged {
            value: "Routed Owner Updated".to_owned(),
        })
        .metadata(&Metadata::default())
        .commit(executor)
        .await?;

    // Remove this test's accounts from projection
    {
        let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
        rows.remove(&default_account_id);
        rows.remove(&routed_account_id);
    }

    // Run default subscription (no routing key = processes events with routing_key IS NULL)
    account_details_subscription().execute(executor).await?;

    // Verify only default (no routing key) account was processed
    {
        let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();

        // Default account should exist with updated owner name
        let default_row = rows
            .get(&default_account_id)
            .expect("Default account should exist in projection");
        assert_eq!(default_row.0.balance, 1000);
        assert_eq!(default_row.0.owner_id, default_owner_id);
        assert_eq!(default_row.0.owner_name, "Default Owner Updated"); // NameChanged was processed
        assert_eq!(default_row.2, None); // no routing key

        // Routed account should NOT exist (has routing key)
        assert!(
            !rows.contains_key(&routed_account_id),
            "Routed account should NOT be in projection (has routing key)"
        );
    }

    // Now run subscription with specific routing key
    account_details_subscription()
        .routing_key("eu-west-1")
        .execute(executor)
        .await?;

    // Verify routed account was now processed
    {
        let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();

        let routed_row = rows
            .get(&routed_account_id)
            .expect("Routed account should exist in projection");
        assert_eq!(routed_row.0.balance, 2000);
        assert_eq!(routed_row.0.owner_id, routed_owner_id);
        assert_eq!(routed_row.0.owner_name, "Routed Owner Updated"); // NameChanged was processed
        assert_eq!(routed_row.2, Some("eu-west-1".to_owned()));
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
pub async fn all_commands<E: Executor + Clone>(
    executor: &E,
    _events: Vec<Event>,
) -> anyhow::Result<()> {
    // =========================================================================
    // 1. Open two accounts
    // =========================================================================

    // Account A: Primary test account
    let account_a_id = Command::open_account(
        OpenAccount {
            owner_id: "owner_a".to_owned(),
            owner_name: "Alice".to_owned(),
            account_type: AccountType::Checking,
            currency: "USD".to_owned(),
            initial_balance: 5000,
        },
        executor,
    )
    .await?;

    // Account B: Secondary account for transfers (with routing key)
    let account_b_id = Command::open_account_with_routing(
        OpenAccount {
            owner_id: "owner_b".to_owned(),
            owner_name: "Bob".to_owned(),
            account_type: AccountType::Savings,
            currency: "USD".to_owned(),
            initial_balance: 1000,
        },
        executor,
        "region-1",
    )
    .await?;

    // Verify initial state
    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.balance, 5000);
    assert_eq!(account_a.event_version, 1);
    assert!(account_a.is_active());

    let account_b = bank::load(executor, &account_b_id)
        .await?
        .expect("Account B should exist");
    assert_eq!(account_b.balance, 1000);
    assert_eq!(account_b.event_routing_key, Some("region-1".to_owned()));

    // =========================================================================
    // 2. DepositMoney
    // =========================================================================

    account_a
        .deposit_money(DepositMoney {
            amount: 2500,
            transaction_id: Ulid::new().to_string(),
            description: "Salary deposit".to_owned(),
        })
        .await?;

    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.balance, 7500); // 5000 + 2500
    assert_eq!(account_a.event_version, 2);

    // =========================================================================
    // 3. WithdrawMoney
    // =========================================================================

    account_a
        .withdraw_money(WithdrawMoney {
            amount: 500,
            transaction_id: Ulid::new().to_string(),
            description: "ATM withdrawal".to_owned(),
        })
        .await?;

    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.balance, 7000); // 7500 - 500
    assert_eq!(account_a.event_version, 3);

    // =========================================================================
    // 4. ChangeOverdraftLimit
    // =========================================================================

    account_a
        .change_overdraft_limit(ChangeOverdraftLimit { new_limit: 1000 })
        .await?;

    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.overdraft_limit, 1000);
    assert_eq!(account_a.event_version, 4);

    // =========================================================================
    // 5. TransferMoney / ReceiveMoney
    // =========================================================================

    let transfer_tx_id = Ulid::new().to_string();

    // Alice transfers to Bob
    account_a
        .transfer_money(TransferMoney {
            amount: 2000,
            to_account_id: account_b_id.clone(),
            transaction_id: transfer_tx_id.clone(),
            description: "Payment to Bob".to_owned(),
        })
        .await?;

    // Bob receives from Alice
    let account_b = bank::load(executor, &account_b_id)
        .await?
        .expect("Account B should exist");
    account_b
        .receive_money(ReceiveMoney {
            amount: 2000,
            from_account_id: account_a_id.clone(),
            transaction_id: transfer_tx_id,
            description: "Payment from Alice".to_owned(),
        })
        .await?;

    // Verify balances after transfer
    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    let account_b = bank::load(executor, &account_b_id)
        .await?
        .expect("Account B should exist");

    assert_eq!(account_a.balance, 5000); // 7000 - 2000
    assert_eq!(account_a.event_version, 5);
    assert_eq!(account_b.balance, 3000); // 1000 + 2000
    assert_eq!(account_b.event_version, 2);

    // =========================================================================
    // 6. FreezeAccount / UnfreezeAccount
    // =========================================================================

    account_a
        .freeze_account(FreezeAccount {
            reason: "Suspicious activity detected".to_owned(),
        })
        .await?;

    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    assert!(account_a.is_frozen());
    assert_eq!(account_a.event_version, 6);

    // Try to withdraw while frozen - should fail
    let withdraw_result = account_a
        .withdraw_money(WithdrawMoney {
            amount: 100,
            transaction_id: Ulid::new().to_string(),
            description: "Should fail".to_owned(),
        })
        .await;
    assert!(withdraw_result.is_err());

    // Unfreeze the account
    account_a
        .unfreeze_account(UnfreezeAccount {
            reason: "Investigation complete".to_owned(),
        })
        .await?;

    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    assert!(account_a.is_active());
    assert_eq!(account_a.event_version, 7);

    // =========================================================================
    // 7. CloseAccount
    // =========================================================================

    // First, withdraw remaining balance to prepare for closure
    account_a
        .withdraw_money(WithdrawMoney {
            amount: 5000,
            transaction_id: Ulid::new().to_string(),
            description: "Final withdrawal before closure".to_owned(),
        })
        .await?;

    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    assert_eq!(account_a.balance, 0);
    assert_eq!(account_a.event_version, 8);

    // Close the account
    account_a
        .close_account(CloseAccount {
            reason: "Customer request".to_owned(),
        })
        .await?;

    let account_a = bank::load(executor, &account_a_id)
        .await?
        .expect("Account A should exist");
    assert!(account_a.is_closed());
    assert_eq!(account_a.event_version, 9);

    // Try operations on closed account - should fail
    let deposit_result = account_a
        .deposit_money(DepositMoney {
            amount: 100,
            transaction_id: Ulid::new().to_string(),
            description: "Should fail".to_owned(),
        })
        .await;
    assert!(deposit_result.is_err());

    // =========================================================================
    // 8. Verify final state via subscription
    // =========================================================================

    // Clear projection state for our test accounts
    {
        let mut rows = COMMAND_ROWS.write().unwrap();
        rows.remove(&account_a_id);
        rows.remove(&account_b_id);
    }

    // Run subscription to rebuild projection from events
    bank::subscription().execute(executor).await?;

    // Verify Account A projection
    {
        let rows = COMMAND_ROWS.read().unwrap();

        let account_a_row = rows
            .get(&account_a_id)
            .expect("Account A should exist in projection");
        assert_eq!(account_a_row.0.balance, 0);
        assert_eq!(account_a_row.0.status, AccountStatus::Closed);
        assert_eq!(account_a_row.1, 9); // version
    }

    // Run subscription with routing key for Account B
    bank::subscription()
        .routing_key("region-1")
        .execute(executor)
        .await?;

    // Verify Account B projection
    {
        let rows = COMMAND_ROWS.read().unwrap();

        let account_b_row = rows
            .get(&account_b_id)
            .expect("Account B should exist in projection");
        assert_eq!(account_b_row.0.balance, 3000);
        assert_eq!(account_b_row.0.status, AccountStatus::Active);
        assert_eq!(account_b_row.1, 2); // version
        assert_eq!(account_b_row.2, Some("region-1".to_owned()));
    }

    Ok(())
}
