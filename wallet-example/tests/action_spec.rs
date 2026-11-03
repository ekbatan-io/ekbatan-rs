//! The example's actions under `AsyncActionSpec`: `perform` on a fresh plan, no database. Java's
//! action unit tests.

use std::sync::Arc;

use ekbatan::{Error, Model, Persistable};
use ekbatan_test_support::VirtualClock;
use ekbatan_test_support::aio::AsyncActionSpec;
use ekbatan_test_support::blocking::ActionSpec;
use rust_decimal::Decimal;
use uuid::Uuid;
use wallet_example::action::wallet_create_action::MEXICO_SHARD;
use wallet_example::action::{
    CreateNotificationAction, WalletCreateAction, create_notification_action, wallet_create_action,
};
use wallet_example::model::{
    Notification, Wallet, WalletClosedEvent, WalletCreatedEvent, WalletState,
};

#[tokio::test]
async fn create_stages_one_wallet_and_its_event() {
    let clock = Arc::new(VirtualClock::new());
    clock.pause();
    let action = WalletCreateAction::new(clock.clone());
    let owner = Uuid::new_v4();
    AsyncActionSpec::of(&action)
        .with_principal("alice")
        .execute(wallet_create_action::Params {
            country_code: Some("mx".to_owned()),
            owner_id: owner,
            currency: "MXN".to_owned(),
            initial_balance: Decimal::TEN,
        })
        .await
        .assert_added_one::<Wallet>(|wallet| {
            assert_eq!(wallet.id().shard(), MEXICO_SHARD);
            assert_eq!(wallet.state(), WalletState::Opened);
            assert_eq!(wallet.created_date(), ekbatan::Clock::instant(&*clock));
        })
        .assert_total_additions(1)
        .assert_no_updates()
        .assert_emitted_one::<WalletCreatedEvent>(|event| {
            assert_eq!(event.owner_id, owner);
            assert_eq!(event.initial_balance, Decimal::TEN);
        })
        .assert_not_emitted::<WalletClosedEvent>()
        .assert_no_changes_of::<Notification>()
        .assert_result(|wallet| assert_eq!(wallet.version(), 1));
}

#[tokio::test]
async fn a_blank_recipient_fails_and_stages_nothing() {
    let error = AsyncActionSpec::of(&CreateNotificationAction)
        .execute_err(create_notification_action::Params {
            wallet_id: Uuid::new_v4(),
            kind: "DEPOSIT".to_owned(),
            recipient: " ".to_owned(),
            message: "hi".to_owned(),
        })
        .await;
    assert!(matches!(error, Error::Invalid(m) if m == "recipient cannot be blank"));
}

#[tokio::test]
async fn an_entity_emits_nothing() {
    AsyncActionSpec::of(&CreateNotificationAction)
        .execute(create_notification_action::Params {
            wallet_id: Uuid::new_v4(),
            kind: "DEPOSIT".to_owned(),
            recipient: "a@b.c".to_owned(),
            message: "hi".to_owned(),
        })
        .await
        .assert_added::<Notification>(1)
        .assert_no_events();
}

#[tokio::test]
#[should_panic(expected = "Expected exactly 1 update of Wallet but found 0")]
async fn a_failed_assertion_names_the_type() {
    AsyncActionSpec::of(&CreateNotificationAction)
        .execute(create_notification_action::Params {
            wallet_id: Uuid::new_v4(),
            kind: "DEPOSIT".to_owned(),
            recipient: "a@b.c".to_owned(),
            message: "hi".to_owned(),
        })
        .await
        .assert_updated_one::<Wallet>(|_| {});
}

/// A blocking action: `perform` is a plain `fn`.
struct SyncOpen;

#[derive(Debug)]
struct NoNegativeBalance;

impl std::fmt::Display for NoNegativeBalance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a wallet can't open with a negative balance")
    }
}

impl std::error::Error for NoNegativeBalance {}

impl ekbatan::blocking::Action for SyncOpen {
    type Params = wallet_create_action::Params;
    type Result = Wallet;

    fn perform(
        &self,
        plan: &mut ekbatan::action::ActionPlan,
        _principal: Option<&str>,
        params: &wallet_create_action::Params,
    ) -> Result<Wallet, Error> {
        if params.initial_balance.is_sign_negative() {
            return Err(Error::action(NoNegativeBalance));
        }
        let wallet = Wallet::create_wallet(
            MEXICO_SHARD,
            params.owner_id,
            &params.currency,
            params.initial_balance,
            ekbatan::Instant::default(),
        )
        .build()?;
        plan.add(wallet)
    }
}

#[test]
fn a_blocking_action_under_the_blocking_spec() {
    let params = |balance| wallet_create_action::Params {
        country_code: None,
        owner_id: Uuid::new_v4(),
        currency: "EUR".to_owned(),
        initial_balance: balance,
    };
    ActionSpec::of(&SyncOpen)
        .execute(params(Decimal::ONE))
        .assert_added_one::<Wallet>(|wallet| assert_eq!(*wallet.balance(), Decimal::ONE))
        .assert_emitted::<WalletCreatedEvent>()
        .assert_no_updates();
    ActionSpec::of(&SyncOpen)
        .assert_fails_with::<NoNegativeBalance>(params(Decimal::NEGATIVE_ONE), |error| {
            assert!(error.to_string().contains("negative"))
        });
}
