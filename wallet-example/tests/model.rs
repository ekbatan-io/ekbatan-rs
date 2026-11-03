//! What `#[model]`, `#[entity]` and `#[model_event]` write, checked on the wallet example's
//! types. No database.

use ekbatan::{Clock, Entity, Error, Model, ModelEvent, Persistable, ShardIdentifier, SystemClock};
use rust_decimal::Decimal;
use uuid::Uuid;
use wallet_example::model::{
    Notification, NotificationBuilder, Wallet, WalletBuilder, WalletCreatedEvent,
    WalletMoneyDepositedEvent, WalletState,
};

fn wallet() -> Wallet {
    Wallet::create_wallet(
        ShardIdentifier::new(1, 0),
        Uuid::new_v4(),
        "EUR",
        Decimal::new(10, 0),
        SystemClock.instant(),
    )
    .build()
    .unwrap()
}

#[test]
fn a_new_wallet_has_version_1_its_event_and_equal_dates() {
    let wallet = wallet();
    assert_eq!(wallet.version(), 1);
    assert_eq!(wallet.state(), WalletState::Opened);
    assert_eq!(wallet.created_date(), wallet.updated_date());
    assert_eq!(wallet.events().len(), 1);
    assert_eq!(wallet.events()[0].event_type(), "WalletCreatedEvent");
    assert!(Wallet::is_model());
    assert!(!Notification::is_model());
}

#[test]
fn a_missing_field_is_named_as_java_names_it() {
    let missing = WalletBuilder::new()
        .id(*wallet().id())
        .state(WalletState::Opened)
        .with_initial_version()
        .created_date(SystemClock.instant())
        .owner_id(Uuid::new_v4())
        .balance(Decimal::ZERO)
        .build();
    assert!(
        matches!(&missing, Err(Error::Invalid(message)) if message == "currency cannot be null"),
        "{missing:?}"
    );
}

#[test]
fn copy_keeps_the_events_and_adds_new_ones() {
    let wallet = wallet();
    let richer = wallet.deposit(Decimal::new(5, 0)).unwrap();
    assert_eq!(*richer.balance(), Decimal::new(15, 0));
    assert_eq!(richer.version(), 1, "a change doesn't bump the version");
    assert_eq!(richer.id(), wallet.id());
    let types: Vec<_> = richer.events().iter().map(|e| e.event_type()).collect();
    assert_eq!(types, ["WalletCreatedEvent", "WalletMoneyDepositedEvent"]);
    assert!(wallet.deposit(Decimal::ZERO).is_err());
}

#[test]
fn equality_ignores_events_and_next_version_adds_one() {
    let wallet = wallet();
    let without_events = wallet.copy().events(Vec::new()).build().unwrap();
    assert_eq!(wallet, without_events);
    let next = wallet.next_version();
    assert_eq!(next.version(), 2);
    assert_ne!(next, wallet);
    assert_eq!(*next.balance(), *wallet.balance());
}

#[test]
fn closing_twice_emits_once() {
    let closed = wallet().close().unwrap();
    assert_eq!(closed.state(), WalletState::Closed);
    assert_eq!(closed.events().len(), 2);
    assert_eq!(closed.close().unwrap().events().len(), 2);
}

#[test]
fn an_entity_runs_its_validate() {
    let blank = Notification::create_notification(Uuid::new_v4(), "DEPOSIT", " ", "hi").build();
    assert!(
        matches!(&blank, Err(Error::Invalid(message)) if message == "recipient cannot be blank"),
        "{blank:?}"
    );
    let ok = Notification::create_notification(Uuid::new_v4(), "DEPOSIT", "a@b.c", "hi")
        .build()
        .unwrap();
    let copied: NotificationBuilder = ok.copy();
    assert_eq!(copied.build().unwrap(), ok);
}

#[test]
fn events_serialize_with_java_names() {
    let event = WalletMoneyDepositedEvent {
        amount: Decimal::new(5, 0),
        new_balance: Decimal::new(15, 0),
    };
    let json = serde_json::to_value(&event).unwrap();
    assert!(json.get("newBalance").is_some(), "{json}");
    let created = WalletCreatedEvent {
        owner_id: Uuid::nil(),
        currency: "EUR".to_owned(),
        initial_balance: Decimal::ONE,
    };
    let json = serde_json::to_value(&created).unwrap();
    assert!(json.get("ownerId").is_some() && json.get("initialBalance").is_some());
    let _: &dyn ModelEvent<Wallet> = &created;
}
