//! ekbatan's JSON against what Java's Jackson wrote for the same values (`fixtures/`).

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use ekbatan::json::{java_big_decimal, java_double, to_string};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;

const FORMATS: &str = include_str!("fixtures/java_formats.txt");
const JAVA_EVENT: &str = include_str!("fixtures/java_event.json");

#[test]
fn doubles_match_jackson() {
    let mut checked = 0;
    for line in FORMATS.lines().filter(|l| l.starts_with("dbl\t")) {
        let mut parts = line.split('\t').skip(1);
        let bits = u64::from_str_radix(parts.next().unwrap(), 16).unwrap();
        let java = parts.next().unwrap();
        let value = f64::from_bits(bits);
        assert_eq!(java_double(value), java, "bits {bits:x}");
        assert_eq!(to_string(&value).unwrap(), java, "bits {bits:x}");
        checked += 1;
    }
    assert!(checked >= 6000);
}

#[test]
fn decimals_match_jackson() {
    let mut checked = 0;
    for line in FORMATS.lines().filter(|l| l.starts_with("bd\t")) {
        let mut parts = line.split('\t').skip(1);
        let (unscaled, scale) = parts.next().unwrap().split_once(',').unwrap();
        let java = parts.next().unwrap();
        let value =
            Decimal::from_i128_with_scale(unscaled.parse().unwrap(), scale.parse().unwrap());
        assert_eq!(
            java_big_decimal(&value.to_string()),
            java,
            "{unscaled},{scale}"
        );
        assert_eq!(to_string(&value).unwrap(), java, "{unscaled},{scale}");
        checked += 1;
    }
    assert!(checked >= 2000);
}

#[test]
fn instants_match_jackson() {
    let mut checked = 0;
    for line in FORMATS.lines().filter(|l| l.starts_with("instant\t")) {
        let mut parts = line.split('\t').skip(1);
        let (seconds, micros) = parts.next().unwrap().split_once(',').unwrap();
        let java = parts.next().unwrap();
        let micros: u32 = micros.parse().unwrap();
        let instant =
            DateTime::<Utc>::from_timestamp(seconds.parse().unwrap(), micros * 1000).unwrap();
        assert_eq!(to_string(&instant).unwrap(), java, "{seconds},{micros}");
        checked += 1;
    }
    assert!(checked >= 2000);
}

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum State {
    Opened,
}

#[derive(Serialize)]
struct Nested {
    amount: Decimal,
    currency: String,
}

/// Java's probe event, field for field, leaving out the fields this port can't hold the same
/// way (see the test below).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProbeEvent {
    model_id: String,
    model_name: String,
    amount: Decimal,
    zero: Decimal,
    tiny: Decimal,
    negative: Decimal,
    owner_id: Uuid,
    at: DateTime<Utc>,
    at_micros: DateTime<Utc>,
    at_millis: DateTime<Utc>,
    at_offset: DateTime<FixedOffset>,
    local_at: NaiveDateTime,
    day: NaiveDate,
    clock: NaiveTime,
    state: State,
    note: String,
    missing: Option<String>,
    count: i64,
    ratio: f64,
    flag: bool,
    tags: Vec<String>,
    nested: Nested,
}

#[test]
fn an_event_matches_jackson_byte_for_byte() {
    let at = DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let event = ProbeEvent {
        model_id: "01941f29-7c7b-7abc-8042-123456789abc".to_owned(),
        model_name: "Wallet".to_owned(),
        amount: Decimal::new(1050, 2),
        zero: Decimal::new(0, 2),
        tiny: Decimal::new(1, 7),
        negative: Decimal::new(-5, 1),
        owner_id: Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap(),
        at,
        at_micros: at + chrono::Duration::microseconds(123_456),
        at_millis: at + chrono::Duration::milliseconds(120),
        at_offset: DateTime::parse_from_rfc3339("2025-01-01T02:00:00+02:00").unwrap(),
        local_at: NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap(),
        day: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
        clock: NaiveTime::from_hms_opt(10, 15, 30).unwrap(),
        state: State::Opened,
        note: "caf\u{e9} \u{20ac} </tag> \"q\" \\ \n \u{1}".to_owned(),
        missing: None,
        count: 1_152_921_504_606_846_976,
        ratio: 0.1,
        flag: true,
        tags: vec!["b".to_owned(), "a".to_owned()],
        nested: Nested {
            amount: Decimal::new(500, 2),
            currency: "EUR".to_owned(),
        },
    };
    // Jackson's text with the fields left out above removed: `sci` (1E+3: a negative scale,
    // which rust_decimal can't hold), `huge` (39 digits: more than rust_decimal's 28), `attrs`
    // (a map in insertion order: use an IndexMap for that) and `raw` (bytes: Jackson writes
    // base64, serde an array).
    let mut java: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(JAVA_EVENT).unwrap();
    for left_out in ["sci", "huge", "attrs", "raw"] {
        java.remove(left_out);
    }
    let expected = JAVA_EVENT
        .trim()
        .replace(r#""sci":1E+3,"#, "")
        .replace(r#""huge":123456789012345678901234567890.123456789,"#, "")
        .replace(r#""attrs":{"z":1,"a":2,"10":3,"9":4},"#, "")
        .replace(r#","raw":"AQL/""#, "");
    assert_eq!(to_string(&event).unwrap(), expected);
}
