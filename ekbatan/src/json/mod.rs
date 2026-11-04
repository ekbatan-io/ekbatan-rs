//! Event payloads and action params as Java's Jackson writes them, so a Java service, a CDC
//! pipeline or a Python port reading the event log sees the same text.
//!
//! serde already writes fields in declaration order, strings with Jackson's escaping, and
//! `Instant` (`DateTime<Utc>`) as Java's `Instant.toString()`. What differs is numbers, so this
//! writer changes only those:
//!
//! - a `Decimal` is a JSON number in Java's `BigDecimal.toString()` form: `10.50`, `0.00`, and
//!   `1E-7` once the exponent is below -6; never `-0`;
//! - an `f64` / `f32` is Java's `Double.toString()` / `Float.toString()`: `0.1`, `1.0`,
//!   `1.0E20`, `1.234E-5`.
//!
//! Proven against values Jackson wrote (`tests/fixtures`).

mod ids;

use std::io;

use serde::Serialize;
use serde_json::ser::Formatter;

use crate::Error;

/// `value` as JSON text, numbers written as Java writes them.
pub fn to_string<T: Serialize + ?Sized>(value: &T) -> Result<String, Error> {
    let mut out = Vec::with_capacity(128);
    let mut serializer = serde_json::Serializer::with_formatter(&mut out, JavaFormatter);
    value.serialize(&mut serializer)?;
    Ok(String::from_utf8(out).expect("serde_json writes UTF-8"))
}

struct JavaFormatter;

impl Formatter for JavaFormatter {
    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(java_double(value).as_bytes())
    }

    fn write_f32<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f32) -> io::Result<()> {
        writer.write_all(java_float(value).as_bytes())
    }

    /// A number serde_json carries as text: here, a `Decimal`.
    fn write_number_str<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        value: &str,
    ) -> io::Result<()> {
        writer.write_all(java_big_decimal(value).as_bytes())
    }
}

/// Java's `Double.toString(value)` for a finite value.
pub fn java_double(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .to_owned();
    }
    let magnitude = value.abs();
    java_floating(&format!("{value:e}"), (1e-3..1e7).contains(&magnitude))
}

/// Java's `Float.toString(value)` for a finite value.
pub fn java_float(value: f32) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .to_owned();
    }
    let magnitude = value.abs();
    java_floating(&format!("{value:e}"), (1e-3..1e7).contains(&magnitude))
}

/// From Rust's shortest `{:e}` form (`-1.234e-5`): Java's plain form inside [10^-3, 10^7),
/// its `d.dddE±n` form outside it, always with a digit after the point.
fn java_floating(scientific: &str, plain: bool) -> String {
    let (sign, rest) = match scientific.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", scientific),
    };
    let (mantissa, exponent) = rest.split_once('e').expect("{:e} has an exponent");
    let exponent: i32 = exponent.parse().expect("an integer exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    if !plain {
        let (first, others) = digits.split_at(1);
        let others = if others.is_empty() { "0" } else { others };
        return format!("{sign}{first}.{others}E{exponent}");
    }
    if exponent >= 0 {
        let point = exponent as usize + 1;
        let (integer, fraction) = if digits.len() > point {
            (digits[..point].to_owned(), digits[point..].to_owned())
        } else {
            (format!("{digits:0<point$}"), "0".to_owned())
        };
        format!("{sign}{integer}.{fraction}")
    } else {
        let zeros = "0".repeat((-exponent - 1) as usize);
        format!("{sign}0.{zeros}{digits}")
    }
}

/// Java's `BigDecimal.toString()` from a plain decimal (`-0.000000120`): unchanged, except
/// scientific notation once the adjusted exponent is below -6 (`-1.20E-7`), and no minus sign
/// on zero. Text already in exponent form is left alone.
pub fn java_big_decimal(text: &str) -> String {
    if text.contains(['e', 'E']) {
        return text.to_owned();
    }
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (integer, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    let all: String = format!("{integer}{fraction}");
    let unscaled = all.trim_start_matches('0');
    let unscaled = if unscaled.is_empty() { "0" } else { unscaled };
    let is_zero = unscaled == "0";
    let sign = if negative && !is_zero { "-" } else { "" };
    let scale = fraction.len() as i64;
    let adjusted = unscaled.len() as i64 - 1 - scale;
    if adjusted >= -6 {
        return format!("{sign}{rest}");
    }
    let (first, others) = unscaled.split_at(1);
    if others.is_empty() {
        format!("{sign}{first}E{adjusted}")
    } else {
        format!("{sign}{first}.{others}E{adjusted}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_as_big_decimal_writes_them() {
        assert_eq!(java_big_decimal("10.50"), "10.50");
        assert_eq!(java_big_decimal("0.00"), "0.00");
        assert_eq!(java_big_decimal("-0.00"), "0.00");
        assert_eq!(java_big_decimal("0.000001"), "0.000001");
        assert_eq!(java_big_decimal("0.0000001"), "1E-7");
        assert_eq!(java_big_decimal("-0.000000120"), "-1.20E-7");
        assert_eq!(java_big_decimal("0.0000000"), "0E-7");
        assert_eq!(java_big_decimal("1000"), "1000");
    }

    #[test]
    fn doubles_as_double_to_string_writes_them() {
        assert_eq!(java_double(0.1), "0.1");
        assert_eq!(java_double(1.0), "1.0");
        assert_eq!(java_double(100.0), "100.0");
        assert_eq!(java_double(1e7), "1.0E7");
        assert_eq!(java_double(1e20), "1.0E20");
        assert_eq!(java_double(0.001), "0.001");
        assert_eq!(java_double(0.0001), "1.0E-4");
        assert_eq!(java_double(-1.234e-5), "-1.234E-5");
        assert_eq!(java_double(-0.0), "-0.0");
    }
}
