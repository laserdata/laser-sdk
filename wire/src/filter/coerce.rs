use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MILLI: i64 = 1_000;
const SECONDS_PER_DAY: i64 = 86_400;
const MAX_DECIMAL_DIGITS: usize = 512;

/// How a timestamp is written in the payload or literal.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TimestampFormat {
    /// RFC 3339 text with an explicit `Z` or `+hh:mm` offset. Fractions up to
    /// nanoseconds are accepted when the digits past microseconds are zero.
    Rfc3339,
    /// Whole seconds since the Unix epoch, as a number or an integer string.
    EpochSeconds,
    /// Whole milliseconds since the Unix epoch.
    EpochMillis,
    /// Whole microseconds since the Unix epoch.
    EpochMicros,
}

impl TimestampFormat {
    /// Microseconds since the Unix epoch for a text value, `None` when the text
    /// does not parse exactly under this format.
    pub fn micros_from_text(self, text: &str) -> Option<i64> {
        match self {
            Self::Rfc3339 => rfc3339_micros(text),
            Self::EpochSeconds | Self::EpochMillis | Self::EpochMicros => {
                let value = parse_integer(text)?;
                self.micros_from_integer(value)
            }
        }
    }

    /// Microseconds since the Unix epoch for an integer value, `None` for the
    /// text-only RFC 3339 format and on overflow.
    pub fn micros_from_integer(self, value: i128) -> Option<i64> {
        let value = i64::try_from(value).ok()?;
        match self {
            Self::Rfc3339 => None,
            Self::EpochSeconds => value.checked_mul(MICROS_PER_SECOND),
            Self::EpochMillis => value.checked_mul(MICROS_PER_MILLI),
            Self::EpochMicros => Some(value),
        }
    }
}

/// An exact decimal number, compared without floating-point rounding.
///
/// Built from decimal text (`-12.500`), an integer, or the shortest exact
/// rendering of a finite `f64`. Leading and trailing zeros are normalized away,
/// so `1.50` equals `1.5` and `-0` equals `0`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactDecimal {
    negative: bool,
    integer: String,
    fraction: String,
}

impl ExactDecimal {
    /// Parse `[+-]digits[.digits]`. No exponent, no surrounding whitespace, at
    /// most 512 digits.
    pub fn parse(text: &str) -> Option<Self> {
        let (negative, unsigned) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (integer, fraction) = match unsigned.split_once('.') {
            Some((integer, fraction)) => (integer, fraction),
            None => (unsigned, ""),
        };
        if integer.is_empty() && fraction.is_empty() {
            return None;
        }
        if integer.len() + fraction.len() > MAX_DECIMAL_DIGITS
            || !integer.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
            || (unsigned.contains('.') && fraction.is_empty())
        {
            return None;
        }
        Some(Self::normalized(negative, integer, fraction))
    }

    pub fn from_integer(value: i128) -> Self {
        let digits = value.unsigned_abs().to_string();
        Self::normalized(value < 0, &digits, "")
    }

    /// The shortest decimal that round-trips a finite double, so a JSON
    /// number compares at `f64` precision. `None` for infinities and NaN.
    pub fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        Self::parse(&value.to_string())
    }

    fn normalized(negative: bool, integer: &str, fraction: &str) -> Self {
        let integer = integer.trim_start_matches('0').to_owned();
        let fraction = fraction.trim_end_matches('0').to_owned();
        let negative = negative && !(integer.is_empty() && fraction.is_empty());
        Self {
            negative,
            integer,
            fraction,
        }
    }

    fn magnitude_cmp(&self, other: &Self) -> Ordering {
        self.integer
            .len()
            .cmp(&other.integer.len())
            .then_with(|| self.integer.cmp(&other.integer))
            .then_with(|| self.fraction.cmp(&other.fraction))
    }
}

impl Ord for ExactDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => self.magnitude_cmp(other),
            (true, true) => other.magnitude_cmp(self),
        }
    }
}

impl PartialOrd for ExactDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A decimal integer string: optional sign, digits only.
pub(crate) fn parse_integer(text: &str) -> Option<i128> {
    let digits = text.strip_prefix(['-', '+']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse::<i128>().ok()
}

fn rfc3339_micros(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    let year = digits_at(bytes, 0, 4)?;
    let month = digits_at(bytes, 5, 2)?;
    let day = digits_at(bytes, 8, 2)?;
    let hour = digits_at(bytes, 11, 2)?;
    let minute = digits_at(bytes, 14, 2)?;
    let second = digits_at(bytes, 17, 2)?;
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let mut position = 19;
    let mut fraction_micros = 0i64;
    if bytes.get(position) == Some(&b'.') {
        position += 1;
        let start = position;
        while bytes.get(position).is_some_and(u8::is_ascii_digit) {
            position += 1;
        }
        let fraction = &text[start..position];
        if fraction.is_empty() || fraction.len() > 9 {
            return None;
        }
        let (kept, dropped) = fraction.split_at(fraction.len().min(6));
        if dropped.bytes().any(|byte| byte != b'0') {
            return None;
        }
        let scale = 10i64.pow(u32::try_from(6 - kept.len()).ok()?);
        fraction_micros = kept.parse::<i64>().ok()? * scale;
    }
    let offset_seconds = match bytes.get(position)? {
        b'Z' | b'z' if position + 1 == bytes.len() => 0,
        sign @ (b'+' | b'-') if position + 6 == bytes.len() => {
            let offset_hour = digits_at(bytes, position + 1, 2)?;
            let offset_minute = digits_at(bytes, position + 4, 2)?;
            if bytes[position + 3] != b':' || offset_hour > 23 || offset_minute > 59 {
                return None;
            }
            let magnitude = offset_hour * 3600 + offset_minute * 60;
            if *sign == b'-' { -magnitude } else { magnitude }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(SECONDS_PER_DAY)?
        .checked_add(hour * 3600 + minute * 60 + second)?
        .checked_sub(offset_seconds)?;
    seconds
        .checked_mul(MICROS_PER_SECOND)?
        .checked_add(fraction_micros)
}

fn digits_at(bytes: &[u8], start: usize, len: usize) -> Option<i64> {
    let slice = bytes.get(start..start + len)?;
    slice.iter().try_fold(0i64, |value, byte| {
        byte.is_ascii_digit()
            .then(|| value * 10 + i64::from(byte - b'0'))
    })
}

const fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

const fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if is_leap_year(year) => 29,
        _ => 28,
    }
}

// Days since 1970-01-01 for a proleptic Gregorian date (H. Hinnant's algorithm).
const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_rfc3339_instants_when_parsed_then_should_normalize_to_utc_micros() {
        let utc = TimestampFormat::Rfc3339
            .micros_from_text("2026-09-21T18:04:12.331Z")
            .expect("parses");
        let offset = TimestampFormat::Rfc3339
            .micros_from_text("2026-09-21T20:04:12.331000+02:00")
            .expect("parses");
        assert_eq!(utc, offset, "the same instant in two offsets is equal");
        assert_eq!(
            TimestampFormat::Rfc3339.micros_from_text("1970-01-01T00:00:00Z"),
            Some(0)
        );
        assert_eq!(
            TimestampFormat::Rfc3339.micros_from_text("1969-12-31T23:59:59.999999Z"),
            Some(-1)
        );
    }

    #[test]
    fn given_ambiguous_or_lossy_rfc3339_when_parsed_then_should_be_rejected() {
        for text in [
            "2026-09-21T18:04:12.331000",
            "2026-09-21 18:04:12Z",
            "2026-02-29T00:00:00Z",
            "2026-09-21T24:00:00Z",
            "2026-09-21T18:04:60Z",
            "2026-09-21T18:04:12.0000001Z",
            "2026-09-21T18:04:12.Z",
            "2026-09-21T18:04:12+2:00",
        ] {
            assert_eq!(
                TimestampFormat::Rfc3339.micros_from_text(text),
                None,
                "`{text}` must be rejected"
            );
        }
        assert_eq!(
            TimestampFormat::Rfc3339.micros_from_text("2024-02-29T00:00:00.000000000Z"),
            Some(1_709_164_800_000_000)
        );
    }

    #[test]
    fn given_epoch_units_when_coerced_then_should_scale_exactly() {
        assert_eq!(
            TimestampFormat::EpochSeconds.micros_from_integer(1_758_470_652),
            Some(1_758_470_652_000_000)
        );
        assert_eq!(
            TimestampFormat::EpochMillis.micros_from_text("1758470652331"),
            Some(1_758_470_652_331_000)
        );
        assert_eq!(
            TimestampFormat::EpochMicros.micros_from_integer(-5),
            Some(-5)
        );
        assert_eq!(
            TimestampFormat::EpochSeconds.micros_from_integer(i128::from(i64::MAX)),
            None
        );
        assert_eq!(TimestampFormat::EpochMillis.micros_from_text("12.5"), None);
    }

    #[test]
    fn given_decimals_when_compared_then_should_order_exactly() {
        let parse = |text: &str| ExactDecimal::parse(text).expect("parses");
        assert_eq!(parse("1.50"), parse("1.5"));
        assert_eq!(parse("-0"), parse("0.000"));
        assert!(parse("9007199254740993") > parse("9007199254740992"));
        assert!(parse("-2") < parse("-1.999"));
        assert!(parse("0.1") < parse("0.10001"));
        assert!(parse("10") > parse("9.999999"));
        assert_eq!(ExactDecimal::from_integer(-42), parse("-42.0"));
        assert_eq!(ExactDecimal::from_f64(0.5), Some(parse("0.5")));
    }

    #[test]
    fn given_malformed_decimals_when_parsed_then_should_be_rejected() {
        for text in ["", "-", ".", "1.", "1e5", " 1", "1,5", "0x10", "--1"] {
            assert_eq!(ExactDecimal::parse(text), None, "`{text}` must be rejected");
        }
        assert!(ExactDecimal::parse(".5").is_some());
    }
}
