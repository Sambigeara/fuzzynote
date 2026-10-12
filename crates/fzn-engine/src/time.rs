//! Whole-day arithmetic. The engine never needs anything finer than a day.

/// Days since 1970-01-01 in the proleptic Gregorian calendar.
pub type Day = i32;

const SECS_PER_DAY: i64 = 86_400;

/// Converts a civil date to a day number.
pub fn day_from_civil(year: i32, month: u32, day: u32) -> Day {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i32 - 719_468
}

/// Converts a day number back to a civil `(year, month, day)`.
pub fn civil_from_day(day: Day) -> (i32, u32, u32) {
    let z = day + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe as i32 + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

pub fn day_from_unix_secs(secs: i64) -> Day {
    secs.div_euclid(SECS_PER_DAY) as Day
}

pub fn day_from_unix_nanos(nanos: i64) -> Day {
    day_from_unix_secs(nanos.div_euclid(1_000_000_000))
}

/// Formats a day as `YYYY-MM-DD`.
pub fn format_day(day: Day) -> String {
    let (y, m, d) = civil_from_day(day);
    format!("{y:04}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_day_zero() {
        assert_eq!(day_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_day(0), (1970, 1, 1));
    }

    #[test]
    fn round_trips_across_leap_years_and_centuries() {
        for (y, m, d) in [
            (2000, 2, 29),
            (2024, 2, 29),
            (1900, 3, 1),
            (2026, 10, 12),
            (1969, 12, 31),
        ] {
            let day = day_from_civil(y, m, d);
            assert_eq!(civil_from_day(day), (y, m, d), "{y}-{m}-{d}");
        }
        assert_eq!(day_from_civil(1969, 12, 31), -1);
    }

    #[test]
    fn unix_conversions() {
        assert_eq!(day_from_unix_secs(86_399), 0);
        assert_eq!(day_from_unix_secs(86_400), 1);
        assert_eq!(
            day_from_unix_nanos(1_642_000_000_000_000_000),
            day_from_civil(2022, 1, 12)
        );
        assert_eq!(format_day(day_from_civil(2026, 9, 5)), "2026-09-05");
    }
}
