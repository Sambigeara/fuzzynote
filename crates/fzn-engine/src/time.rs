//! Days and moments. Relevance never needs anything finer than a day. A note's times are kept to
//! the second, with the offset of the clock that recorded them.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

/// The day of the week, counting from Monday as 0.
pub fn weekday(day: Day) -> usize {
    // 1970-01-01 was a Thursday.
    (i64::from(day) + 3).rem_euclid(7) as usize
}

/// Formats a day as `YYYY-MM-DD`.
pub fn format_day(day: Day) -> String {
    let (y, m, d) = civil_from_day(day);
    format!("{y:04}-{m:02}-{d:02}")
}

/// A moment, and how far from UTC the clock that recorded it was. The offset keeps the day the
/// moment fell on where it was written, whatever zone it is read in. Written as RFC 3339,
/// `2026-10-13T10:12:56+01:00`, so that a line can be read without the app.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Moment {
    /// Unix seconds.
    pub secs: i64,
    /// Seconds east of UTC.
    pub offset: i32,
}

impl Moment {
    pub fn utc(secs: i64) -> Self {
        Self { secs, offset: 0 }
    }

    /// The day it was where the moment was recorded.
    pub fn day(self) -> Day {
        day_from_unix_secs(self.wall())
    }

    pub fn plus(self, secs: i64) -> Self {
        Self {
            secs: self.secs.saturating_add(secs),
            ..self
        }
    }

    fn wall(self) -> i64 {
        self.secs.saturating_add(i64::from(self.offset))
    }

    /// Reads RFC 3339. Fractions of a second are dropped.
    fn parse(s: &str) -> Option<Self> {
        let b = s.as_bytes();
        let num = |from: usize, n: usize| -> Option<i64> {
            b.get(from..from + n)?.iter().try_fold(0i64, |acc, c| {
                c.is_ascii_digit().then(|| acc * 10 + i64::from(c - b'0'))
            })
        };
        let is = |at: usize, c: u8| b.get(at) == Some(&c);
        if !(is(4, b'-') && is(7, b'-') && is(13, b':') && is(16, b':'))
            || !matches!(b.get(10), Some(b'T' | b't' | b' '))
        {
            return None;
        }
        let (month, day) = (num(5, 2)?, num(8, 2)?);
        let (hour, minute, second) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 {
            return None;
        }
        let mut at = 19;
        if is(at, b'.') {
            at += 1;
            while b.get(at).is_some_and(u8::is_ascii_digit) {
                at += 1;
            }
        }
        let offset = match b.get(at)? {
            b'Z' | b'z' if at + 1 == b.len() => 0,
            sign @ (b'+' | b'-') if at + 6 == b.len() && is(at + 3, b':') => {
                let east = num(at + 1, 2)? * 3600 + num(at + 4, 2)? * 60;
                if *sign == b'+' { east } else { -east }
            }
            _ => return None,
        };
        let days = i64::from(day_from_civil(num(0, 4)? as i32, month as u32, day as u32));
        Some(Self {
            secs: days * SECS_PER_DAY + hour * 3600 + minute * 60 + second - offset,
            offset: offset as i32,
        })
    }
}

impl fmt::Display for Moment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let wall = self.wall();
        let (y, m, d) = civil_from_day(day_from_unix_secs(wall));
        let secs = wall.rem_euclid(SECS_PER_DAY);
        write!(
            f,
            "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
            secs / 3600,
            secs % 3600 / 60,
            secs % 60
        )?;
        if self.offset == 0 {
            return f.write_str("Z");
        }
        let (sign, east) = if self.offset < 0 {
            ('-', -self.offset)
        } else {
            ('+', self.offset)
        };
        write!(f, "{sign}{:02}:{:02}", east / 3600, east % 3600 / 60)
    }
}

impl Serialize for Moment {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Moment {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = Moment;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an RFC 3339 time, or unix seconds")
            }

            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Moment, E> {
                Moment::parse(s).ok_or_else(|| E::custom(format!("not an RFC 3339 time: {s:?}")))
            }

            fn visit_i64<E: serde::de::Error>(self, secs: i64) -> Result<Moment, E> {
                Ok(Moment::utc(secs))
            }

            fn visit_u64<E: serde::de::Error>(self, secs: u64) -> Result<Moment, E> {
                i64::try_from(secs)
                    .map(Moment::utc)
                    .map_err(|_| E::custom("unix seconds out of range"))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
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

    #[test]
    fn weekdays_count_from_monday() {
        assert_eq!(weekday(0), 3);
        assert_eq!(weekday(-1), 2);
        assert_eq!(weekday(day_from_civil(2026, 10, 12)), 0);
        assert_eq!(weekday(day_from_civil(2026, 10, 18)), 6);
    }

    #[test]
    fn a_moment_is_written_as_rfc_3339_and_keeps_its_day() {
        // 23:30 on the 13th in Lisbon, in summer: the 13th there, already the 13th in UTC too.
        // 00:30 on the 14th in Lisbon is still the 13th in UTC, and the note says the 14th.
        let late = Moment {
            secs: 1_791_934_200,
            offset: 3600,
        };
        assert_eq!(late.to_string(), "2026-10-14T00:30:00+01:00");
        assert_eq!(format_day(late.day()), "2026-10-14");
        assert_eq!(format_day(Moment::utc(late.secs).day()), "2026-10-13");
        assert_eq!(Moment::utc(late.secs).to_string(), "2026-10-13T23:30:00Z");

        for text in [
            "2026-10-14T00:30:00+01:00",
            "2026-10-13T23:30:00Z",
            "2026-10-13T18:30:00-05:00",
            "1969-12-31T23:59:59Z",
        ] {
            let moment = Moment::parse(text).unwrap();
            assert_eq!(moment.to_string(), text);
            assert_eq!(
                moment.secs,
                if text.starts_with("19") {
                    -1
                } else {
                    late.secs
                }
            );
        }
        assert_eq!(
            Moment::parse("2026-10-13t23:30:00.250z"),
            Some(Moment::utc(late.secs)),
            "fractions are dropped"
        );
        for bad in [
            "",
            "2026-10-13",
            "2026-10-13T23:30:00",
            "2026-13-13T23:30:00Z",
            "soon",
        ] {
            assert_eq!(Moment::parse(bad), None, "{bad:?}");
        }
        assert_eq!(late.plus(60).secs - late.secs, 60);
        assert_eq!(late.plus(60).offset, late.offset);
    }
}
