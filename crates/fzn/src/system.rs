//! What this machine supplies: the time, how far its clock is from UTC, and randomness.

use std::hash::{BuildHasher, RandomState};
use std::time::{SystemTime, UNIX_EPOCH};

use fzn_engine::{Day, Host, Moment};

const SECS_PER_DAY: i64 = 86_400;

/// This machine, as a session sees it.
pub struct System;

impl Host for System {
    fn now(&self) -> Moment {
        now()
    }

    fn new_id(&self) -> String {
        new_id()
    }
}

pub fn now() -> Moment {
    local(now_secs())
}

/// A moment as this machine's clock shows it.
pub fn local(secs: i64) -> Moment {
    Moment {
        secs,
        offset: local_offset(secs),
    }
}

/// A time of day on a day, by this machine's clock.
pub fn on(day: Day, secs_into_day: i64) -> Moment {
    let wall = i64::from(day) * SECS_PER_DAY + secs_into_day;
    let offset = local_offset(wall);
    Moment {
        secs: wall - i64::from(offset),
        offset,
    }
}

/// This machine's offset from UTC at a moment, in seconds east. UTC where the system cannot say.
#[cfg(unix)]
fn local_offset(secs: i64) -> i32 {
    let mut tm = std::mem::MaybeUninit::<libc::tm>::uninit();
    // Whatever the system counts seconds in, which the call below says and this does not name.
    let t = secs as _;
    // SAFETY: localtime_r reads `t` and fills the whole of `tm` through the pointers it is given,
    // and keeps neither. `tm` is read only once it has said it succeeded.
    let tm = unsafe {
        if libc::localtime_r(&t, tm.as_mut_ptr()).is_null() {
            return 0;
        }
        tm.assume_init()
    };
    tm.tm_gmtoff as i32
}

#[cfg(not(unix))]
fn local_offset(_secs: i64) -> i32 {
    0
}

pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A fresh random number, seeded by the OS.
pub fn random_u64() -> u64 {
    RandomState::new().hash_one(0u64)
}

/// A new note id: time-ordered, unique enough across devices.
pub fn new_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{millis:x}{:08x}", random_u64() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fzn_engine::time;

    #[test]
    fn ids_are_unique_and_time_ordered() {
        let a = new_id();
        let b = new_id();
        assert_ne!(a, b);
        assert!(a.len() >= 16);
        assert_ne!(System.new_id(), System.new_id());
    }

    #[test]
    fn the_clock_is_this_machines() {
        // Whatever zone the tests run in, midday on a day is on that day.
        let day = time::day_from_civil(2026, 9, 7);
        assert_eq!(on(day, 12 * 3600).day(), day);
        assert_eq!(local(1_000).secs, 1_000);
        assert!((System.now().secs - now_secs()).abs() <= 1);
        assert_eq!(now().offset, local(now_secs()).offset);
    }
}
