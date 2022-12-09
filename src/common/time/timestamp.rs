// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::default::Default;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use chrono::offset::Local;
use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Timestamp {
    value: i64,
    unit: TimeUnit,
}

impl Timestamp {
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { unit, value }
    }

    pub fn from_millis(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Millisecond,
        }
    }

    pub fn unit(&self) -> TimeUnit {
        self.unit
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    pub fn convert_to(&self, unit: TimeUnit) -> i64 {
        // TODO(hl): May result into overflow
        self.value * self.unit.factor() / unit.factor()
    }

    /// Format timestamp to ISO8601 string. If the timestamp exceeds what chrono timestamp can
    /// represent, this function simply print the timestamp unit and value in plain string.
    pub fn to_iso8601_string(&self) -> String {
        let nano_factor = TimeUnit::Second.factor() / TimeUnit::Nanosecond.factor();

        let mut secs = self.convert_to(TimeUnit::Second);
        let mut nsecs = self.convert_to(TimeUnit::Nanosecond) % nano_factor;

        if nsecs < 0 {
            secs -= 1;
            nsecs += nano_factor;
        }

        if let LocalResult::Single(datetime) = Utc.timestamp_opt(secs, nsecs as u32) {
            format!("{}", datetime.format("%Y-%m-%d %H:%M:%S%.f%z"))
        } else {
            format!("[Timestamp{}: {}]", self.unit, self.value)
        }
    }
}

impl FromStr for Timestamp {
    type Err = Error;

    /// Accepts a string in RFC3339 / ISO8601 standard format and some variants and converts it to a nanosecond precision timestamp.
    /// This code is copied from [arrow-datafusion](https://github.com/apache/arrow-datafusion/blob/arrow2/datafusion-physical-expr/src/arrow_temporal_util.rs#L71)
    /// with some bugfixes.
    /// Supported format:
    /// - `2022-09-20T14:16:43.012345Z` (Zulu timezone)
    /// - `2022-09-20T14:16:43.012345+08:00` (Explicit offset)
    /// - `2022-09-20T14:16:43.012345` (local timezone, with T)
    /// - `2022-09-20T14:16:43` (local timezone, no fractional seconds, with T)
    /// - `2022-09-20 14:16:43.012345Z` (Zulu timezone, without T)
    /// - `2022-09-20 14:16:43` (local timezone, without T)
    /// - `2022-09-20 14:16:43.012345` (local timezone, without T)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // RFC3339 timestamp (with a T)
        if let Ok(ts) = DateTime::parse_from_rfc3339(s) {
            return Ok(Timestamp::new(ts.timestamp_nanos(), TimeUnit::Nanosecond));
        }
        if let Ok(ts) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z") {
            return Ok(Timestamp::new(ts.timestamp_nanos(), TimeUnit::Nanosecond));
        }
        if let Ok(ts) = Utc.datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.fZ") {
            return Ok(Timestamp::new(ts.timestamp_nanos(), TimeUnit::Nanosecond));
        }

        if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            return naive_datetime_to_timestamp(s, ts);
        }

        if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
            return naive_datetime_to_timestamp(s, ts);
        }

        if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return naive_datetime_to_timestamp(s, ts);
        }

        if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
            return naive_datetime_to_timestamp(s, ts);
        }

        panic!();
    }
}

/// Converts the naive datetime (which has no specific timezone) to a
/// nanosecond epoch timestamp relative to UTC.
/// This code is copied from [arrow-datafusion](https://github.com/apache/arrow-datafusion/blob/arrow2/datafusion-physical-expr/src/arrow_temporal_util.rs#L137).
fn naive_datetime_to_timestamp(
    _s: &str,
    datetime: NaiveDateTime,
) -> crate::error::Result<Timestamp> {
    let l = Local {};

    match l.from_local_datetime(&datetime) {
        LocalResult::None => panic!(),
        LocalResult::Single(local_datetime) => Ok(Timestamp::new(
            local_datetime.with_timezone(&Utc).timestamp_nanos(),
            TimeUnit::Nanosecond,
        )),
        LocalResult::Ambiguous(local_datetime, _) => Ok(Timestamp::new(
            local_datetime.with_timezone(&Utc).timestamp_nanos(),
            TimeUnit::Nanosecond,
        )),
    }
}

impl From<i64> for Timestamp {
    fn from(v: i64) -> Self {
        Self {
            value: v,
            unit: TimeUnit::Millisecond,
        }
    }
}

impl From<Timestamp> for i64 {
    fn from(t: Timestamp) -> Self {
        t.value
    }
}

impl From<Timestamp> for serde_json::Value {
    fn from(d: Timestamp) -> Self {
        serde_json::Value::String(d.to_iso8601_string())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeUnit {
    Second,
    #[default]
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Second => {
                write!(f, "Second")
            }
            TimeUnit::Millisecond => {
                write!(f, "Millisecond")
            }
            TimeUnit::Microsecond => {
                write!(f, "Microsecond")
            }
            TimeUnit::Nanosecond => {
                write!(f, "Nanosecond")
            }
        }
    }
}

impl TimeUnit {
    pub fn factor(&self) -> i64 {
        match self {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        }
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.value * self.unit.factor()).partial_cmp(&(other.value * other.unit.factor()))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.value * self.unit.factor()).cmp(&(other.value * other.unit.factor()))
    }
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.convert_to(TimeUnit::Nanosecond) == other.convert_to(TimeUnit::Nanosecond)
    }
}

impl Eq for Timestamp {}

impl Hash for Timestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i64(self.convert_to(TimeUnit::Nanosecond));
        state.finish();
    }
}
