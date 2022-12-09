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

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

const UNIX_EPOCH_FROM_CE: i32 = 719_163;

/// ISO 8601 [Date] values. The inner representation is a signed 32 bit integer that represents the
/// **days since "1970-01-01 00:00:00 UTC" (UNIX Epoch)**.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct Date(i32);

impl From<Date> for Value {
    fn from(d: Date) -> Self {
        Value::String(d.to_string())
    }
}

impl FromStr for Date {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let date = NaiveDate::parse_from_str(s, "%F").unwrap();
        Ok(Self(date.num_days_from_ce() - UNIX_EPOCH_FROM_CE))
    }
}

impl From<i32> for Date {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl Display for Date {
    /// [Date] is formatted according to ISO-8601 standard.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(abs_date) = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_FROM_CE + self.0) {
            write!(f, "{}", abs_date.format("%F"))
        } else {
            write!(f, "Date({})", self.0)
        }
    }
}

impl Date {
    pub fn new(val: i32) -> Self {
        Self(val)
    }

    pub fn val(&self) -> i32 {
        self.0
    }
}
