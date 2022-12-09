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

use crate::common::time::timestamp::TimeUnit;
use crate::common::time::Timestamp;
use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::DataType;
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error::{ConversionSnafu, Result};
use crate::value::{ListValue, Value};

pub type BinaryArray = arrow::array::LargeBinaryArray;
pub type MutableBinaryArray = arrow::array::LargeBinaryBuilder;
pub type StringArray = arrow::array::StringArray;
pub type MutableStringArray = arrow::array::StringBuilder;
