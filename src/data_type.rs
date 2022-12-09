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

use std::sync::Arc;

use crate::common::time::timestamp::TimeUnit;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::error::{self, Error, Result};
use crate::type_id::LogicalTypeId;
use crate::types::{
    DateTimeType, Float32Type, Float64Type, Int16Type,
    // BinaryType, BooleanType, DateTimeType, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, NullType, TimestampMicrosecondType,
    // Int32Type, Int64Type, Int8Type, NullType, StringType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, TimestampType,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::value::Value;
use crate::vectors::MutableVector;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[enum_dispatch::enum_dispatch(DataType)]
pub enum ConcreteDataType {
    Null(NullType),
    // Boolean(BooleanType),

    // Numeric types:
    Int8(Int8Type),
    Int16(Int16Type),
    Int32(Int32Type),
    Int64(Int64Type),
    UInt8(UInt8Type),
    UInt16(UInt16Type),
    UInt32(UInt32Type),
    UInt64(UInt64Type),
    Float32(Float32Type),
    Float64(Float64Type),

    // String types:
    // Binary(BinaryType),
    // String(StringType),

    // Date types:
    // Date(DateType),
    DateTime(DateTimeType),
    Timestamp(TimestampType),

    // Compound types:
    // List(ListType),
}

// TODO(yingwen): Refactor these `is_xxx()` methods, such as adding a `properties()` method
// returning all these properties to the `DataType` trait
impl ConcreteDataType {
    // pub fn is_float(&self) -> bool {
    //     matches!(
    //         self,
    //         ConcreteDataType::Float64(_) | ConcreteDataType::Float32(_)
    //     )
    // }

    // pub fn is_boolean(&self) -> bool {
    //     matches!(self, ConcreteDataType::Boolean(_))
    // }

    // pub fn is_stringifiable(&self) -> bool {
    //     matches!(
    //         self,
    //         ConcreteDataType::String(_)
    //             // | ConcreteDataType::Date(_)
    //             | ConcreteDataType::DateTime(_)
    //             | ConcreteDataType::Timestamp(_)
    //     )
    // }

    // pub fn is_signed(&self) -> bool {
    //     matches!(
    //         self,
    //         ConcreteDataType::Int8(_)
    //             | ConcreteDataType::Int16(_)
    //             | ConcreteDataType::Int32(_)
    //             | ConcreteDataType::Int64(_)
    //             // | ConcreteDataType::Date(_)
    //             | ConcreteDataType::DateTime(_)
    //             | ConcreteDataType::Timestamp(_)
    //     )
    // }

    // pub fn is_unsigned(&self) -> bool {
    //     matches!(
    //         self,
    //         ConcreteDataType::UInt8(_)
    //             | ConcreteDataType::UInt16(_)
    //             | ConcreteDataType::UInt32(_)
    //             | ConcreteDataType::UInt64(_)
    //     )
    // }

    // pub fn numerics() -> Vec<ConcreteDataType> {
    //     vec![
    //         ConcreteDataType::int8_datatype(),
    //         ConcreteDataType::int16_datatype(),
    //         ConcreteDataType::int32_datatype(),
    //         ConcreteDataType::int64_datatype(),
    //         ConcreteDataType::uint8_datatype(),
    //         ConcreteDataType::uint16_datatype(),
    //         ConcreteDataType::uint32_datatype(),
    //         ConcreteDataType::uint64_datatype(),
    //         ConcreteDataType::float32_datatype(),
    //         ConcreteDataType::float64_datatype(),
    //     ]
    // }

    /// Convert arrow data type to [ConcreteDataType].
    ///
    /// # Panics
    /// Panic if given arrow data type is not supported.
    pub fn from_arrow_type(dt: &ArrowDataType) -> Self {
        ConcreteDataType::try_from(dt).expect("Unimplemented type")
    }

    // pub fn is_null(&self) -> bool {
    //     matches!(self, ConcreteDataType::Null(NullType))
    // }

    // /// Try to cast the type as a [`ListType`].
    // pub fn as_list(&self) -> Option<&ListType> {
    //     match self {
    //         ConcreteDataType::List(t) => Some(t),
    //         _ => None,
    //     }
    // }
}

impl TryFrom<&ArrowDataType> for ConcreteDataType {
    type Error = Error;

    fn try_from(dt: &ArrowDataType) -> Result<ConcreteDataType> {
        let concrete_type = match dt {
            ArrowDataType::Null => Self::null_datatype(),
            // ArrowDataType::Boolean => Self::boolean_datatype(),
            ArrowDataType::UInt8 => Self::uint8_datatype(),
            ArrowDataType::UInt16 => Self::uint16_datatype(),
            ArrowDataType::UInt32 => Self::uint32_datatype(),
            ArrowDataType::UInt64 => Self::uint64_datatype(),
            ArrowDataType::Int8 => Self::int8_datatype(),
            ArrowDataType::Int16 => Self::int16_datatype(),
            ArrowDataType::Int32 => Self::int32_datatype(),
            ArrowDataType::Int64 => Self::int64_datatype(),
            ArrowDataType::Float32 => Self::float32_datatype(),
            ArrowDataType::Float64 => Self::float64_datatype(),
            // ArrowDataType::Date32 => Self::date_datatype(),
            ArrowDataType::Date64 => Self::datetime_datatype(),
            ArrowDataType::Timestamp(u, _) => ConcreteDataType::from_arrow_time_unit(u),
            // ArrowDataType::Binary | ArrowDataType::LargeBinary => Self::binary_datatype(),
            // ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Self::string_datatype(),
            // ArrowDataType::List(field) => Self::List(ListType::new(
            //     ConcreteDataType::from_arrow_type(field.data_type()),
            // )),
            _ => {
                return error::UnsupportedArrowTypeSnafu {
                    arrow_type: dt.clone(),
                }
                .fail()
            }
        };

        Ok(concrete_type)
    }
}

macro_rules! impl_new_concrete_type_functions {
    ($($Type: ident), +) => {
        paste! {
            impl ConcreteDataType {
                $(
                    pub fn [<$Type:lower _datatype>]() -> ConcreteDataType {
                        ConcreteDataType::$Type([<$Type Type>]::default())
                    }
                )+
            }
        }
    }
}

impl_new_concrete_type_functions!(
    Null, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64,
    // Null, Boolean, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64,
    DateTime
    // Binary, Date, DateTime, String
);

impl ConcreteDataType {
    pub fn timestamp_second_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Second(TimestampSecondType::default()))
    }

    pub fn timestamp_millisecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Millisecond(
            TimestampMillisecondType::default(),
        ))
    }

    pub fn timestamp_microsecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Microsecond(
            TimestampMicrosecondType::default(),
        ))
    }

    pub fn timestamp_nanosecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Nanosecond(TimestampNanosecondType::default()))
    }

    pub fn timestamp_datatype(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::timestamp_second_datatype(),
            TimeUnit::Millisecond => Self::timestamp_millisecond_datatype(),
            TimeUnit::Microsecond => Self::timestamp_microsecond_datatype(),
            TimeUnit::Nanosecond => Self::timestamp_nanosecond_datatype(),
        }
    }

    /// Converts from arrow timestamp unit to
    pub fn from_arrow_time_unit(t: &ArrowTimeUnit) -> Self {
        match t {
            ArrowTimeUnit::Second => Self::timestamp_second_datatype(),
            ArrowTimeUnit::Millisecond => Self::timestamp_millisecond_datatype(),
            ArrowTimeUnit::Microsecond => Self::timestamp_microsecond_datatype(),
            ArrowTimeUnit::Nanosecond => Self::timestamp_nanosecond_datatype(),
        }
    }

    // pub fn list_datatype(item_type: ConcreteDataType) -> ConcreteDataType {
    //     ConcreteDataType::List(ListType::new(item_type))
    // }
}

/// Data type abstraction.
#[enum_dispatch::enum_dispatch]
pub trait DataType: std::fmt::Debug + Send + Sync {
    /// Name of this data type.
    fn name(&self) -> &str;

    /// Returns id of the Logical data type.
    fn logical_type_id(&self) -> LogicalTypeId;

    /// Returns the default value of this type.
    fn default_value(&self) -> Value;

    /// Convert this type as [arrow::datatypes::DataType].
    fn as_arrow_type(&self) -> ArrowDataType;

    /// Creates a mutable vector with given `capacity` of this type.
    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector>;

    /// Returns true if the data type is compatible with timestamp type so we can
    /// use it as a timestamp.
    fn is_timestamp_compatible(&self) -> bool;
}

pub type DataTypeRef = Arc<dyn DataType>;
