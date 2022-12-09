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

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use crate::common::base::{Bytes, StringBytes};
use crate::common::time::date::Date;
use crate::common::time::datetime::DateTime;
use crate::common::time::timestamp::{TimeUnit, Timestamp};
use arrow::datatypes::{DataType as ArrowDataType, Field};
use datafusion_common::ScalarValue;
pub use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error::{self, Result};
use crate::prelude::*;
use crate::type_id::LogicalTypeId;
// use crate::types::ListType;
// use crate::vectors::ListVector;

pub type OrderedF32 = OrderedFloat<f32>;
pub type OrderedF64 = OrderedFloat<f64>;

/// Value holds a single arbitrary value of any [DataType](crate::data_type::DataType).
///
/// Comparison between values with different types (expect Null) is not allowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Null,

    // Numeric types:
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedF32),
    Float64(OrderedF64),

    // String types:
    String(StringBytes),
    Binary(Bytes),

    // Date & Time types:
    // Date(Date),
    DateTime(DateTime),
    Timestamp(Timestamp),

    // List(ListValue),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "{}", self.data_type().name()),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::UInt8(v) => write!(f, "{}", v),
            Value::UInt16(v) => write!(f, "{}", v),
            Value::UInt32(v) => write!(f, "{}", v),
            Value::UInt64(v) => write!(f, "{}", v),
            Value::Int8(v) => write!(f, "{}", v),
            Value::Int16(v) => write!(f, "{}", v),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v.as_utf8()),
            Value::Binary(v) => {
                let hex = v
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<String>>()
                    .join("");
                write!(f, "{}", hex)
            }
            // Value::Date(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v),
            Value::Timestamp(v) => write!(f, "{}", v.to_iso8601_string()),
            // Value::List(v) => {
            //     let default = Box::new(vec![]);
            //     let items = v.items().as_ref().unwrap_or(&default);
            //     let items = items
            //         .iter()
            //         .map(|i| i.to_string())
            //         .collect::<Vec<String>>()
            //         .join(", ");
            //     write!(f, "{}[{}]", v.datatype.name(), items)
            // }
        }
    }
}

impl Value {
    /// Returns data type of the value.
    ///
    /// # Panics
    /// Panics if the data type is not supported.
    pub fn data_type(&self) -> ConcreteDataType {
        match self {
            Value::Null => ConcreteDataType::null_datatype(),
            Value::Boolean(_) => ConcreteDataType::boolean_datatype(),
            Value::UInt8(_) => ConcreteDataType::uint8_datatype(),
            Value::UInt16(_) => ConcreteDataType::uint16_datatype(),
            Value::UInt32(_) => ConcreteDataType::uint32_datatype(),
            Value::UInt64(_) => ConcreteDataType::uint64_datatype(),
            Value::Int8(_) => ConcreteDataType::int8_datatype(),
            Value::Int16(_) => ConcreteDataType::int16_datatype(),
            Value::Int32(_) => ConcreteDataType::int32_datatype(),
            Value::Int64(_) => ConcreteDataType::int64_datatype(),
            Value::Float32(_) => ConcreteDataType::float32_datatype(),
            Value::Float64(_) => ConcreteDataType::float64_datatype(),
            Value::String(_) => ConcreteDataType::string_datatype(),
            Value::Binary(_) => ConcreteDataType::binary_datatype(),
            // Value::Date(_) => ConcreteDataType::date_datatype(),
            Value::DateTime(_) => ConcreteDataType::datetime_datatype(),
            Value::Timestamp(v) => ConcreteDataType::timestamp_datatype(v.unit()),
            // Value::List(list) => ConcreteDataType::list_datatype(list.datatype().clone()),
        }
    }

    /// Returns true if this is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    // /// Cast itself to [ListValue].
    // pub fn as_list(&self) -> Result<Option<&ListValue>> {
    //     match self {
    //         Value::Null => Ok(None),
    //         Value::List(v) => Ok(Some(v)),
    //         other => error::CastTypeSnafu {
    //             msg: format!("Failed to cast {:?} to list value", other),
    //         }
    //         .fail(),
    //     }
    // }

    /// Cast itself to [ValueRef].
    pub fn as_value_ref(&self) -> ValueRef {
        match self {
            Value::Null => ValueRef::Null,
            Value::Boolean(v) => ValueRef::Boolean(*v),
            Value::UInt8(v) => ValueRef::UInt8(*v),
            Value::UInt16(v) => ValueRef::UInt16(*v),
            Value::UInt32(v) => ValueRef::UInt32(*v),
            Value::UInt64(v) => ValueRef::UInt64(*v),
            Value::Int8(v) => ValueRef::Int8(*v),
            Value::Int16(v) => ValueRef::Int16(*v),
            Value::Int32(v) => ValueRef::Int32(*v),
            Value::Int64(v) => ValueRef::Int64(*v),
            Value::Float32(v) => ValueRef::Float32(*v),
            Value::Float64(v) => ValueRef::Float64(*v),
            Value::String(v) => ValueRef::String(v.as_utf8()),
            Value::Binary(v) => ValueRef::Binary(v),
            // Value::Date(v) => ValueRef::Date(*v),
            Value::DateTime(v) => ValueRef::DateTime(*v),
            // Value::List(v) => ValueRef::List(ListValueRef::Ref { val: v }),
            Value::Timestamp(v) => ValueRef::Timestamp(*v),
        }
    }

    /// Returns the logical type of the value.
    pub fn logical_type_id(&self) -> LogicalTypeId {
        match self {
            Value::Null => LogicalTypeId::Null,
            Value::Boolean(_) => LogicalTypeId::Boolean,
            Value::UInt8(_) => LogicalTypeId::UInt8,
            Value::UInt16(_) => LogicalTypeId::UInt16,
            Value::UInt32(_) => LogicalTypeId::UInt32,
            Value::UInt64(_) => LogicalTypeId::UInt64,
            Value::Int8(_) => LogicalTypeId::Int8,
            Value::Int16(_) => LogicalTypeId::Int16,
            Value::Int32(_) => LogicalTypeId::Int32,
            Value::Int64(_) => LogicalTypeId::Int64,
            Value::Float32(_) => LogicalTypeId::Float32,
            Value::Float64(_) => LogicalTypeId::Float64,
            Value::String(_) => LogicalTypeId::String,
            Value::Binary(_) => LogicalTypeId::Binary,
            // Value::List(_) => LogicalTypeId::List,
            // Value::Date(_) => LogicalTypeId::Date,
            Value::DateTime(_) => LogicalTypeId::DateTime,
            Value::Timestamp(t) => match t.unit() {
                TimeUnit::Second => LogicalTypeId::TimestampSecond,
                TimeUnit::Millisecond => LogicalTypeId::TimestampMillisecond,
                TimeUnit::Microsecond => LogicalTypeId::TimestampMicrosecond,
                TimeUnit::Nanosecond => LogicalTypeId::TimestampNanosecond,
            },
        }
    }

    /// Convert the value into [`ScalarValue`] according to the `output_type`.
    pub fn try_to_scalar_value(&self, output_type: &ConcreteDataType) -> Result<ScalarValue> {
        // Compare logical type, since value might not contains full type information.
        let value_type_id = self.logical_type_id();
        let output_type_id = output_type.logical_type_id();
        ensure!(
            output_type_id == value_type_id || self.is_null(),
            error::ToScalarValueSnafu {
                reason: format!(
                    "expect value to return output_type {:?}, actual: {:?}",
                    output_type_id, value_type_id,
                ),
            }
        );

        let scalar_value = match self {
            Value::Boolean(v) => ScalarValue::Boolean(Some(*v)),
            Value::UInt8(v) => ScalarValue::UInt8(Some(*v)),
            Value::UInt16(v) => ScalarValue::UInt16(Some(*v)),
            Value::UInt32(v) => ScalarValue::UInt32(Some(*v)),
            Value::UInt64(v) => ScalarValue::UInt64(Some(*v)),
            Value::Int8(v) => ScalarValue::Int8(Some(*v)),
            Value::Int16(v) => ScalarValue::Int16(Some(*v)),
            Value::Int32(v) => ScalarValue::Int32(Some(*v)),
            Value::Int64(v) => ScalarValue::Int64(Some(*v)),
            Value::Float32(v) => ScalarValue::Float32(Some(v.0)),
            Value::Float64(v) => ScalarValue::Float64(Some(v.0)),
            Value::String(v) => ScalarValue::Utf8(Some(v.as_utf8().to_string())),
            Value::Binary(v) => ScalarValue::LargeBinary(Some(v.to_vec())),
            // Value::Date(v) => ScalarValue::Date32(Some(v.val())),
            Value::DateTime(v) => ScalarValue::Date64(Some(v.val())),
            Value::Null => to_null_value(output_type),
            // Value::List(list) => {
            //     // Safety: The logical type of the value and output_type are the same.
            //     let list_type = output_type.as_list().unwrap();
            //     list.try_to_scalar_value(list_type)?
            // }
            Value::Timestamp(t) => timestamp_to_scalar_value(t.unit(), Some(t.value())),
        };

        Ok(scalar_value)
    }
}

fn to_null_value(output_type: &ConcreteDataType) -> ScalarValue {
    match output_type {
        ConcreteDataType::Null(_) => ScalarValue::Null,
        ConcreteDataType::Boolean(_) => ScalarValue::Boolean(None),
        ConcreteDataType::Int8(_) => ScalarValue::Int8(None),
        ConcreteDataType::Int16(_) => ScalarValue::Int16(None),
        ConcreteDataType::Int32(_) => ScalarValue::Int32(None),
        ConcreteDataType::Int64(_) => ScalarValue::Int64(None),
        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(None),
        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(None),
        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(None),
        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(None),
        ConcreteDataType::Float32(_) => ScalarValue::Float32(None),
        ConcreteDataType::Float64(_) => ScalarValue::Float64(None),
        ConcreteDataType::Binary(_) => ScalarValue::LargeBinary(None),
        ConcreteDataType::String(_) => ScalarValue::Utf8(None),
        // ConcreteDataType::Date(_) => ScalarValue::Date32(None),
        ConcreteDataType::DateTime(_) => ScalarValue::Date64(None),
        ConcreteDataType::Timestamp(t) => timestamp_to_scalar_value(t.unit(), None),
        // ConcreteDataType::List(_) => {
        //     ScalarValue::List(None, Box::new(new_item_field(output_type.as_arrow_type())))
        // }
    }
}

fn new_item_field(data_type: ArrowDataType) -> Field {
    Field::new("item", data_type, false)
}

fn timestamp_to_scalar_value(unit: TimeUnit, val: Option<i64>) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(val, None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(val, None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(val, None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(val, None),
    }
}

macro_rules! impl_ord_for_value_like {
    ($Type: ident, $left: ident, $right: ident) => {
        if $left.is_null() && !$right.is_null() {
            return Ordering::Less;
        } else if !$left.is_null() && $right.is_null() {
            return Ordering::Greater;
        } else {
            match ($left, $right) {
                ($Type::Null, $Type::Null) => Ordering::Equal,
                ($Type::Boolean(v1), $Type::Boolean(v2)) => v1.cmp(v2),
                ($Type::UInt8(v1), $Type::UInt8(v2)) => v1.cmp(v2),
                ($Type::UInt16(v1), $Type::UInt16(v2)) => v1.cmp(v2),
                ($Type::UInt32(v1), $Type::UInt32(v2)) => v1.cmp(v2),
                ($Type::UInt64(v1), $Type::UInt64(v2)) => v1.cmp(v2),
                ($Type::Int8(v1), $Type::Int8(v2)) => v1.cmp(v2),
                ($Type::Int16(v1), $Type::Int16(v2)) => v1.cmp(v2),
                ($Type::Int32(v1), $Type::Int32(v2)) => v1.cmp(v2),
                ($Type::Int64(v1), $Type::Int64(v2)) => v1.cmp(v2),
                ($Type::Float32(v1), $Type::Float32(v2)) => v1.cmp(v2),
                ($Type::Float64(v1), $Type::Float64(v2)) => v1.cmp(v2),
                ($Type::String(v1), $Type::String(v2)) => v1.cmp(v2),
                ($Type::Binary(v1), $Type::Binary(v2)) => v1.cmp(v2),
                // ($Type::Date(v1), $Type::Date(v2)) => v1.cmp(v2),
                ($Type::DateTime(v1), $Type::DateTime(v2)) => v1.cmp(v2),
                ($Type::Timestamp(v1), $Type::Timestamp(v2)) => v1.cmp(v2),
                // ($Type::List(v1), $Type::List(v2)) => v1.cmp(v2),
                _ => panic!(
                    "Cannot compare different values {:?} and {:?}",
                    $left, $right
                ),
            }
        }
    };
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        impl_ord_for_value_like!(Value, self, other)
    }
}

macro_rules! impl_value_from {
    ($Variant: ident, $Type: ident) => {
        impl From<$Type> for Value {
            fn from(value: $Type) -> Self {
                Value::$Variant(value.into())
            }
        }

        impl From<Option<$Type>> for Value {
            fn from(value: Option<$Type>) -> Self {
                match value {
                    Some(v) => Value::$Variant(v.into()),
                    None => Value::Null,
                }
            }
        }
    };
}

impl_value_from!(Boolean, bool);
impl_value_from!(UInt8, u8);
impl_value_from!(UInt16, u16);
impl_value_from!(UInt32, u32);
impl_value_from!(UInt64, u64);
impl_value_from!(Int8, i8);
impl_value_from!(Int16, i16);
impl_value_from!(Int32, i32);
impl_value_from!(Int64, i64);
impl_value_from!(Float32, f32);
impl_value_from!(Float64, f64);
impl_value_from!(String, StringBytes);
impl_value_from!(Binary, Bytes);
// impl_value_from!(Date, Date);
impl_value_from!(DateTime, DateTime);
impl_value_from!(Timestamp, Timestamp);

impl From<String> for Value {
    fn from(string: String) -> Value {
        Value::String(string.into())
    }
}

impl From<&str> for Value {
    fn from(string: &str) -> Value {
        Value::String(string.into())
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Value {
        Value::Binary(bytes.into())
    }
}

impl From<&[u8]> for Value {
    fn from(bytes: &[u8]) -> Value {
        Value::Binary(bytes.into())
    }
}

impl TryFrom<Value> for serde_json::Value {
    type Error = serde_json::Error;

    fn try_from(value: Value) -> serde_json::Result<serde_json::Value> {
        let json_value = match value {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(v) => serde_json::Value::Bool(v),
            Value::UInt8(v) => serde_json::Value::from(v),
            Value::UInt16(v) => serde_json::Value::from(v),
            Value::UInt32(v) => serde_json::Value::from(v),
            Value::UInt64(v) => serde_json::Value::from(v),
            Value::Int8(v) => serde_json::Value::from(v),
            Value::Int16(v) => serde_json::Value::from(v),
            Value::Int32(v) => serde_json::Value::from(v),
            Value::Int64(v) => serde_json::Value::from(v),
            Value::Float32(v) => serde_json::Value::from(v.0),
            Value::Float64(v) => serde_json::Value::from(v.0),
            Value::String(bytes) => serde_json::Value::String(bytes.as_utf8().to_string()),
            Value::Binary(bytes) => serde_json::to_value(bytes)?,
            // Value::Date(v) => serde_json::Value::Number(v.val().into()),
            Value::DateTime(v) => serde_json::Value::Number(v.val().into()),
            // Value::List(v) => serde_json::to_value(v)?,
            Value::Timestamp(v) => serde_json::to_value(v.value())?,
        };

        Ok(json_value)
    }
}

// // TODO(yingwen): Consider removing the `datatype` field from `ListValue`.
// /// List value.
// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// pub struct ListValue {
//     /// List of nested Values (boxed to reduce size_of(Value))
//     #[allow(clippy::box_collection)]
//     items: Option<Box<Vec<Value>>>,
//     /// Inner values datatype, to distinguish empty lists of different datatypes.
//     /// Restricted by DataFusion, cannot use null datatype for empty list.
//     datatype: ConcreteDataType,
// }

// impl Eq for ListValue {}

// impl ListValue {
//     pub fn new(items: Option<Box<Vec<Value>>>, datatype: ConcreteDataType) -> Self {
//         Self { items, datatype }
//     }

//     pub fn items(&self) -> &Option<Box<Vec<Value>>> {
//         &self.items
//     }

//     pub fn datatype(&self) -> &ConcreteDataType {
//         &self.datatype
//     }

//     fn try_to_scalar_value(&self, output_type: &ListType) -> Result<ScalarValue> {
//         let vs = if let Some(items) = self.items() {
//             Some(
//                 items
//                     .iter()
//                     .map(|v| v.try_to_scalar_value(output_type.item_type()))
//                     .collect::<Result<Vec<_>>>()?,
//             )
//         } else {
//             None
//         };

//         Ok(ScalarValue::List(
//             vs,
//             Box::new(new_item_field(output_type.item_type().as_arrow_type())),
//         ))
//     }
// }

// impl Default for ListValue {
//     fn default() -> ListValue {
//         ListValue::new(None, ConcreteDataType::null_datatype())
//     }
// }

// impl PartialOrd for ListValue {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl Ord for ListValue {
//     fn cmp(&self, other: &Self) -> Ordering {
//         assert_eq!(
//             self.datatype, other.datatype,
//             "Cannot compare different datatypes!"
//         );
//         self.items.cmp(&other.items)
//     }
// }

impl TryFrom<ScalarValue> for Value {
    type Error = error::Error;

    fn try_from(v: ScalarValue) -> Result<Self> {
        let v = match v {
            ScalarValue::Null => Value::Null,
            ScalarValue::Boolean(b) => Value::from(b),
            ScalarValue::Float32(f) => Value::from(f),
            ScalarValue::Float64(f) => Value::from(f),
            ScalarValue::Int8(i) => Value::from(i),
            ScalarValue::Int16(i) => Value::from(i),
            ScalarValue::Int32(i) => Value::from(i),
            ScalarValue::Int64(i) => Value::from(i),
            ScalarValue::UInt8(u) => Value::from(u),
            ScalarValue::UInt16(u) => Value::from(u),
            ScalarValue::UInt32(u) => Value::from(u),
            ScalarValue::UInt64(u) => Value::from(u),
            ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => {
                Value::from(s.map(StringBytes::from))
            }
            ScalarValue::Binary(b)
            | ScalarValue::LargeBinary(b)
            | ScalarValue::FixedSizeBinary(_, b) => Value::from(b.map(Bytes::from)),
            // ScalarValue::List(vs, field) => {
            //     let items = if let Some(vs) = vs {
            //         let vs = vs
            //             .into_iter()
            //             .map(ScalarValue::try_into)
            //             .collect::<Result<_>>()?;
            //         Some(Box::new(vs))
            //     } else {
            //         None
            //     };
            //     let datatype = ConcreteDataType::try_from(field.data_type())?;
            //     Value::List(ListValue::new(items, datatype))
            // }
            // ScalarValue::Date32(d) => d.map(|x| Value::Date(Date::new(x))).unwrap_or(Value::Null),
            ScalarValue::Date64(d) => d
                .map(|x| Value::DateTime(DateTime::new(x)))
                .unwrap_or(Value::Null),
            ScalarValue::TimestampSecond(t, _) => t
                .map(|x| Value::Timestamp(Timestamp::new(x, TimeUnit::Second)))
                .unwrap_or(Value::Null),
            ScalarValue::TimestampMillisecond(t, _) => t
                .map(|x| Value::Timestamp(Timestamp::new(x, TimeUnit::Millisecond)))
                .unwrap_or(Value::Null),
            ScalarValue::TimestampMicrosecond(t, _) => t
                .map(|x| Value::Timestamp(Timestamp::new(x, TimeUnit::Microsecond)))
                .unwrap_or(Value::Null),
            ScalarValue::TimestampNanosecond(t, _) => t
                .map(|x| Value::Timestamp(Timestamp::new(x, TimeUnit::Nanosecond)))
                .unwrap_or(Value::Null),
            ScalarValue::Decimal128(_, _, _)
            | ScalarValue::Date32(_)
            | ScalarValue::List(_, _)
            | ScalarValue::Time64(_)
            | ScalarValue::IntervalYearMonth(_)
            | ScalarValue::IntervalDayTime(_)
            | ScalarValue::IntervalMonthDayNano(_)
            | ScalarValue::Struct(_, _)
            | ScalarValue::Dictionary(_, _) => {
                return error::UnsupportedArrowTypeSnafu {
                    arrow_type: v.get_datatype(),
                }
                .fail()
            }
        };
        Ok(v)
    }
}

/// Reference to [Value].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueRef<'a> {
    Null,

    // Numeric types:
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedF32),
    Float64(OrderedF64),

    // String types:
    String(&'a str),
    Binary(&'a [u8]),

    // Date & Time types:
    // Date(Date),
    DateTime(DateTime),
    Timestamp(Timestamp),
    // List(ListValueRef<'a>),
}

macro_rules! impl_as_for_value_ref {
    ($value: ident, $Variant: ident) => {
        match $value {
            ValueRef::Null => Ok(None),
            ValueRef::$Variant(v) => Ok(Some(*v)),
            other => error::CastTypeSnafu {
                msg: format!(
                    "Failed to cast value ref {:?} to {}",
                    other,
                    stringify!($Variant)
                ),
            }
            .fail(),
        }
    };
}

impl<'a> ValueRef<'a> {
    /// Returns true if this is null.
    pub fn is_null(&self) -> bool {
        matches!(self, ValueRef::Null)
    }

    /// Cast itself to binary slice.
    pub fn as_binary(&self) -> Result<Option<&[u8]>> {
        impl_as_for_value_ref!(self, Binary)
    }

    /// Cast itself to string slice.
    pub fn as_string(&self) -> Result<Option<&str>> {
        impl_as_for_value_ref!(self, String)
    }

    /// Cast itself to boolean.
    pub fn as_boolean(&self) -> Result<Option<bool>> {
        impl_as_for_value_ref!(self, Boolean)
    }

    // /// Cast itself to [Date].
    // pub fn as_date(&self) -> Result<Option<Date>> {
    //     impl_as_for_value_ref!(self, Date)
    // }

    /// Cast itself to [DateTime].
    pub fn as_datetime(&self) -> Result<Option<DateTime>> {
        impl_as_for_value_ref!(self, DateTime)
    }

    pub fn as_timestamp(&self) -> Result<Option<Timestamp>> {
        impl_as_for_value_ref!(self, Timestamp)
    }

    // /// Cast itself to [ListValueRef].
    // pub fn as_list(&self) -> Result<Option<ListValueRef>> {
    //     impl_as_for_value_ref!(self, List)
    // }
}

impl<'a> PartialOrd for ValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ValueRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        impl_ord_for_value_like!(ValueRef, self, other)
    }
}

macro_rules! impl_value_ref_from {
    ($Variant:ident, $Type:ident) => {
        impl From<$Type> for ValueRef<'_> {
            fn from(value: $Type) -> Self {
                ValueRef::$Variant(value.into())
            }
        }

        impl From<Option<$Type>> for ValueRef<'_> {
            fn from(value: Option<$Type>) -> Self {
                match value {
                    Some(v) => ValueRef::$Variant(v.into()),
                    None => ValueRef::Null,
                }
            }
        }
    };
}

impl_value_ref_from!(Boolean, bool);
impl_value_ref_from!(UInt8, u8);
impl_value_ref_from!(UInt16, u16);
impl_value_ref_from!(UInt32, u32);
impl_value_ref_from!(UInt64, u64);
impl_value_ref_from!(Int8, i8);
impl_value_ref_from!(Int16, i16);
impl_value_ref_from!(Int32, i32);
impl_value_ref_from!(Int64, i64);
impl_value_ref_from!(Float32, f32);
impl_value_ref_from!(Float64, f64);
// impl_value_ref_from!(Date, Date);
impl_value_ref_from!(DateTime, DateTime);
impl_value_ref_from!(Timestamp, Timestamp);

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(string: &'a str) -> ValueRef<'a> {
        ValueRef::String(string)
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(bytes: &'a [u8]) -> ValueRef<'a> {
        ValueRef::Binary(bytes)
    }
}

// impl<'a> From<Option<ListValueRef<'a>>> for ValueRef<'a> {
//     fn from(list: Option<ListValueRef>) -> ValueRef {
//         match list {
//             Some(v) => ValueRef::List(v),
//             None => ValueRef::Null,
//         }
//     }
// }

// /// Reference to a [ListValue].
// ///
// /// Now comparison still requires some allocation (call of `to_value()`) and
// /// might be avoidable by downcasting and comparing the underlying array slice
// /// if it becomes bottleneck.
// #[derive(Debug, Clone, Copy)]
// pub enum ListValueRef<'a> {
//     // TODO(yingwen): Consider replace this by VectorRef.
//     Indexed { vector: &'a ListVector, idx: usize },
//     Ref { val: &'a ListValue },
// }

// impl<'a> ListValueRef<'a> {
//     /// Convert self to [Value]. This method would clone the underlying data.
//     fn to_value(self) -> Value {
//         match self {
//             ListValueRef::Indexed { vector, idx } => vector.get(idx),
//             ListValueRef::Ref { val } => Value::List(val.clone()),
//         }
//     }
// }

// impl<'a> PartialEq for ListValueRef<'a> {
//     fn eq(&self, other: &Self) -> bool {
//         self.to_value().eq(&other.to_value())
//     }
// }

// impl<'a> Eq for ListValueRef<'a> {}

// impl<'a> Ord for ListValueRef<'a> {
//     fn cmp(&self, other: &Self) -> Ordering {
//         // Respect the order of `Value` by converting into value before comparison.
//         self.to_value().cmp(&other.to_value())
//     }
// }

// impl<'a> PartialOrd for ListValueRef<'a> {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }
