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

//! Vector helper functions, inspired by databend Series mod

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::compute;
use arrow::compute::kernels::comparison;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion_common::ScalarValue;
use snafu::{OptionExt, ResultExt};

use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::scalars::{Scalar, ScalarVectorBuilder};
use crate::value::{ListValue, ListValueRef};
use crate::vectors::{
    BinaryVector, BooleanVector, DateTimeVector, Float32Vector, Float64Vector,
    // BinaryVector, BooleanVector, DateTimeVector, DateVector, Float32Vector, Float64Vector,
    Int16Vector, Int32Vector, Int64Vector, Int8Vector, ListVector, ListVectorBuilder,
    MutableVector, NullVector, StringVector, TimestampMicrosecondVector,
    TimestampMillisecondVector, TimestampNanosecondVector, TimestampSecondVector, UInt16Vector,
    UInt32Vector, UInt64Vector, UInt8Vector, Vector, VectorRef,
};

/// Helper functions for `Vector`.
pub struct Helper;

impl Helper {
    /// Get a pointer to the underlying data of this vectors.
    /// Can be useful for fast comparisons.
    /// # Safety
    /// Assumes that the `vector` is  T.
    pub unsafe fn static_cast<T: Any>(vector: &VectorRef) -> &T {
        let object = vector.as_ref();
        debug_assert!(object.as_any().is::<T>());
        &*(object as *const dyn Vector as *const T)
    }

    // pub fn check_get_scalar<T: Scalar>(vector: &VectorRef) -> Result<&<T as Scalar>::VectorType> {
    //     let arr = vector
    //         .as_any()
    //         .downcast_ref::<<T as Scalar>::VectorType>()
    //         .with_context(|| error::UnknownVectorSnafu {
    //             msg: format!(
    //                 "downcast vector error, vector type: {:?}, expected vector: {:?}",
    //                 vector.vector_type_name(),
    //                 std::any::type_name::<T>(),
    //             ),
    //         });
    //     arr
    // }

    // pub fn check_get<T: 'static + Vector>(vector: &VectorRef) -> Result<&T> {
    //     let arr = vector
    //         .as_any()
    //         .downcast_ref::<T>()
    //         .with_context(|| error::UnknownVectorSnafu {
    //             msg: format!(
    //                 "downcast vector error, vector type: {:?}, expected vector: {:?}",
    //                 vector.vector_type_name(),
    //                 std::any::type_name::<T>(),
    //             ),
    //         });
    //     arr
    // }

    // pub fn check_get_mutable_vector<T: 'static + MutableVector>(
    //     vector: &mut dyn MutableVector,
    // ) -> Result<&mut T> {
    //     let ty = vector.data_type();
    //     let arr = vector
    //         .as_mut_any()
    //         .downcast_mut()
    //         .with_context(|| error::UnknownVectorSnafu {
    //             msg: format!(
    //                 "downcast vector error, vector type: {:?}, expected vector: {:?}",
    //                 ty,
    //                 std::any::type_name::<T>(),
    //             ),
    //         });
    //     arr
    // }

    // pub fn check_get_scalar_vector<T: Scalar>(
    //     vector: &VectorRef,
    // ) -> Result<&<T as Scalar>::VectorType> {
    //     let arr = vector
    //         .as_any()
    //         .downcast_ref::<<T as Scalar>::VectorType>()
    //         .with_context(|| error::UnknownVectorSnafu {
    //             msg: format!(
    //                 "downcast vector error, vector type: {:?}, expected vector: {:?}",
    //                 vector.vector_type_name(),
    //                 std::any::type_name::<T>(),
    //             ),
    //         });
    //     arr
    // }

    /// Try to cast an arrow array into vector
    ///
    /// # Panics
    /// Panic if given arrow data type is not supported.
    pub fn try_into_vector(array: impl AsRef<dyn Array>) -> Result<VectorRef> {
        Ok(match array.as_ref().data_type() {
            ArrowDataType::Null => Arc::new(NullVector::try_from_arrow_array(array)?),
            ArrowDataType::Boolean => Arc::new(BooleanVector::try_from_arrow_array(array)?),
            ArrowDataType::LargeBinary => Arc::new(BinaryVector::try_from_arrow_array(array)?),
            ArrowDataType::Int8 => Arc::new(Int8Vector::try_from_arrow_array(array)?),
            ArrowDataType::Int16 => Arc::new(Int16Vector::try_from_arrow_array(array)?),
            ArrowDataType::Int32 => Arc::new(Int32Vector::try_from_arrow_array(array)?),
            ArrowDataType::Int64 => Arc::new(Int64Vector::try_from_arrow_array(array)?),
            ArrowDataType::UInt8 => Arc::new(UInt8Vector::try_from_arrow_array(array)?),
            ArrowDataType::UInt16 => Arc::new(UInt16Vector::try_from_arrow_array(array)?),
            ArrowDataType::UInt32 => Arc::new(UInt32Vector::try_from_arrow_array(array)?),
            ArrowDataType::UInt64 => Arc::new(UInt64Vector::try_from_arrow_array(array)?),
            ArrowDataType::Float32 => Arc::new(Float32Vector::try_from_arrow_array(array)?),
            ArrowDataType::Float64 => Arc::new(Float64Vector::try_from_arrow_array(array)?),
            ArrowDataType::Utf8 => Arc::new(StringVector::try_from_arrow_array(array)?),
            // ArrowDataType::Date32 => Arc::new(DateVector::try_from_arrow_array(array)?),
            ArrowDataType::Date64 => Arc::new(DateTimeVector::try_from_arrow_array(array)?),
            ArrowDataType::List(_) => Arc::new(ListVector::try_from_arrow_array(array)?),
            ArrowDataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => Arc::new(TimestampSecondVector::try_from_arrow_array(array)?),
                TimeUnit::Millisecond => {
                    Arc::new(TimestampMillisecondVector::try_from_arrow_array(array)?)
                }
                TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondVector::try_from_arrow_array(array)?)
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondVector::try_from_arrow_array(array)?)
                }
            },
            ArrowDataType::Float16
            | ArrowDataType::Date32
            | ArrowDataType::Time32(_)
            | ArrowDataType::Time64(_)
            | ArrowDataType::Duration(_)
            | ArrowDataType::Interval(_)
            | ArrowDataType::Binary
            | ArrowDataType::FixedSizeBinary(_)
            | ArrowDataType::LargeUtf8
            | ArrowDataType::LargeList(_)
            | ArrowDataType::FixedSizeList(_, _)
            | ArrowDataType::Struct(_)
            | ArrowDataType::Union(_, _, _)
            | ArrowDataType::Dictionary(_, _)
            | ArrowDataType::Decimal128(_, _)
            | ArrowDataType::Decimal256(_, _)
            | ArrowDataType::Map(_, _) => {
                unimplemented!("Arrow array datatype: {:?}", array.as_ref().data_type())
            }
        })
    }

    // /// Try to cast slice of `arrays` to vectors.
    // pub fn try_into_vectors(arrays: &[ArrayRef]) -> Result<Vec<VectorRef>> {
    //     arrays.iter().map(Self::try_into_vector).collect()
    // }

    // /// Perform SQL like operation on `names` and a scalar `s`.
    // pub fn like_utf8(names: Vec<String>, s: &str) -> Result<VectorRef> {
    //     let array = StringArray::from(names);

    //     let filter = comparison::like_utf8_scalar(&array, s).context(error::ArrowComputeSnafu)?;

    //     let result = compute::filter(&array, &filter).context(error::ArrowComputeSnafu)?;
    //     Helper::try_into_vector(result)
    // }
}
