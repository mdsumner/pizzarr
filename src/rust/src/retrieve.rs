//! Retrieve array subsets via zarrs.
//!
//! Implements the hot-path read function: open an array, convert R-side
//! ranges to an `ArraySubset`, dispatch on the stored data type, retrieve
//! elements, transpose from C-order to F-order, widen to R-compatible
//! types, and return `list(data, shape)`.

use std::ops::Range;

use extendr_api::prelude::*;
use zarrs::array::ArraySubset;
use zarrs_codec::CodecOptions;

use crate::array_open;
use crate::dtype_dispatch::{self, RTypeFamily};
use crate::error::PizzarrError;
use crate::transpose;

/// Retrieve a contiguous subset of an array as an R list.
///
/// # Arguments
///
/// * `store_url` - Filesystem path or URL to the store root.
/// * `array_path` - Path to the array within the store.
/// * `ranges` - R list of length-2 integer vectors `c(start, stop)`,
///   0-based, exclusive stop.
/// * `concurrent_target` - Optional codec concurrency override.
///
/// # Returns
///
/// A named list with:
/// - `data`: numeric, integer, or logical vector (F-order)
/// - `shape`: integer vector (one element per dimension)
///
/// # Errors
///
/// Returns an R error if the store/array cannot be opened, the dtype
/// is unsupported, or the retrieve operation fails.
pub(crate) fn retrieve_subset(
    store_url: &str,
    array_path: &str,
    ranges: List,
    concurrent_target: Nullable<i32>,
) -> extendr_api::Result<List> {
    let array = array_open::open_array_at_path(store_url, array_path)?;

    // Convert R list of c(start, stop) to Vec<Range<u64>>.
    let ranges_vec = parse_ranges(&ranges, store_url, array_path)?;

    // Compute output shape from ranges (each dim: stop - start).
    let shape: Vec<u64> = ranges_vec.iter().map(|r| r.end - r.start).collect();

    let subset = ArraySubset::new_with_ranges(&ranges_vec);

    // Build CodecOptions if concurrent_target is provided.
    let opts = match concurrent_target {
        Nullable::NotNull(n) => {
            CodecOptions::default().with_concurrent_target(n as usize)
        }
        Nullable::Null => CodecOptions::default(),
    };

    // Classify dtype and dispatch retrieval.
    let dt = array.data_type();
    let family = dtype_dispatch::dtype_family(dt).ok_or_else(|| PizzarrError::DTypeUnsupported {
        dtype: dt.to_string(),
    })?;

    let data = dispatch_retrieve(&array, &subset, &opts, family, &shape, store_url, array_path)?;

    let shape_i32: Vec<i32> = shape.iter().map(|&s| s as i32).collect();
    Ok(list!(data = data, shape = shape_i32))
}

/// Parse R list of `c(start, stop)` into `Vec<Range<u64>>`.
fn parse_ranges(
    ranges: &List,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<Vec<Range<u64>>, PizzarrError> {
    let mut out = Vec::with_capacity(ranges.len());
    for (_, robj) in ranges.iter() {
        let pair: Vec<i32> = robj
            .as_integer_slice()
            .ok_or_else(|| PizzarrError::Retrieve {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: "each range must be an integer vector of length 2".to_string(),
            })?
            .to_vec();
        if pair.len() != 2 {
            return Err(PizzarrError::Retrieve {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: format!("range has length {}, expected 2", pair.len()),
            });
        }
        out.push(pair[0] as u64..pair[1] as u64);
    }
    Ok(out)
}

/// Helper: retrieve elements as `Vec<T>` using the current API.
///
/// The generic parameter is the *output container* (`Vec<T>`), not
/// the element type. `Vec<T>: FromArrayBytes` requires `T: ElementOwned`.
fn retrieve_with_opts<T: zarrs::array::ElementOwned>(
    array: &zarrs::array::Array<dyn zarrs_storage::ReadableStorageTraits>,
    subset: &ArraySubset,
    opts: &CodecOptions,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<Vec<T>, PizzarrError> {
    array
        .retrieve_array_subset_opt::<Vec<T>>(subset, opts)
        .map_err(|e| PizzarrError::Retrieve {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: e.to_string(),
        })
}

/// Dispatch retrieval based on R type family, widening as needed.
///
/// Each arm retrieves as the stored type, then transposes from C-order
/// to F-order, then converts to the R-compatible type before crossing
/// the FFI boundary.
fn dispatch_retrieve(
    array: &zarrs::array::Array<dyn zarrs_storage::ReadableStorageTraits>,
    subset: &ArraySubset,
    opts: &CodecOptions,
    family: RTypeFamily,
    shape: &[u64],
    store_url: &str,
    array_path: &str,
) -> std::result::Result<Robj, PizzarrError> {
    // Macro: retrieve, transpose C→F, widen to f64 for R.
    // Decode with zarrs, then transpose C->F and convert in one pass into a
    // freshly allocated R vector (no intermediate vectors, no extra copy).
    let n: usize = shape.iter().product::<u64>() as usize;

    macro_rules! retrieve_as_double {
        ($t:ty) => {{
            let raw: Vec<$t> = retrieve_with_opts(array, subset, opts, store_url, array_path)?;
            let mut out = Doubles::new(n);
            transpose::c_to_f_into(&raw, shape, &mut out, |v| Rfloat::from(v as f64));
            Ok(out.into())
        }};
    }

    macro_rules! retrieve_as_integer {
        ($t:ty) => {{
            let raw: Vec<$t> = retrieve_with_opts(array, subset, opts, store_url, array_path)?;
            let mut out = Integers::new(n);
            transpose::c_to_f_into(&raw, shape, &mut out, |v| Rint::from(v as i32));
            Ok(out.into())
        }};
    }

    match family {
        RTypeFamily::Double => retrieve_as_double!(f64),
        RTypeFamily::Float32AsDouble => retrieve_as_double!(f32),
        RTypeFamily::Integer => retrieve_as_integer!(i32),
        RTypeFamily::Int16AsInteger => retrieve_as_integer!(i16),
        RTypeFamily::Int8AsInteger => retrieve_as_integer!(i8),
        RTypeFamily::Uint8AsInteger => retrieve_as_integer!(u8),
        RTypeFamily::Uint16AsInteger => retrieve_as_integer!(u16),
        RTypeFamily::Uint32AsDouble => retrieve_as_double!(u32),
        RTypeFamily::Int64AsDouble => retrieve_as_double!(i64),
        RTypeFamily::Uint64AsDouble => retrieve_as_double!(u64),
        RTypeFamily::Logical => {
            let raw: Vec<bool> = retrieve_with_opts(array, subset, opts, store_url, array_path)?;
            let transposed = transpose::c_to_f_order(&raw, shape);
            let logical: Vec<Rbool> = transposed.into_iter().map(Rbool::from).collect();
            Ok(logical.into_robj())
        }
    }
}
