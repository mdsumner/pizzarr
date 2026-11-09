//! Stubs for the `icechunk` feature when it is not compiled in, so the R
//! wrappers always exist and fail with a clear message.

use extendr_api::prelude::*;

fn off() -> extendr_api::Error {
    crate::error::PizzarrError::FeatureNotCompiled { feature: "icechunk".to_string() }.into()
}

/// @export
#[extendr]
fn zarrs_icechunk_open_session(bytes: Raw) -> extendr_api::Result<String> { let _ = bytes; Err(off()) }
/// @export
#[extendr]
fn zarrs_icechunk_session_bytes(key: &str) -> extendr_api::Result<Raw> { let _ = key; Err(off()) }
/// @export
#[extendr]
fn zarrs_icechunk_close_session(key: &str) -> bool { let _ = key; false }
/// @export
#[extendr]
fn zarrs_set_key(store_url: &str, key: &str, value: Raw) -> extendr_api::Result<bool> { let _ = (store_url, key, value); Err(off()) }
/// @export
#[extendr]
fn zarrs_list_dir(store_url: &str, prefix: &str) -> extendr_api::Result<List> { let _ = (store_url, prefix); Err(off()) }
/// @export
#[extendr]
fn zarrs_icechunk_version() -> &'static str { "" }

extendr_module! {
    mod icechunk_handoff;
    fn zarrs_icechunk_open_session;
    fn zarrs_icechunk_session_bytes;
    fn zarrs_icechunk_close_session;
    fn zarrs_set_key;
    fn zarrs_list_dir;
    fn zarrs_icechunk_version;
}
