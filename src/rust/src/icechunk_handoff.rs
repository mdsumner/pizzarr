//! Icechunk session handoff (feature `icechunk`).
//!
//! The `icechunk` R package owns repositories, branches, commits and
//! virtual-chunk authorisation. It hands pizzarr a session as bytes:
//!
//!   b"ICR1" | u16 LE N | N bytes ASCII icechunk crate version | rmp payload
//!
//! where the payload is `icechunk::session::Session::as_bytes()`. pizzarr
//! rebuilds the session, wraps it in `zarrs_icechunk::AsyncIcechunkStore`,
//! and registers it in the store cache under a synthetic
//! `icechunk://<snapshot>/<nonce>` key, so every zarrs code path works on
//! it unchanged. Writes accumulate in the session; the bytes go back to the
//! `icechunk` package to merge and commit. pizzarr never commits.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use extendr_api::prelude::*;
use parking_lot::Mutex;
use zarrs::storage::storage_adapter::async_to_sync::{AsyncToSyncBlockOn, AsyncToSyncStorageAdapter};
use zarrs_icechunk::AsyncIcechunkStore;

use crate::error::PizzarrError;
use crate::store_cache::{self, StorageEntry};

/// Exact icechunk crate version compiled in (Cargo.lock pins `=2.3.1`).
pub(crate) const ICECHUNK_VERSION: &str = "2.3.1";
const MAGIC: &[u8; 4] = b"ICR1";

/// Wraps [`AsyncIcechunkStore`] to fix missing-chunk handling in partial
/// reads. zarrs_icechunk 0.6.0 maps per-range results of
/// `get_partial_many` with `handle_result`, so a chunk that was never
/// written (read as fill value) surfaces as `StorageError::Other` instead
/// of `Ok(None)`. Everything else delegates.
// TODO: report upstream (zarrs/zarrs_icechunk) and drop when fixed.
pub(crate) struct HandoffStore {
    inner: Arc<AsyncIcechunkStore>,
}

mod handoff_store {
    use super::*;
    use std::result::Result;
    use futures::StreamExt;
    use icechunk::store::StoreErrorKind;
    use zarrs_storage::byte_range::{ByteRange, ByteRangeIterator};
    use zarrs_storage::{
        AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableStorageTraits,
        AsyncWritableStorageTraits, Bytes, MaybeBytes, OffsetBytesIterator, StorageError,
        StoreKey, StoreKeys, StoreKeysPrefixes, StorePrefix,
    };

    fn is_not_found(e: &icechunk::store::StoreError) -> bool {
        matches!(e.kind(), StoreErrorKind::NotFound(_))
    }

    #[async_trait::async_trait]
    impl AsyncReadableStorageTraits for HandoffStore {
        async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
            self.inner.get(key).await
        }

        async fn get_partial_many<'a>(
            &'a self,
            key: &StoreKey,
            byte_ranges: ByteRangeIterator<'a>,
        ) -> Result<AsyncMaybeBytesIterator<'a>, StorageError> {
            let ranges: Vec<_> = byte_ranges
                .map(|r| {
                    let r = match r {
                        ByteRange::FromStart(o, None) => icechunk::format::ByteRange::from_offset(o),
                        ByteRange::FromStart(o, Some(n)) => {
                            icechunk::format::ByteRange::from_offset_with_length(o, n)
                        }
                        ByteRange::Suffix(n) => icechunk::format::ByteRange::Last(n),
                    };
                    (key.to_string(), r)
                })
                .collect();
            let store = icechunk::Store::from_session(self.inner.session()).await;
            let results = match store.get_partial_values(ranges).await {
                Ok(r) => r,
                Err(e) if is_not_found(&e) => return Ok(None),
                Err(e) => return Err(StorageError::Other(e.to_string())),
            };
            // All ranges target one key: if it is absent, every range is NotFound.
            if results.iter().any(|r| matches!(r, Err(e) if is_not_found(e))) {
                return Ok(None);
            }
            let items: Vec<Result<Bytes, StorageError>> = results
                .into_iter()
                .map(|r| r.map_err(|e| StorageError::Other(e.to_string())))
                .collect();
            Ok(Some(futures::stream::iter(items).boxed()))
        }

        async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
            self.inner.size_key(key).await
        }

        fn supports_get_partial(&self) -> bool {
            self.inner.supports_get_partial()
        }
    }

    #[async_trait::async_trait]
    impl AsyncWritableStorageTraits for HandoffStore {
        async fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
            self.inner.set(key, value).await
        }
        async fn set_partial_many<'a>(
            &'a self,
            key: &StoreKey,
            offset_values: OffsetBytesIterator<'a>,
        ) -> Result<(), StorageError> {
            self.inner.set_partial_many(key, offset_values).await
        }
        async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
            self.inner.erase(key).await
        }
        async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
            self.inner.erase_prefix(prefix).await
        }
        fn supports_set_partial(&self) -> bool {
            self.inner.supports_set_partial()
        }
    }

    #[async_trait::async_trait]
    impl AsyncListableStorageTraits for HandoffStore {
        async fn list(&self) -> Result<StoreKeys, StorageError> {
            self.inner.list().await
        }
        async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
            self.inner.list_prefix(prefix).await
        }
        async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
            self.inner.list_dir(prefix).await
        }
        async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
            self.inner.size_prefix(prefix).await
        }
        async fn size(&self) -> Result<u64, StorageError> {
            self.inner.size().await
        }
    }
}

static SESSIONS: LazyLock<Mutex<HashMap<String, Arc<AsyncIcechunkStore>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static NONCE: AtomicU64 = AtomicU64::new(1);

fn err(reason: impl Into<String>) -> extendr_api::Error {
    PizzarrError::StoreOpen { url: "icechunk session".into(), reason: reason.into() }.into()
}

fn decode(b: &[u8]) -> std::result::Result<&[u8], String> {
    if b.len() < 6 || &b[0..4] != MAGIC {
        return Err("not icechunk session bytes (missing ICR1 header)".into());
    }
    let n = u16::from_le_bytes([b[4], b[5]]) as usize;
    if b.len() < 6 + n {
        return Err("truncated icechunk session header".into());
    }
    let v = std::str::from_utf8(&b[6..6 + n]).map_err(|_| "bad version string".to_string())?;
    if v != ICECHUNK_VERSION {
        return Err(format!(
            "session bytes were written by icechunk {v} but pizzarr was built with icechunk {ICECHUNK_VERSION}; \
             install matching versions of the icechunk and pizzarr packages"
        ));
    }
    Ok(&b[6 + n..])
}

fn encode(payload: &[u8]) -> Vec<u8> {
    let v = ICECHUNK_VERSION.as_bytes();
    let mut out = Vec::with_capacity(6 + v.len() + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(v.len() as u16).to_le_bytes());
    out.extend_from_slice(v);
    out.extend_from_slice(payload);
    out
}

/// Register an icechunk session (bytes from `icechunk::ic_session_bytes()`)
/// and return the store key to use as `store_url` with every zarrs_*
/// function. Call `zarrs_close_store(key)` when done.
/// @param bytes raw vector with the ICR1 header.
/// @return character store key, `icechunk://<snapshot>/<nonce>`.
/// @export
#[extendr]
fn zarrs_icechunk_open_session(bytes: Raw) -> extendr_api::Result<String> {
    let payload = decode(bytes.as_slice()).map_err(err)?;
    let session = icechunk::session::Session::from_bytes(payload).map_err(|e| err(e.to_string()))?;
    let snap = session.snapshot_id().to_string();
    let key = format!("icechunk://{snap}/{}", NONCE.fetch_add(1, Ordering::Relaxed));
    let store = Arc::new(AsyncIcechunkStore::new(session));
    let wrapped = Arc::new(HandoffStore { inner: Arc::clone(&store) });
    let sync = Arc::new(AsyncToSyncStorageAdapter::new(wrapped, crate::runtime::TokioBlockOn));
    store_cache::insert(&key, StorageEntry::ReadWriteList(sync));
    SESSIONS.lock().insert(key.clone(), store);
    Ok(key)
}

/// Serialise the session behind a registered key (including any chunks
/// pizzarr wrote) for `icechunk::ic_session_merge()`.
/// @param key store key from `zarrs_icechunk_open_session()`.
/// @return raw vector with the ICR1 header.
/// @export
#[extendr]
fn zarrs_icechunk_session_bytes(key: &str) -> extendr_api::Result<Raw> {
    let store = SESSIONS
        .lock()
        .get(key)
        .cloned()
        .ok_or_else(|| err(format!("no icechunk session registered under {key}")))?;
    let session = store.session();
    let payload = crate::runtime::TokioBlockOn.block_on(async move {
        session.read().await.as_bytes()
    })
    .map_err(|e| err(e.to_string()))?;
    Ok(Raw::from_bytes(&encode(&payload)))
}

/// Forget a registered icechunk session (also removes it from the store cache).
/// @param key store key.
/// @return `TRUE` if it was registered.
/// @export
#[extendr]
fn zarrs_icechunk_close_session(key: &str) -> bool {
    store_cache::remove(key);
    SESSIONS.lock().remove(key).is_some()
}

/// Write one key's bytes through a registered store (metadata writes from
/// the R side).
/// @param store_url store key.
/// @param key zarr key.
/// @param value raw vector.
/// @export
#[extendr]
fn zarrs_set_key(store_url: &str, key: &str, value: Raw) -> extendr_api::Result<bool> {
    use zarrs_storage::WritableStorageTraits;
    let store = crate::store_open::open_store(store_url)?;
    let w = store.as_readable_writable_listable(store_url)?;
    let k = zarrs_storage::StoreKey::new(key).map_err(|e| err(e.to_string()))?;
    w.set(&k, zarrs_storage::Bytes::copy_from_slice(value.as_slice()))
        .map_err(|e| err(e.to_string()))?;
    Ok(true)
}

/// Erase one key in a registered store.
/// @param store_url store key.
/// @param key zarr key.
/// @export
#[extendr]
fn zarrs_erase_key(store_url: &str, key: &str) -> extendr_api::Result<bool> {
    use zarrs_storage::WritableStorageTraits;
    let store = crate::store_open::open_store(store_url)?;
    let w = store.as_readable_writable_listable(store_url)?;
    let k = zarrs_storage::StoreKey::new(key).map_err(|e| err(e.to_string()))?;
    w.erase(&k).map_err(|e| err(e.to_string()))?;
    Ok(true)
}

/// Erase every key under a prefix in a registered store.
/// @param store_url store key.
/// @param prefix "" for the root, else ending in "/".
/// @export
#[extendr]
fn zarrs_erase_prefix(store_url: &str, prefix: &str) -> extendr_api::Result<bool> {
    use zarrs_storage::WritableStorageTraits;
    let store = crate::store_open::open_store(store_url)?;
    let w = store.as_readable_writable_listable(store_url)?;
    let p = zarrs_storage::StorePrefix::new(prefix).map_err(|e| err(e.to_string()))?;
    w.erase_prefix(&p).map_err(|e| err(e.to_string()))?;
    Ok(true)
}

/// List keys and prefixes one level below `prefix` in a registered store.
/// @param store_url store key.
/// @param prefix "" for the root, else ending in "/".
/// @export
#[extendr]
fn zarrs_list_dir(store_url: &str, prefix: &str) -> extendr_api::Result<List> {
    use zarrs_storage::ListableStorageTraits;
    let store = crate::store_open::open_store(store_url)?;
    let l = store.as_readable_writable_listable(store_url)?;
    let p = zarrs_storage::StorePrefix::new(prefix).map_err(|e| err(e.to_string()))?;
    let r = l.list_dir(&p).map_err(|e| err(e.to_string()))?;
    let keys: Vec<String> = r.keys().iter().map(|k| k.as_str().to_string()).collect();
    let prefixes: Vec<String> = r.prefixes().iter().map(|k| k.as_str().to_string()).collect();
    Ok(list!(keys = keys, prefixes = prefixes))
}

/// Icechunk crate version compiled into pizzarr.
/// @export
#[extendr]
fn zarrs_icechunk_version() -> &'static str {
    ICECHUNK_VERSION
}

extendr_module! {
    mod icechunk_handoff;
    fn zarrs_icechunk_open_session;
    fn zarrs_icechunk_session_bytes;
    fn zarrs_icechunk_close_session;
    fn zarrs_set_key;
    fn zarrs_erase_key;
    fn zarrs_erase_prefix;
    fn zarrs_list_dir;
    fn zarrs_icechunk_version;
}
