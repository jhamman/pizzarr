//! Read-only Icechunk repositories (the `icechunk` Cargo feature).
//!
//! [`open_icechunk`] builds the Icechunk `Storage` from a URL and options,
//! opens the `Repository` with the requested virtual chunk containers,
//! starts a read-only session at a branch, tag or snapshot, and caches it
//! under an `icechunk+<url>@<kind>:<value>` identifier. Every other zarrs
//! entry point then finds the session in the store cache by that
//! identifier, exactly like a URL-backed store.
//!
//! The session is wrapped in [`zarrs_icechunk::AsyncIcechunkStore`] and
//! pizzarr's async-to-sync adapter on the shared tokio runtime.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use icechunk::config::{
    AzureCredentials, AzureStaticCredentials, Credentials, GcsCredentials, GcsStaticCredentials,
    S3Credentials, S3Options, S3StaticCredentials,
};
use icechunk::format::{ByteRange as IcByteRange, SnapshotId};
use icechunk::repository::VersionInfo;
use icechunk::storage::{
    new_azure_blob_storage, new_gcs_storage, new_http_storage, new_local_filesystem_storage,
    new_s3_object_store_storage,
};
use icechunk::virtual_chunks::VirtualChunkContainer;
use icechunk::{ObjectStoreConfig, Repository, RepositoryConfig, Storage};
use serde::Deserialize;
use zarrs::storage::storage_adapter::async_to_sync::{
    AsyncToSyncBlockOn, AsyncToSyncStorageAdapter,
};
use zarrs_icechunk::AsyncIcechunkStore;
use zarrs_storage::byte_range::{ByteRange, ByteRangeIterator};
use zarrs_storage::{
    AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableStorageTraits, MaybeBytes,
    StorageError, StoreKey, StoreKeys, StoreKeysPrefixes, StorePrefix,
};

use crate::error::PizzarrError;
use crate::runtime::TokioBlockOn;
use crate::store_cache::{self, StorageEntry};

/// Identifier prefix for Icechunk stores in the store cache.
pub(crate) const SCHEME_PREFIX: &str = "icechunk+";

/// Storage and credential options, shared by the repository storage and
/// virtual chunk containers. Unknown names are rejected so typos surface.
#[derive(Debug, Default, Deserialize, Hash, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreOptions {
    // S3 and S3-compatible.
    region: Option<String>,
    endpoint_url: Option<String>,
    allow_http: Option<bool>,
    force_path_style: Option<bool>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    // Any backend: unsigned requests.
    anonymous: Option<bool>,
    // GCS.
    service_account_file: Option<String>,
    service_account_key: Option<String>,
    application_credentials: Option<String>,
    bearer_token: Option<String>,
    // Azure.
    account: Option<String>,
    access_key: Option<String>,
    sas_token: Option<String>,
    // Virtual chunk containers: register the container under this name, so
    // relative `vcc://name/...` references resolve to this prefix.
    name: Option<String>,
}

/// Which version of the repository to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefSpec {
    /// Tip of a branch.
    Branch(String),
    /// A tag.
    Tag(String),
    /// A snapshot id (Crockford base32).
    Snapshot(String),
}

impl RefSpec {
    /// Parse the `kind` / `value` pair passed from R.
    pub(crate) fn parse(kind: &str, value: &str) -> Result<Self, PizzarrError> {
        if value.is_empty() {
            return Err(PizzarrError::IcechunkOpen {
                url: String::new(),
                reason: format!("empty {kind} name"),
            });
        }
        match kind {
            "branch" => Ok(Self::Branch(value.to_string())),
            "tag" => Ok(Self::Tag(value.to_string())),
            "snapshot" => Ok(Self::Snapshot(value.to_string())),
            other => Err(PizzarrError::IcechunkOpen {
                url: String::new(),
                reason: format!("unknown ref kind {other:?}; use branch, tag or snapshot"),
            }),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Branch(_) => "branch",
            Self::Tag(_) => "tag",
            Self::Snapshot(_) => "snapshot",
        }
    }

    fn value(&self) -> &str {
        match self {
            Self::Branch(v) | Self::Tag(v) | Self::Snapshot(v) => v,
        }
    }

    fn version_info(&self, url: &str) -> Result<VersionInfo, PizzarrError> {
        Ok(match self {
            Self::Branch(b) => VersionInfo::BranchTipRef(b.clone()),
            Self::Tag(t) => VersionInfo::TagRef(t.clone()),
            Self::Snapshot(s) => {
                VersionInfo::SnapshotId(SnapshotId::try_from(s.as_str()).map_err(|_| {
                    PizzarrError::IcechunkRefNotFound {
                        url: url.to_string(),
                        reference: format!("snapshot {s:?} (not a valid snapshot id)"),
                    }
                })?)
            }
        })
    }
}

/// Where the repository lives, parsed from its URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Location {
    /// A local directory (bare path or `file://`).
    Local(String),
    /// `s3://bucket/prefix`.
    S3 {
        bucket: String,
        prefix: Option<String>,
    },
    /// `gs://bucket/prefix` (or `gcs://`).
    Gcs {
        bucket: String,
        prefix: Option<String>,
    },
    /// `az://container/prefix` (or `azure://`, `abfs://`).
    Azure {
        container: String,
        prefix: Option<String>,
    },
    /// `http(s)://...`, read-only.
    Http(String),
}

impl Location {
    /// Parse a repository URL or local path.
    pub(crate) fn parse(url: &str) -> Result<Self, PizzarrError> {
        let bad = |reason: &str| PizzarrError::IcechunkOpen {
            url: url.to_string(),
            reason: reason.to_string(),
        };
        let trimmed = url.trim_end_matches('/');
        if trimmed.is_empty() {
            return Err(bad("empty repository URL"));
        }
        let Some((scheme, rest)) = trimmed.split_once("://") else {
            return Ok(Self::Local(trimmed.to_string()));
        };
        let bucket_prefix = || -> Result<(String, Option<String>), PizzarrError> {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(bad("URL has no bucket or container name"));
            }
            let prefix = prefix.trim_matches('/');
            Ok((
                bucket.to_string(),
                (!prefix.is_empty()).then(|| prefix.to_string()),
            ))
        };
        match scheme.to_ascii_lowercase().as_str() {
            "file" => Ok(Self::Local(file_url_to_path(rest))),
            "s3" => {
                let (bucket, prefix) = bucket_prefix()?;
                Ok(Self::S3 { bucket, prefix })
            }
            "gs" | "gcs" => {
                let (bucket, prefix) = bucket_prefix()?;
                Ok(Self::Gcs { bucket, prefix })
            }
            "az" | "azure" | "abfs" => {
                let (container, prefix) = bucket_prefix()?;
                Ok(Self::Azure { container, prefix })
            }
            "http" | "https" => Ok(Self::Http(trimmed.to_string())),
            other => Err(bad(&format!(
                "unsupported URL scheme {other:?}; use a local path, s3://, gs://, az:// or https://"
            ))),
        }
    }

    /// Canonical URL used in the store identifier.
    fn canonical(&self) -> String {
        let with_prefix = |scheme: &str, bucket: &str, prefix: &Option<String>| match prefix {
            Some(p) => format!("{scheme}://{bucket}/{p}"),
            None => format!("{scheme}://{bucket}"),
        };
        match self {
            Self::Local(path) => {
                let abs = std::path::Path::new(path)
                    .canonicalize()
                    .map_or_else(|_| path.clone(), |p| p.to_string_lossy().to_string());
                let abs = abs.replace('\\', "/");
                if abs.starts_with('/') {
                    format!("file://{abs}")
                } else {
                    format!("file:///{abs}")
                }
            }
            Self::S3 { bucket, prefix } => with_prefix("s3", bucket, prefix),
            Self::Gcs { bucket, prefix } => with_prefix("gs", bucket, prefix),
            Self::Azure { container, prefix } => with_prefix("az", container, prefix),
            Self::Http(url) => url.clone(),
        }
    }
}

/// `file:///path` → `/path` (and `file:///C:/x` → `C:/x` on Windows).
fn file_url_to_path(rest: &str) -> String {
    let b = rest.as_bytes();
    if cfg!(windows) && b.len() >= 3 && b[0] == b'/' && b[2] == b':' {
        rest[1..].to_string()
    } else {
        rest.to_string()
    }
}

/// The store-cache identifier for a repository at a ref.
///
/// Options that change what is read (credentials, endpoints, virtual chunk
/// containers) are folded in as a short hash, so stores opened with
/// different options do not share a cache entry and secrets never appear
/// in the identifier.
pub(crate) fn store_identifier(
    location: &Location,
    reference: &RefSpec,
    options: &StoreOptions,
    containers: &BTreeMap<String, StoreOptions>,
) -> String {
    let mut id = format!(
        "{SCHEME_PREFIX}{}@{}:{}",
        location.canonical(),
        reference.kind(),
        reference.value()
    );
    if *options != StoreOptions::default() || !containers.is_empty() {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        options.hash(&mut hasher);
        containers.hash(&mut hasher);
        let _ = write!(id, "#{:016x}", hasher.finish());
    }
    id
}

/// Parse a JSON object of options. `""`, `"{}"`, `"[]"` and `"null"` are empty.
fn parse_json<T: for<'de> Deserialize<'de> + Default>(
    what: &str,
    json: &str,
) -> Result<T, PizzarrError> {
    let trimmed = json.trim();
    if trimmed.is_empty() || trimmed == "[]" || trimmed == "null" {
        return Ok(T::default());
    }
    serde_json::from_str(trimmed).map_err(|e| PizzarrError::IcechunkOpen {
        url: String::new(),
        reason: format!("invalid {what}: {e}"),
    })
}

// ---------------------------------------------------------------------------
// Credentials and storage
// ---------------------------------------------------------------------------

fn s3_options(o: &StoreOptions) -> S3Options {
    // `S3Options` is non-exhaustive: start from its defaults.
    let mut opts = S3Options::default();
    opts.region.clone_from(&o.region);
    opts.endpoint_url.clone_from(&o.endpoint_url);
    opts.anonymous = o.anonymous.unwrap_or(false);
    opts.allow_http = o.allow_http.unwrap_or(false);
    // Custom endpoints (MinIO, Ceph) usually need path-style addressing.
    opts.force_path_style = o.force_path_style.unwrap_or(o.endpoint_url.is_some());
    opts
}

fn s3_credentials(o: &StoreOptions) -> Result<S3Credentials, String> {
    if o.anonymous == Some(true) {
        return Ok(S3Credentials::Anonymous);
    }
    match (&o.access_key_id, &o.secret_access_key) {
        (Some(id), Some(secret)) => Ok(S3Credentials::Static(S3StaticCredentials {
            access_key_id: id.clone(),
            secret_access_key: secret.clone(),
            session_token: o.session_token.clone(),
            expires_after: None,
        })),
        (None, None) => Ok(S3Credentials::FromEnv),
        _ => Err("access_key_id and secret_access_key must be given together".into()),
    }
}

fn gcs_credentials(o: &StoreOptions) -> GcsCredentials {
    if o.anonymous == Some(true) {
        GcsCredentials::Anonymous
    } else if let Some(path) = &o.service_account_file {
        GcsCredentials::Static(GcsStaticCredentials::ServiceAccount(path.into()))
    } else if let Some(key) = &o.service_account_key {
        GcsCredentials::Static(GcsStaticCredentials::ServiceAccountKey(key.clone()))
    } else if let Some(path) = &o.application_credentials {
        GcsCredentials::Static(GcsStaticCredentials::ApplicationCredentials(path.into()))
    } else if let Some(token) = &o.bearer_token {
        GcsCredentials::Static(GcsStaticCredentials::BearerToken(
            icechunk::config::GcsBearerCredential {
                bearer: token.clone(),
                expires_after: None,
            },
        ))
    } else {
        GcsCredentials::FromEnv
    }
}

/// GCS client options (`object_store` `GoogleConfigKey` names).
fn gcs_config(o: &StoreOptions) -> HashMap<String, String> {
    let mut config = HashMap::new();
    if let Some(endpoint) = &o.endpoint_url {
        config.insert("google_base_url".to_string(), endpoint.clone());
    }
    config
}

fn azure_credentials(o: &StoreOptions) -> AzureCredentials {
    if o.anonymous == Some(true) {
        AzureCredentials::Anonymous
    } else if let Some(key) = &o.access_key {
        AzureCredentials::Static(AzureStaticCredentials::AccessKey(key.clone()))
    } else if let Some(sas) = &o.sas_token {
        AzureCredentials::Static(AzureStaticCredentials::SASToken(sas.clone()))
    } else if let Some(token) = &o.bearer_token {
        AzureCredentials::Static(AzureStaticCredentials::BearerToken(token.clone()))
    } else {
        AzureCredentials::FromEnv
    }
}

/// Build the repository storage.
async fn build_storage(
    url: &str,
    location: &Location,
    o: &StoreOptions,
) -> Result<Arc<dyn Storage + Send + Sync>, PizzarrError> {
    let fail = |reason: String| PizzarrError::IcechunkOpen {
        url: url.to_string(),
        reason,
    };
    let storage_err = |e: icechunk::StorageError| fail(e.kind().to_string());
    match location {
        Location::Local(path) => {
            let p = std::path::Path::new(path);
            if !p.is_dir() {
                return Err(fail(format!("no repository directory at {path}")));
            }
            new_local_filesystem_storage(p).await.map_err(storage_err)
        }
        Location::S3 { bucket, prefix } => new_s3_object_store_storage(
            s3_options(o),
            bucket.clone(),
            prefix.clone(),
            Some(s3_credentials(o).map_err(fail)?),
            vec![],
            vec![],
        )
        .await
        .map_err(storage_err),
        Location::Gcs { bucket, prefix } => new_gcs_storage(
            bucket.clone(),
            prefix.clone(),
            Some(gcs_credentials(o)),
            Some(gcs_config(o)),
            vec![],
            vec![],
        )
        .map_err(storage_err),
        Location::Azure { container, prefix } => {
            let account = o
                .account
                .clone()
                .ok_or_else(|| fail("az:// URLs need storage_options$account".into()))?;
            new_azure_blob_storage(
                account,
                container.clone(),
                prefix.clone(),
                Some(azure_credentials(o)),
                None,
            )
            .await
            .map_err(storage_err)
        }
        Location::Http(base) => new_http_storage(base, None, None).map_err(storage_err),
    }
}

/// The credential that authorizes a virtual chunk container prefix.
fn container_credentials(prefix: &str, o: &StoreOptions) -> Result<Credentials, String> {
    let scheme = prefix.split_once("://").map_or("", |(s, _)| s);
    match scheme {
        "s3" => Ok(Credentials::S3(s3_credentials(o)?)),
        "gs" | "gcs" => Ok(Credentials::Gcs(gcs_credentials(o))),
        "az" | "azure" | "abfs" => Ok(Credentials::Azure(azure_credentials(o))),
        "file" => Ok(Credentials::LocalFileSystemAccess),
        "http" | "https" => Ok(Credentials::HttpAccess),
        _ => Err(format!(
            "unsupported virtual chunk container prefix {prefix:?}; \
             use s3://, gs://, az://, file:// or http(s)://"
        )),
    }
}

/// A container definition for a prefix the repository config doesn't know.
fn container_store_config(prefix: &str, o: &StoreOptions) -> Result<ObjectStoreConfig, String> {
    let scheme = prefix.split_once("://").map_or("", |(s, _)| s);
    match scheme {
        "s3" => Ok(ObjectStoreConfig::S3Compatible(s3_options(o))),
        "gs" | "gcs" => Ok(ObjectStoreConfig::Gcs(gcs_config(o))),
        "az" | "azure" | "abfs" => {
            let mut config = HashMap::new();
            let account = o
                .account
                .clone()
                .ok_or("Azure virtual chunk containers need an `account` option")?;
            config.insert("account".to_string(), account);
            Ok(ObjectStoreConfig::Azure(config))
        }
        "file" => Ok(ObjectStoreConfig::LocalFileSystem(
            file_url_to_path(prefix.trim_start_matches("file://")).into(),
        )),
        "http" | "https" => Ok(ObjectStoreConfig::Http(
            icechunk::config::HttpConfig::default(),
        )),
        _ => Err(format!(
            "unsupported virtual chunk container prefix {prefix:?}"
        )),
    }
}

/// Open the repository, authorizing the given virtual chunk containers.
///
/// Containers the repository config already declares (by prefix) are only
/// authorized. Others, and any given a `name`, are added to the runtime
/// config first, built from their options.
async fn open_repository(
    url: &str,
    storage: Arc<dyn Storage + Send + Sync>,
    containers: &BTreeMap<String, StoreOptions>,
) -> Result<Repository, PizzarrError> {
    let fail = |reason: String| PizzarrError::IcechunkOpen {
        url: url.to_string(),
        reason,
    };
    let mut authorize = HashMap::new();
    for (prefix, o) in containers {
        if !prefix.ends_with('/') {
            return Err(fail(format!(
                "virtual chunk container prefix {prefix:?} must end in '/'"
            )));
        }
        authorize.insert(
            prefix.clone(),
            Some(container_credentials(prefix, o).map_err(fail)?),
        );
    }

    let repo = Repository::open(storage)
        .execute()
        .await
        .map_err(|e| fail(e.kind().to_string()))?;

    let needs_definition: Vec<_> = containers
        .iter()
        .filter(|(prefix, o)| {
            o.name.is_some() || repo.config().get_virtual_chunk_container(prefix).is_none()
        })
        .collect();

    // Persisted containers are replaced wholesale by a runtime list, so
    // start from them and add ours on top.
    let mut config = None;
    if !needs_definition.is_empty() {
        let mut cfg = RepositoryConfig::default();
        for c in repo.config().virtual_chunk_containers() {
            cfg.set_virtual_chunk_container(c.clone()).map_err(fail)?;
        }
        for (prefix, o) in needs_definition {
            let store = container_store_config(prefix, o).map_err(fail)?;
            let container = match &o.name {
                Some(name) => VirtualChunkContainer::new_named(name.clone(), prefix.clone(), store),
                None => VirtualChunkContainer::new(prefix.clone(), store),
            }
            .map_err(|e| fail(format!("virtual chunk container {prefix:?}: {e}")))?;
            cfg.set_virtual_chunk_container(container).map_err(fail)?;
        }
        config = Some(cfg);
    }

    repo.reopen(config, Some(authorize))
        .await
        .map_err(|e| fail(e.kind().to_string()))
}

/// Open a read-only Icechunk session and cache it.
///
/// Always (re)opens: a branch is resolved to its current tip, replacing
/// any session cached under the same identifier. Returns the identifier.
///
/// # Errors
///
/// [`PizzarrError::IcechunkOpen`] for a bad URL, bad options or a
/// repository that cannot be opened; [`PizzarrError::IcechunkRefNotFound`]
/// for an unknown branch, tag or snapshot.
pub(crate) fn open_icechunk(
    url: &str,
    reference: &RefSpec,
    options_json: &str,
    containers_json: &str,
) -> Result<String, PizzarrError> {
    let location = Location::parse(url)?;
    let with_url = |e: PizzarrError| match e {
        PizzarrError::IcechunkOpen { url: u, reason } if u.is_empty() => {
            PizzarrError::IcechunkOpen {
                url: url.to_string(),
                reason,
            }
        }
        other => other,
    };
    let options: StoreOptions = parse_json("storage_options", options_json).map_err(with_url)?;
    let containers: BTreeMap<String, StoreOptions> =
        parse_json("virtual_chunk_containers", containers_json).map_err(with_url)?;
    let id = store_identifier(&location, reference, &options, &containers);
    let version = reference.version_info(url)?;

    let session = TokioBlockOn.block_on(async {
        let storage = build_storage(url, &location, &options).await?;
        let repo = open_repository(url, storage, &containers).await?;
        repo.readonly_session(&version).await.map_err(|e| {
            let reason = e.kind().to_string();
            if is_ref_not_found(e.kind()) {
                PizzarrError::IcechunkRefNotFound {
                    url: url.to_string(),
                    reference: format!("{} {:?}", reference.kind(), reference.value()),
                }
            } else {
                PizzarrError::IcechunkOpen {
                    url: url.to_string(),
                    reason,
                }
            }
        })
    })?;

    let store = Arc::new(IcechunkReadStore {
        inner: AsyncIcechunkStore::new(session),
    });
    let sync = Arc::new(AsyncToSyncStorageAdapter::new(store, TokioBlockOn));
    store_cache::replace(&id, StorageEntry::ReadList(sync));
    Ok(id)
}

fn is_ref_not_found(kind: &icechunk::repository::RepositoryErrorKind) -> bool {
    use icechunk::format::IcechunkFormatErrorKind as F;
    use icechunk::refs::RefErrorKind;
    use icechunk::repository::RepositoryErrorKind as K;
    matches!(
        kind,
        K::Ref(RefErrorKind::RefNotFound(_))
            | K::SnapshotNotFound { .. }
            | K::FormatError(F::SnapshotIdNotFound { .. })
    )
}

// ---------------------------------------------------------------------------
// Store wrapper
// ---------------------------------------------------------------------------

/// [`AsyncIcechunkStore`] with two read fixes for `zarrs_icechunk` 0.6.0.
///
/// Upstream bug (not yet filed): `get_partial_many` maps "key not found"
/// to `None` only when the whole `get_partial_values` call fails, but
/// icechunk reports a missing chunk per range, so a never-written chunk
/// becomes an error. zarrs' partial decoders (part of a shard, sparse
/// sharded arrays) then fail instead of using the fill value. Likewise
/// `size_key` turns a missing key into an error instead of `None`. Both
/// are re-implemented here; everything else is delegated. Drop this
/// wrapper once a fixed `zarrs_icechunk` is released.
struct IcechunkReadStore {
    inner: AsyncIcechunkStore,
}

impl IcechunkReadStore {
    async fn store(&self) -> icechunk::Store {
        icechunk::Store::from_session(self.inner.session()).await
    }
}

fn is_not_found(e: &icechunk::store::StoreError) -> bool {
    matches!(e.kind(), icechunk::store::StoreErrorKind::NotFound { .. })
}

/// An icechunk error as a zarrs storage error, without the span trace and
/// with a pointer to the R option when a virtual chunk location is not
/// authorized.
fn storage_err(message: &str) -> StorageError {
    let message = message
        .split("\n\ncontext:")
        .next()
        .unwrap_or(message)
        .trim_end();
    if message.contains("authorize the virtual chunk container")
        || message.contains("no virtual chunk container can handle")
    {
        StorageError::Other(format!(
            "unauthorized virtual chunk location: {message}. Authorize it with \
             IcechunkStore$new(..., virtual_chunk_containers = list(\"<prefix>/\" = list(...)))"
        ))
    } else {
        StorageError::Other(message.to_string())
    }
}

fn map_storage_err(e: StorageError) -> StorageError {
    match e {
        StorageError::Other(m) => storage_err(&m),
        other => other,
    }
}

#[async_trait::async_trait]
impl AsyncReadableStorageTraits for IcechunkReadStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        self.inner.get(key).await.map_err(map_storage_err)
    }

    async fn get_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<AsyncMaybeBytesIterator<'a>, StorageError> {
        use futures::StreamExt as _;
        let ranges: Vec<_> = byte_ranges
            .map(|r| {
                let r = match r {
                    ByteRange::FromStart(o, None) => IcByteRange::from_offset(o),
                    ByteRange::FromStart(o, Some(l)) => IcByteRange::from_offset_with_length(o, l),
                    ByteRange::Suffix(l) => IcByteRange::Last(l),
                };
                (key.to_string(), r)
            })
            .collect();
        let results = match self.store().await.get_partial_values(ranges).await {
            Ok(r) => r,
            Err(e) if is_not_found(&e) => return Ok(None),
            Err(e) => return Err(storage_err(&e.to_string())),
        };
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(b) => out.push(Ok(b)),
                Err(e) if is_not_found(&e) => return Ok(None),
                Err(e) => return Err(storage_err(&e.to_string())),
            }
        }
        Ok(Some(futures::stream::iter(out).boxed()))
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        match self.store().await.getsize(key.as_str()).await {
            Ok(n) => Ok(Some(n)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(storage_err(&e.to_string())),
        }
    }

    fn supports_get_partial(&self) -> bool {
        true
    }
}

#[async_trait::async_trait]
impl AsyncListableStorageTraits for IcechunkReadStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        self.inner.list().await.map_err(map_storage_err)
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        self.inner
            .list_prefix(prefix)
            .await
            .map_err(map_storage_err)
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        self.inner.list_dir(prefix).await.map_err(map_storage_err)
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        self.inner
            .size_prefix(prefix)
            .await
            .map_err(map_storage_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_locations() {
        assert_eq!(
            Location::parse("s3://bucket/a/b/").unwrap(),
            Location::S3 {
                bucket: "bucket".into(),
                prefix: Some("a/b".into())
            }
        );
        assert_eq!(
            Location::parse("gs://bucket").unwrap(),
            Location::Gcs {
                bucket: "bucket".into(),
                prefix: None
            }
        );
        assert_eq!(
            Location::parse("az://c/p").unwrap(),
            Location::Azure {
                container: "c".into(),
                prefix: Some("p".into())
            }
        );
        assert_eq!(
            Location::parse("/tmp/repo").unwrap(),
            Location::Local("/tmp/repo".into())
        );
        assert!(matches!(
            Location::parse("https://example.com/repo").unwrap(),
            Location::Http(_)
        ));
        assert!(Location::parse("ftp://x/y").is_err());
        assert!(Location::parse("s3:///nobucket").is_err());
        assert!(Location::parse("").is_err());
    }

    #[test]
    fn parses_refs() {
        assert_eq!(
            RefSpec::parse("branch", "main").unwrap(),
            RefSpec::Branch("main".into())
        );
        assert!(RefSpec::parse("branch", "").is_err());
        assert!(RefSpec::parse("commit", "x").is_err());
        assert!(RefSpec::Snapshot("not-an-id".into())
            .version_info("u")
            .is_err());
    }

    #[test]
    fn identifier_hides_options() {
        let loc = Location::parse("s3://b/repo").unwrap();
        let r = RefSpec::Branch("main".into());
        let plain = store_identifier(&loc, &r, &StoreOptions::default(), &BTreeMap::new());
        assert_eq!(plain, "icechunk+s3://b/repo@branch:main");

        let secret: StoreOptions =
            serde_json::from_str(r#"{"access_key_id":"AKIA","secret_access_key":"shh"}"#).unwrap();
        let with = store_identifier(&loc, &r, &secret, &BTreeMap::new());
        assert!(with.starts_with("icechunk+s3://b/repo@branch:main#"));
        assert!(!with.contains("shh"));
        assert_ne!(with, plain);
    }

    #[test]
    fn rejects_unknown_options() {
        assert!(parse_json::<StoreOptions>("o", r#"{"regoin":"x"}"#).is_err());
        assert!(parse_json::<StoreOptions>("o", "[]").is_ok());
    }

    #[test]
    fn container_credentials_by_scheme() {
        let anon = StoreOptions {
            anonymous: Some(true),
            ..StoreOptions::default()
        };
        assert!(matches!(
            container_credentials("s3://b/", &anon).unwrap(),
            Credentials::S3(S3Credentials::Anonymous)
        ));
        assert!(matches!(
            container_credentials("file:///data/", &StoreOptions::default()).unwrap(),
            Credentials::LocalFileSystemAccess
        ));
        assert!(container_credentials("ftp://x/", &anon).is_err());
    }
}
