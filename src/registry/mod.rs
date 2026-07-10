//! Internal model-facts registry.
//!
//! Resolves models.dev's `base_model` inheritance over raw TOML (see
//! [`resolve`]) and maps the result into [`ModelFacts`], the gateway's
//! isolated internal representation of a model's operational facts (design
//! doc §9.6: the external models.dev TOML shape must never flow unmapped
//! into the runtime — everything past `to_facts` only ever sees
//! `ModelFacts`).
//!
//! [`Registry`] loads a models.dev-shaped tree — a provider-agnostic
//! `models/<id>.toml` file per model plus a `providers/<provider>/<id>.toml`
//! serving overlay that inherits it via `base_model` — either from the seed
//! vendored under `registry/seed/` and embedded at compile time
//! ([`Registry::embedded`]), from an arbitrary directory in the same shape
//! ([`Registry::from_dir`], used by `sluice models` against a local
//! models.dev checkout), or from a live fetch of the models.dev dataset
//! itself ([`Registry::from_models_dev_json`] / [`models_dev::fetch_network`],
//! used by `sluice models update --from-network` / `sluice models diff
//! --from-network`). Seed prices/limits are advisory and may lag; refreshing
//! them — whether from a local checkout or live over the network — is
//! always an explicit, out-of-band opt-in step via `sluice models
//! update`/`diff` (design §9.6), never
//! fetched on the request path.

pub mod diff;
pub mod models_dev;
pub mod resolve;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use include_dir::{include_dir, Dir};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use self::models_dev::to_facts;
use self::resolve::{resolve, ResolveError};

/// The vendored models.dev-shaped seed (`registry/seed/`), embedded into the
/// binary at compile time. Advisory only — see the header comment in each
/// seed file and design doc §9.6.
static SEED: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/registry/seed");

/// Default path `sluice models update` writes to, and the path
/// `Registry::load` checks for a locally-refreshed registry before falling
/// back to `embedded()`. Relative to the current working directory, mirroring
/// how `sluice.toml` is resolved by `sluice serve`/`check`/`routes`. Keep in
/// sync with the CLI's `models update --out` default in `cli.rs`.
pub const DEFAULT_REGISTRY_PATH: &str = "sluice-models.json";

/// Raw `(id, toml contents)` pairs read from a `models/` directory.
type BaseFiles = Vec<(String, String)>;
/// Raw `(provider, id, toml contents)` triples read from a `providers/`
/// directory.
type ProviderFiles = Vec<(String, String, String)>;

/// Isolated internal representation of a model's operational facts,
/// decoupled from the upstream models.dev TOML shape. Extend as later
/// milestones need more facts (e.g. reasoning support, knowledge cutoff).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelFacts {
    pub id: String,
    pub provider: String,
    pub context: Option<u64>,
    pub max_output: Option<u64>,
    pub cost_input: Option<f64>,
    pub cost_output: Option<f64>,
    pub modalities: Vec<String>,
    pub tool_call: bool,
    pub status: Option<String>,
}

/// Failure loading and resolving a models.dev-shaped source directory.
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("failed to read registry source '{path}': {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse TOML at '{path}': {source}")]
    Toml {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error(transparent)]
    Resolve(#[from] ResolveError),
    #[error("failed to parse registry JSON at '{path}': {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to fetch models.dev data from '{url}': {source}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("models.dev responded with non-success status {status} from '{url}'")]
    Http { url: String, status: u16 },
    #[error("models.dev response from '{url}' exceeded the {limit}-byte size cap")]
    ResponseTooLarge { url: String, limit: usize },
    #[error("models.dev response from '{url}' was not valid UTF-8: {source}")]
    Utf8 {
        url: String,
        #[source]
        source: std::str::Utf8Error,
    },
}

/// Composite identity of a model entry: `(provider, id)`. The same bare
/// model id (e.g. `"gpt-4o"`) may legitimately be served by more than one
/// provider with different facts (pricing, limits) — keying by bare id alone
/// made the second provider to load silently clobber the first
/// (non-deterministic last-write-wins over `HashMap` iteration order).
/// Keying by the composite pair instead makes both resolve independently.
pub type ModelKey = (String, String);

/// A loaded set of model facts, keyed by the composite `(provider, id)` pair
/// (see [`ModelKey`]) rather than bare id — the same id may be served by
/// more than one provider, each with its own facts.
#[derive(Debug, Clone)]
pub struct Registry {
    pub(crate) models: HashMap<ModelKey, ModelFacts>,
}

impl Registry {
    /// Parse and resolve the seed embedded at compile time
    /// (`registry/seed/`). Infallible: the vendored seed is checked into the
    /// repo and covered by tests, so a parse/resolve failure here is a
    /// programmer error in the seed itself, not a runtime condition callers
    /// need to handle. Also guards against a *silently empty* result (e.g. a
    /// renamed `providers/` directory yielding zero provider files without
    /// tripping `build()`'s error path) — an embedded registry with no
    /// models is exactly as much a broken build as one that fails to parse.
    pub fn embedded() -> Registry {
        let (base_files, provider_files) = collect_embedded(&SEED);
        let registry =
            build(base_files, provider_files).expect("embedded seed registry must be well-formed");
        assert!(
            !registry.models.is_empty(),
            "embedded seed registry must not be empty — check registry/seed/ layout"
        );
        registry
    }

    /// Parse and resolve a models.dev-shaped directory (a `models/` dir of
    /// provider-agnostic files plus a `providers/<provider>/` dir of serving
    /// overlays), same shape as the embedded seed. Used by `sluice models`
    /// against a local models.dev checkout or seed-shaped fixture. Missing
    /// `models/` or `providers/` subdirectories are treated as empty rather
    /// than an error, so a source need only provide the parts it overrides.
    pub fn from_dir(dir: &Path) -> Result<Registry, RegistryError> {
        let base_files = collect_fs_files(&dir.join("models"))?;
        let provider_files = collect_fs_provider_files(&dir.join("providers"))?;
        build(base_files, provider_files)
    }

    /// Parse a models.dev `api.json` payload (see
    /// [`models_dev::MODELS_DEV_URL`], fetched by [`models_dev::fetch_network`])
    /// and resolve it through the exact same `build` pipeline [`Registry::from_dir`]
    /// uses — no parallel resolution path (design §9.6). Each `api.json` model
    /// entry is already fully resolved (no `base_model` chain to follow), so
    /// this passes an empty base-file set and one provider-file row per
    /// model, converted to the same raw-TOML-text representation `build`
    /// consumes for `providers/<provider>/<id>.toml` files by
    /// [`models_dev::parse_provider_rows`].
    pub fn from_models_dev_json(payload: &str) -> Result<Registry, RegistryError> {
        let provider_files = models_dev::parse_provider_rows(payload)?;
        build(Vec::new(), provider_files)
    }

    /// Look up a model's resolved facts by its `(provider, id)` composite
    /// identity. Unknown model → `None`, never an error (design doc
    /// §9.5/§9.6): callers decide open-vs-closed behavior via their own
    /// `on_error`.
    pub fn get(&self, provider: &str, model_id: &str) -> Option<&ModelFacts> {
        self.models
            .get(&(provider.to_string(), model_id.to_string()))
    }

    /// Iterate over every model's facts, in unspecified order. Callers
    /// needing deterministic ordering (e.g. `sluice models list`) should sort
    /// by id themselves, as `to_json` does internally.
    pub fn iter(&self) -> impl Iterator<Item = &ModelFacts> {
        self.models.values()
    }

    /// Number of models in this registry.
    pub fn len(&self) -> usize {
        self.models.len()
    }

    /// Whether this registry has no models at all.
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    /// Load the registry the gateway should use at runtime: prefers a local
    /// registry file at [`DEFAULT_REGISTRY_PATH`] if one has been written by
    /// `sluice models update` and parses successfully, else the embedded
    /// seed.
    ///
    /// A local file that doesn't exist falls back to `embedded()` silently —
    /// that's the normal, expected case for any checkout that has never run
    /// `sluice models update`. But a local file that DOES exist and fails to
    /// read/parse is different: it means a previously-refreshed registry has
    /// silently stopped taking effect (money-relevant, since it's pricing and
    /// context-limit data), so that case is logged via `tracing::warn!`
    /// before falling back, per design doc §9.6.
    pub fn load() -> Registry {
        let path = Path::new(DEFAULT_REGISTRY_PATH);
        if path.is_file() {
            match Registry::from_json_file(path) {
                Ok(registry) => return registry,
                Err(err) => {
                    tracing::warn!(
                        "local registry file '{}' exists but failed to load ({err}); \
                         falling back to the embedded seed",
                        path.display()
                    );
                }
            }
        }
        Registry::embedded()
    }

    /// Read a registry previously written by [`Registry::write_json_file`]
    /// (a JSON array of resolved [`ModelFacts`]), keying it back by its
    /// `(provider, id)` composite identity.
    pub fn from_json_file(path: &Path) -> Result<Registry, RegistryError> {
        let contents = std::fs::read_to_string(path).map_err(|source| RegistryError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let facts: Vec<ModelFacts> =
            serde_json::from_str(&contents).map_err(|source| RegistryError::Json {
                path: path.display().to_string(),
                source,
            })?;
        let models = facts
            .into_iter()
            .map(|f| ((f.provider.clone(), f.id.clone()), f))
            .collect();
        Ok(Registry { models })
    }

    /// Serialize this registry's models as a pretty-printed JSON array,
    /// sorted by `(id, provider)` for deterministic output — id first since
    /// that's the primary axis users scan by, provider as the tiebreaker
    /// when the same id is served by more than one provider. Used both for
    /// `sluice models list --json` and for the on-disk format
    /// `write_json_file` writes and `from_json_file` reads back. Each
    /// element already carries both `id` and `provider`, so the JSON array
    /// itself is the composite-identity round-trip format — no wrapper
    /// keying needed on disk.
    pub fn to_json(&self) -> serde_json::Result<String> {
        let mut sorted: Vec<&ModelFacts> = self.models.values().collect();
        sorted.sort_by(|a, b| (&a.id, &a.provider).cmp(&(&b.id, &b.provider)));
        serde_json::to_string_pretty(&sorted)
    }

    /// Write this registry as JSON to `path` (used by `sluice models
    /// update`). Writes ONLY this file — never config or routes.
    pub fn write_json_file(&self, path: &Path) -> Result<(), RegistryError> {
        let json = self.to_json().map_err(|source| RegistryError::Json {
            path: path.display().to_string(),
            source,
        })?;
        std::fs::write(path, json).map_err(|source| RegistryError::Io {
            path: path.display().to_string(),
            source,
        })
    }
}

/// Merge raw `(id, contents)` base files and `(provider, id, contents)`
/// provider serving files into a resolved `Registry`, via the shared
/// `resolve`/`to_facts` pipeline. The one assembly point both `embedded()`
/// and `from_dir()` funnel through.
fn build(base_files: BaseFiles, provider_files: ProviderFiles) -> Result<Registry, RegistryError> {
    let mut base_lookup: BTreeMap<String, toml::Table> = BTreeMap::new();
    for (id, contents) in &base_files {
        let table: toml::Table =
            toml::from_str(contents).map_err(|source| RegistryError::Toml {
                path: format!("models/{id}.toml"),
                source,
            })?;
        base_lookup.insert(id.clone(), table);
    }

    let mut models = HashMap::new();
    for (provider, id, contents) in provider_files {
        let table: toml::Table =
            toml::from_str(&contents).map_err(|source| RegistryError::Toml {
                path: format!("providers/{provider}/{id}.toml"),
                source,
            })?;
        let resolved = resolve(&base_lookup, &id, table)?;
        let facts = to_facts(&id, &provider, resolved).map_err(|source| RegistryError::Toml {
            path: format!("providers/{provider}/{id}.toml"),
            source,
        })?;
        models.insert((provider, id), facts);
    }

    Ok(Registry { models })
}

/// Derive a model id from a seed/source filename: the stem with its `.toml`
/// extension stripped (e.g. `"gemini-2.5-pro.toml"` -> `"gemini-2.5-pro"`).
fn stem(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(str::to_string)
}

/// Walk the embedded seed `Dir` into raw base files (`models/*.toml`) and
/// provider serving files (`providers/<provider>/*.toml`).
fn collect_embedded(root: &Dir<'_>) -> (BaseFiles, ProviderFiles) {
    let mut base_files = Vec::new();
    if let Some(models_dir) = root.get_dir("models") {
        for file in models_dir.files() {
            if let (Some(id), Some(contents)) = (stem(file.path()), file.contents_utf8()) {
                base_files.push((id, contents.to_string()));
            }
        }
    }

    let mut provider_files = Vec::new();
    if let Some(providers_dir) = root.get_dir("providers") {
        for provider_dir in providers_dir.dirs() {
            let Some(provider) = provider_dir
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(str::to_string)
            else {
                continue;
            };
            for file in provider_dir.files() {
                if let (Some(id), Some(contents)) = (stem(file.path()), file.contents_utf8()) {
                    provider_files.push((provider.clone(), id, contents.to_string()));
                }
            }
        }
    }

    (base_files, provider_files)
}

/// Read every `*.toml` file directly inside `dir` into `(id, contents)`
/// pairs. A `dir` that doesn't exist (or isn't a directory) yields an empty
/// list rather than an error — callers may only override part of the shape.
fn collect_fs_files(dir: &Path) -> Result<BaseFiles, RegistryError> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir).map_err(|source| RegistryError::Io {
        path: dir.display().to_string(),
        source,
    })? {
        let entry = entry.map_err(|source| RegistryError::Io {
            path: dir.display().to_string(),
            source,
        })?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("toml") {
            continue;
        }
        let Some(id) = stem(&path) else { continue };
        let contents = std::fs::read_to_string(&path).map_err(|source| RegistryError::Io {
            path: path.display().to_string(),
            source,
        })?;
        out.push((id, contents));
    }
    Ok(out)
}

/// Read every `<provider>/*.toml` file under `dir` (one level of provider
/// subdirectories) into `(provider, id, contents)` triples. A `dir` that
/// doesn't exist yields an empty list rather than an error.
fn collect_fs_provider_files(dir: &Path) -> Result<ProviderFiles, RegistryError> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir).map_err(|source| RegistryError::Io {
        path: dir.display().to_string(),
        source,
    })? {
        let entry = entry.map_err(|source| RegistryError::Io {
            path: dir.display().to_string(),
            source,
        })?;
        let provider_dir = entry.path();
        if !provider_dir.is_dir() {
            continue;
        }
        let Some(provider) = provider_dir
            .file_name()
            .and_then(|n| n.to_str())
            .map(str::to_string)
        else {
            continue;
        };
        for (id, contents) in collect_fs_files(&provider_dir)? {
            out.push((provider.clone(), id, contents));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_contains_seeded_anthropic_model_with_resolved_facts() {
        let registry = Registry::embedded();
        let facts = registry
            .get("anthropic", "claude-opus-4-1-20250805")
            .expect("seeded anthropic model must be present");

        assert_eq!(facts.id, "claude-opus-4-1-20250805");
        assert_eq!(facts.provider, "anthropic");
        assert_eq!(facts.context, Some(200_000));
        assert_eq!(facts.cost_input, Some(15.0));
        assert_eq!(facts.cost_output, Some(75.0));
        assert!(facts.tool_call);
        assert_eq!(facts.status.as_deref(), Some("stable"));
    }

    #[test]
    fn embedded_contains_seeded_openai_and_google_models() {
        let registry = Registry::embedded();

        let gpt = registry
            .get("openai", "gpt-5")
            .expect("seeded openai model");
        assert_eq!(gpt.provider, "openai");
        assert_eq!(gpt.context, Some(400_000));
        assert!(gpt.tool_call);

        let gemini = registry
            .get("google", "gemini-2.5-pro")
            .expect("seeded google model");
        assert_eq!(gemini.provider, "google");
        assert_eq!(gemini.context, Some(1_048_576));
        assert!(gemini.tool_call);
    }

    #[test]
    fn get_on_unknown_id_returns_none() {
        let registry = Registry::embedded();
        assert!(registry.get("anthropic", "no-such-model").is_none());
    }

    #[test]
    fn get_on_known_id_wrong_provider_returns_none() {
        // The composite key is (provider, id) — the same bare id under a
        // provider that doesn't serve it must miss, not fall back to
        // whichever provider happens to serve that id.
        let registry = Registry::embedded();
        assert!(registry.get("openai", "claude-opus-4-1-20250805").is_none());
    }

    #[test]
    fn load_falls_back_to_embedded() {
        let registry = Registry::load();
        assert!(registry
            .get("anthropic", "claude-opus-4-1-20250805")
            .is_some());
    }

    #[test]
    fn from_dir_resolves_models_and_providers_layout() {
        let tmp = std::env::temp_dir().join(format!(
            "sluice-registry-from-dir-test-{}",
            std::process::id()
        ));
        let models_dir = tmp.join("models");
        let provider_dir = tmp.join("providers").join("anthropic");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::create_dir_all(&provider_dir).unwrap();

        std::fs::write(
            models_dir.join("test-model-base.toml"),
            "modalities = [\"text\"]\n[limit]\ncontext = 100000\noutput = 4096\n",
        )
        .unwrap();
        std::fs::write(
            provider_dir.join("test-model.toml"),
            "base_model = \"test-model-base\"\nstatus = \"stable\"\ntool_call = true\n[cost]\ninput = 1.0\noutput = 2.0\n",
        )
        .unwrap();

        let registry = Registry::from_dir(&tmp).expect("from_dir should succeed");
        let facts = registry
            .get("anthropic", "test-model")
            .expect("test model present");
        assert_eq!(facts.provider, "anthropic");
        assert_eq!(facts.context, Some(100_000));
        assert_eq!(facts.cost_input, Some(1.0));
        assert!(facts.tool_call);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn from_dir_on_nonexistent_source_yields_empty_registry() {
        let registry = Registry::from_dir(Path::new("/no/such/sluice-registry-dir")).unwrap();
        assert!(registry.get("anyone", "anything").is_none());
    }

    #[test]
    fn from_dir_surfaces_malformed_toml_as_toml_error() {
        let tmp = std::env::temp_dir().join(format!(
            "sluice-registry-bad-toml-test-{}",
            std::process::id()
        ));
        let provider_dir = tmp.join("providers").join("anthropic");
        std::fs::create_dir_all(&provider_dir).unwrap();
        std::fs::write(
            provider_dir.join("broken.toml"),
            "this is not = valid [[ toml",
        )
        .unwrap();

        let err = Registry::from_dir(&tmp).expect_err("malformed TOML must be an error");
        assert!(matches!(err, RegistryError::Toml { .. }), "got {err:?}");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn to_json_round_trips_through_from_json_file() {
        let registry = Registry::embedded();
        let json = registry.to_json().unwrap();

        let tmp = std::env::temp_dir().join(format!(
            "sluice-registry-json-roundtrip-test-{}",
            std::process::id()
        ));
        std::fs::write(&tmp, &json).unwrap();

        let reloaded = Registry::from_json_file(&tmp).expect("from_json_file should succeed");
        assert_eq!(reloaded.len(), registry.len());
        let original = registry
            .get("anthropic", "claude-opus-4-1-20250805")
            .expect("seeded model present");
        let round_tripped = reloaded
            .get("anthropic", "claude-opus-4-1-20250805")
            .expect("seeded model present after round-trip");
        assert_eq!(original, round_tripped);

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn from_json_file_on_missing_file_is_io_error() {
        let err = Registry::from_json_file(Path::new("/no/such/sluice-registry.json"))
            .expect_err("missing file must be an error");
        assert!(matches!(err, RegistryError::Io { .. }), "got {err:?}");
    }

    #[test]
    fn from_json_file_on_malformed_json_is_json_error() {
        let tmp = std::env::temp_dir().join(format!(
            "sluice-registry-bad-json-test-{}",
            std::process::id()
        ));
        std::fs::write(&tmp, "not valid json").unwrap();

        let err =
            Registry::from_json_file(&tmp).expect_err("malformed JSON must surface as an error");
        assert!(matches!(err, RegistryError::Json { .. }), "got {err:?}");

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn from_dir_surfaces_missing_base_model_as_resolve_error() {
        let tmp = std::env::temp_dir().join(format!(
            "sluice-registry-missing-base-test-{}",
            std::process::id()
        ));
        let provider_dir = tmp.join("providers").join("anthropic");
        std::fs::create_dir_all(&provider_dir).unwrap();
        std::fs::write(
            provider_dir.join("orphan.toml"),
            "base_model = \"does-not-exist\"\n",
        )
        .unwrap();

        let err = Registry::from_dir(&tmp).expect_err("missing base_model must be an error");
        assert!(
            matches!(err, RegistryError::Resolve(ResolveError::MissingBase(_))),
            "got {err:?}"
        );

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn same_model_id_from_two_providers_resolves_independently() {
        // Regression guard for the composite-key fix: the same bare model id
        // served by two different providers used to collide in a
        // `HashMap<String, ModelFacts>` keyed only by id (non-deterministic
        // last-write-wins). Keyed by `(provider, id)`, both must resolve to
        // their own distinct facts.
        let tmp = std::env::temp_dir().join(format!(
            "sluice-registry-dual-provider-test-{}",
            std::process::id()
        ));
        let models_dir = tmp.join("models");
        let provider_a_dir = tmp.join("providers").join("provider-a");
        let provider_b_dir = tmp.join("providers").join("provider-b");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::create_dir_all(&provider_a_dir).unwrap();
        std::fs::create_dir_all(&provider_b_dir).unwrap();

        std::fs::write(
            models_dir.join("shared-model-base.toml"),
            "modalities = [\"text\"]\n[limit]\ncontext = 100000\noutput = 4096\n",
        )
        .unwrap();
        std::fs::write(
            provider_a_dir.join("shared-model.toml"),
            "base_model = \"shared-model-base\"\nstatus = \"stable\"\ntool_call = true\n[cost]\ninput = 1.0\noutput = 2.0\n",
        )
        .unwrap();
        std::fs::write(
            provider_b_dir.join("shared-model.toml"),
            "base_model = \"shared-model-base\"\nstatus = \"beta\"\ntool_call = false\n[cost]\ninput = 9.0\noutput = 18.0\n",
        )
        .unwrap();

        let registry = Registry::from_dir(&tmp).expect("from_dir should succeed");
        assert_eq!(registry.len(), 2, "both providers' entries must survive");

        let a = registry
            .get("provider-a", "shared-model")
            .expect("provider-a's shared-model present");
        let b = registry
            .get("provider-b", "shared-model")
            .expect("provider-b's shared-model present");

        assert_eq!(a.cost_input, Some(1.0));
        assert_eq!(a.status.as_deref(), Some("stable"));
        assert!(a.tool_call);

        assert_eq!(b.cost_input, Some(9.0));
        assert_eq!(b.status.as_deref(), Some("beta"));
        assert!(!b.tool_call);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn load_falls_back_to_embedded_when_local_file_is_corrupt() {
        // `Registry::load()` reads a *fixed* relative path
        // (`DEFAULT_REGISTRY_PATH`), so this test can't point it at an
        // arbitrary tmpdir; it instead exercises the same fallback logic
        // `load()` uses via `from_json_file`, confirming a corrupt local
        // registry file yields an error (which `load()` logs and falls back
        // from) rather than panicking.
        let tmp = std::env::temp_dir().join(format!(
            "sluice-registry-corrupt-local-test-{}",
            std::process::id()
        ));
        std::fs::write(&tmp, "{ not: valid json").unwrap();

        let result = Registry::from_json_file(&tmp);
        assert!(
            matches!(result, Err(RegistryError::Json { .. })),
            "got {result:?}"
        );

        // And the actual fallback path itself must not panic and must still
        // yield a usable (embedded) registry.
        let fallback = Registry::embedded();
        assert!(fallback
            .get("anthropic", "claude-opus-4-1-20250805")
            .is_some());

        std::fs::remove_file(&tmp).ok();
    }
}
