//! Raw models.dev TOML schema types, and the single mapping point from that
//! external shape into the gateway's isolated `ModelFacts` (design doc
//! §9.6). `to_facts` is the only place allowed to read models.dev field
//! names — everything downstream only ever sees `ModelFacts`. The mapping
//! goes through the typed [`RawModel`] struct (via `toml::Table::try_into`),
//! never by reading the resolved `toml::Table` dynamically field-by-field:
//! an upstream field rename must break the importer at the deserialize step
//! (a loud `RegistryError::Toml`), not silently degrade to `None` at the
//! runtime (design §9.6: "schema drift breaks the importer, not the
//! runtime").

use std::collections::BTreeMap;

use serde::Deserialize;
use toml::Value;

use super::{ModelFacts, Registry, RegistryError};

/// The live models.dev dataset URL fetched by `sluice models update
/// --from-network` / `sluice models diff --from-network` (design §9.6:
/// refreshing pricing/limit data is always an explicit, opt-in step — never
/// on the request path). Tests override this via the hidden `--network-url`
/// CLI flag (see [`fetch_network`]) rather than hitting the real network.
pub const MODELS_DEV_URL: &str = "https://models.dev/api.json";

/// Raw models.dev model entry, deserialized from the fully-resolved
/// (post-`base_model` merge) `toml::Table`. Tolerant of fields this gateway
/// doesn't (yet) understand: anything not named below lands in `extra`
/// rather than failing to parse, so upstream *additions* don't break
/// loading — but a *rename* of a field this struct does name will surface as
/// a hard deserialize error instead of silently mapping to `None`.
#[derive(Debug, Clone, Deserialize)]
pub struct RawModel {
    pub base_model: Option<String>,
    pub base_model_omit: Option<Vec<String>>,
    pub status: Option<String>,
    #[serde(default)]
    pub tool_call: bool,
    pub cost: Option<Cost>,
    pub limit: Option<Limit>,
    pub modalities: Option<Vec<String>>,
    #[serde(flatten)]
    pub extra: toml::Table,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Cost {
    pub input: Option<f64>,
    pub output: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Limit {
    pub context: Option<u64>,
    pub output: Option<u64>,
}

/// Map a fully-resolved (post-`base_model` merge) table into the gateway's
/// isolated `ModelFacts`. `id` and `provider` are supplied by the caller
/// (derived from filename/directory), never read from `resolved`.
///
/// Deserializes `resolved` into the typed [`RawModel`] first, then builds
/// `ModelFacts` from `RawModel`'s own fields — never by reading `resolved`
/// dynamically. This is the load-bearing part of design §9.6: a models.dev
/// field rename (e.g. `cost.input` -> `cost.prompt`) must fail this
/// deserialize (surfacing as `RegistryError::Toml` up through `build()`),
/// not quietly produce a `ModelFacts` with `cost_input: None`.
pub fn to_facts(
    id: &str,
    provider: &str,
    resolved: toml::Table,
) -> Result<ModelFacts, toml::de::Error> {
    let raw: RawModel = resolved.try_into()?;

    let cost_input = raw.cost.as_ref().and_then(|c| c.input);
    let cost_output = raw.cost.as_ref().and_then(|c| c.output);
    let context = raw.limit.as_ref().and_then(|l| l.context);
    let max_output = raw.limit.as_ref().and_then(|l| l.output);
    let modalities = raw.modalities.unwrap_or_default();
    let tool_call = raw.tool_call;
    let status = raw.status;

    Ok(ModelFacts {
        id: id.to_string(),
        provider: provider.to_string(),
        context,
        max_output,
        cost_input,
        cost_output,
        modalities,
        tool_call,
        status,
    })
}

/// One provider entry in models.dev's live `api.json`: `{"<provider-id>":
/// {"id", "name", "models": {"<model-id>": {...}}}}`. `id`/`name` are read
/// defensively (defaulting to empty) since the outer map key is always used
/// as the authoritative provider id if `id` is absent — mirroring how
/// `collect_fs_provider_files` derives a provider id from its directory name
/// rather than trusting file contents.
#[derive(Debug, Default, Deserialize)]
struct ApiProvider {
    #[serde(default)]
    id: String,
    #[serde(default)]
    models: BTreeMap<String, ApiModel>,
}

/// One model entry under a provider in models.dev's `api.json`. Unlike
/// [`RawModel`] (the offline, fully-resolved TOML shape), `modalities` here
/// is an OBJECT (`{input: [...], output: [...]}`), flattened to the flat
/// `Vec<String>` shape by [`flatten_modalities`] before this model is handed
/// to the shared `resolve`/`to_facts` pipeline. `cost`/`limit` reuse the
/// same [`Cost`]/[`Limit`] structs `RawModel` uses, since `api.json`'s
/// `cost: {input, output}` / `limit: {context, output}` sub-shapes match the
/// offline TOML shape exactly.
#[derive(Debug, Default, Deserialize)]
struct ApiModel {
    cost: Option<Cost>,
    limit: Option<Limit>,
    #[serde(default)]
    modalities: ApiModalities,
    #[serde(default)]
    tool_call: bool,
    status: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ApiModalities {
    #[serde(default)]
    input: Vec<String>,
    #[serde(default)]
    output: Vec<String>,
}

/// Flatten models.dev `api.json`'s `modalities: {input: [...], output:
/// [...]}` object into the flat `Vec<String>` shape `ModelFacts.modalities`
/// / [`RawModel::modalities`] expects.
///
/// Rule: the union of `input` and `output`, deduplicated, in a STABLE order
/// — every `input` entry first (in its original order), then every `output`
/// entry not already present (in its original order). E.g. `input:
/// ["text","image"], output: ["text"]` -> `["text","image"]`; `input: [],
/// output: ["text"]` -> `["text"]`.
pub fn flatten_modalities(input: &[String], output: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(input.len() + output.len());
    for m in input {
        if !out.contains(m) {
            out.push(m.clone());
        }
    }
    for m in output {
        if !out.contains(m) {
            out.push(m.clone());
        }
    }
    out
}

/// Build the raw-TOML-text `toml::Table` for one `api.json` model entry,
/// matching the same field shape a `providers/<provider>/<id>.toml` serving
/// file would carry (see [`RawModel`]/[`to_facts`]) — no `base_model` is
/// ever set, since `api.json` entries are already fully resolved. A missing
/// `cost`/`limit`/`status` is omitted from the table entirely (never
/// defaulted to an explicit null), matching the offline path's "missing
/// field -> `None`, not an error" contract (see `to_facts_defaults_when_fields_missing`).
fn provider_row_table(model: &ApiModel, modalities: Vec<String>) -> toml::Table {
    let mut table = toml::Table::new();
    table.insert("tool_call".to_string(), Value::Boolean(model.tool_call));
    if let Some(status) = &model.status {
        table.insert("status".to_string(), Value::String(status.clone()));
    }
    if !modalities.is_empty() {
        table.insert(
            "modalities".to_string(),
            Value::Array(modalities.into_iter().map(Value::String).collect()),
        );
    }
    if let Some(cost) = &model.cost {
        let mut cost_table = toml::Table::new();
        if let Some(v) = cost.input {
            cost_table.insert("input".to_string(), Value::Float(v));
        }
        if let Some(v) = cost.output {
            cost_table.insert("output".to_string(), Value::Float(v));
        }
        if !cost_table.is_empty() {
            table.insert("cost".to_string(), Value::Table(cost_table));
        }
    }
    if let Some(limit) = &model.limit {
        let mut limit_table = toml::Table::new();
        if let Some(v) = limit.context {
            limit_table.insert("context".to_string(), Value::Integer(v as i64));
        }
        if let Some(v) = limit.output {
            limit_table.insert("output".to_string(), Value::Integer(v as i64));
        }
        if !limit_table.is_empty() {
            table.insert("limit".to_string(), Value::Table(limit_table));
        }
    }
    table
}

/// Parse a models.dev `api.json` payload into `(provider, id, contents)`
/// provider-file rows — the exact representation [`super::build`] (private)
/// consumes for `providers/<provider>/<id>.toml` files — so
/// [`Registry::from_models_dev_json`] can feed a live network fetch through
/// the IDENTICAL `resolve`/`to_facts` pipeline the offline `--source`
/// directory path uses, with no parallel resolution logic (design §9.6).
///
/// The `toml::to_string` serialize step is expected to be infallible here:
/// every table `provider_row_table` builds is composed solely of
/// strings/bools/numbers/arrays/tables, which TOML can always represent — a
/// failure would mean a programmer error in `provider_row_table`, not a
/// runtime condition callers need to handle (mirroring `Registry::embedded`'s
/// own `.expect` on its always-well-formed seed).
pub(crate) fn parse_provider_rows(
    payload: &str,
) -> Result<Vec<(String, String, String)>, RegistryError> {
    let doc: BTreeMap<String, ApiProvider> =
        serde_json::from_str(payload).map_err(|source| RegistryError::Json {
            path: "models.dev api.json".to_string(),
            source,
        })?;

    let mut rows = Vec::new();
    for (provider_key, provider) in doc {
        let provider_id = if provider.id.is_empty() {
            provider_key
        } else {
            provider.id
        };
        for (model_id, model) in provider.models {
            let modalities = flatten_modalities(&model.modalities.input, &model.modalities.output);
            let table = provider_row_table(&model, modalities);
            let text = toml::to_string(&table)
                .expect("constructed provider-file table must serialize to TOML");
            rows.push((provider_id.clone(), model_id, text));
        }
    }
    Ok(rows)
}

/// Upper bound on the models.dev response body `fetch_network` will buffer
/// (FIX-4, audit finding m18): a hostile / misconfigured / MITM'd
/// models.dev endpoint (or mirror) must not be able to OOM the process with
/// an unbounded body. 32 MiB is generous headroom over the real `api.json`,
/// which is a few MB. Enforced two ways in `fetch_network`: an upfront
/// `content-length` check (fails fast, before reading anything) AND a
/// streamed running-total check as the body is read (a lying or absent
/// `content-length` header must still be bounded).
pub const MAX_MODELS_DEV_BYTES: usize = 32 * 1024 * 1024;

/// Fetch and resolve the live models.dev dataset (design §9.6: opt-in via
/// `sluice models update --from-network` / `sluice models diff
/// --from-network`, never on the request path). `base_url` is normally
/// [`MODELS_DEV_URL`]; tests inject a wiremock URL via the hidden
/// `--network-url` CLI override. Does only the HTTP GET plus a
/// [`Registry::from_models_dev_json`] call — all parsing/resolution lives
/// there, funneling through the exact same `build` the offline `--source`
/// path uses. A non-2xx response, a transport failure/timeout, an
/// oversized body (see [`MAX_MODELS_DEV_BYTES`]), or a non-UTF8 body is
/// returned as a [`RegistryError`], never partially applied and never a
/// panic (FIX-4: a hostile body is a `Result` error, not an `.unwrap()`).
///
/// A 30s total-request timeout is applied on this GET directly (not relying
/// on the caller's `client` config) so a hanging/slow-drip endpoint can't
/// wedge the process indefinitely; a timeout surfaces through the same
/// `RegistryError::Network` mapping as any other transport failure.
pub async fn fetch_network(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<Registry, RegistryError> {
    use futures_util::TryStreamExt;

    let response = client
        .get(base_url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|source| RegistryError::Network {
            url: base_url.to_string(),
            source,
        })?;

    let status = response.status();
    if !status.is_success() {
        return Err(RegistryError::Http {
            url: base_url.to_string(),
            status: status.as_u16(),
        });
    }

    if let Some(len) = response.content_length() {
        if len > MAX_MODELS_DEV_BYTES as u64 {
            return Err(RegistryError::ResponseTooLarge {
                url: base_url.to_string(),
                limit: MAX_MODELS_DEV_BYTES,
            });
        }
    }

    // Don't trust `content-length` alone (it may be absent, or a lying
    // mirror could understate it): bound the actual bytes read as they
    // stream in, erroring the moment the running total exceeds the cap
    // rather than buffering the whole thing first.
    let mut body: Vec<u8> = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|source| RegistryError::Network {
            url: base_url.to_string(),
            source,
        })?
    {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_MODELS_DEV_BYTES {
            return Err(RegistryError::ResponseTooLarge {
                url: base_url.to_string(),
                limit: MAX_MODELS_DEV_BYTES,
            });
        }
    }

    let body = std::str::from_utf8(&body).map_err(|source| RegistryError::Utf8 {
        url: base_url.to_string(),
        source,
    })?;

    Registry::from_models_dev_json(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_facts_maps_cost_limit_modalities_tool_call_status() {
        let resolved: toml::Table = toml::from_str(
            r#"
            id = "claude-x"
            status = "stable"
            tool_call = true
            modalities = ["text", "image"]
            [cost]
            input = 3.0
            output = 15.0
            [limit]
            context = 200000
            output = 8192
        "#,
        )
        .unwrap();

        let facts = to_facts("claude-x", "anthropic", resolved).unwrap();
        assert_eq!(facts.id, "claude-x");
        assert_eq!(facts.provider, "anthropic");
        assert_eq!(facts.context, Some(200000));
        assert_eq!(facts.max_output, Some(8192));
        assert_eq!(facts.cost_input, Some(3.0));
        assert_eq!(facts.cost_output, Some(15.0));
        assert_eq!(
            facts.modalities,
            vec!["text".to_string(), "image".to_string()]
        );
        assert!(facts.tool_call);
        assert_eq!(facts.status, Some("stable".to_string()));
    }

    #[test]
    fn to_facts_defaults_when_fields_missing() {
        let resolved: toml::Table = toml::Table::new();
        let facts = to_facts("m", "p", resolved).unwrap();
        assert_eq!(facts.context, None);
        assert_eq!(facts.max_output, None);
        assert_eq!(facts.cost_input, None);
        assert_eq!(facts.cost_output, None);
        assert!(facts.modalities.is_empty());
        assert!(!facts.tool_call);
        assert_eq!(facts.status, None);
    }

    #[test]
    fn to_facts_accepts_integer_cost_values() {
        let resolved: toml::Table = toml::from_str(
            r#"
            [cost]
            input = 3
            output = 15
        "#,
        )
        .unwrap();
        let facts = to_facts("m", "p", resolved).unwrap();
        assert_eq!(facts.cost_input, Some(3.0));
        assert_eq!(facts.cost_output, Some(15.0));
    }

    #[test]
    fn to_facts_surfaces_type_mismatch_as_deserialize_error() {
        // Design §9.6: a models.dev schema drift (here, a field given the
        // wrong shape) must break the importer with a loud error, not
        // silently degrade to `None` fields on `ModelFacts`.
        let resolved: toml::Table = toml::from_str(
            r#"
            [cost]
            input = "not-a-number"
        "#,
        )
        .unwrap();
        let err = to_facts("m", "p", resolved).expect_err("type mismatch must be an error");
        let _ = err; // just needs to be Err; message content is toml's own.
    }

    #[test]
    fn raw_model_tolerates_unknown_fields_via_extra() {
        let raw: RawModel = toml::from_str(
            r#"
            base_model = "gpt-x"
            status = "beta"
            some_unknown_future_field = "ignored-but-kept"
            [cost]
            input = 1.5
        "#,
        )
        .unwrap();
        assert_eq!(raw.base_model.as_deref(), Some("gpt-x"));
        assert_eq!(raw.status.as_deref(), Some("beta"));
        assert_eq!(raw.cost.unwrap().input, Some(1.5));
        assert!(raw.extra.contains_key("some_unknown_future_field"));
    }

    #[test]
    fn flatten_modalities_unions_input_then_output_deduped_stable_order() {
        let input = vec!["text".to_string(), "image".to_string()];
        let output = vec!["text".to_string()];
        assert_eq!(
            flatten_modalities(&input, &output),
            vec!["text".to_string(), "image".to_string()]
        );
    }

    #[test]
    fn flatten_modalities_appends_output_only_entries_after_input() {
        let input = vec!["text".to_string()];
        let output = vec!["image".to_string(), "audio".to_string()];
        assert_eq!(
            flatten_modalities(&input, &output),
            vec!["text".to_string(), "image".to_string(), "audio".to_string()]
        );
    }

    #[test]
    fn flatten_modalities_of_empty_input_falls_back_to_output() {
        let input: Vec<String> = vec![];
        let output = vec!["text".to_string()];
        assert_eq!(
            flatten_modalities(&input, &output),
            vec!["text".to_string()]
        );
    }

    #[test]
    fn parse_provider_rows_maps_api_json_shape_into_provider_file_rows() {
        let payload = r#"
        {
            "fixture-provider": {
                "id": "fixture-provider",
                "name": "Fixture Provider",
                "models": {
                    "fixture-model": {
                        "cost": {"input": 1.5, "output": 3.0},
                        "limit": {"context": 50000, "output": 4096},
                        "modalities": {"input": ["text", "image"], "output": ["text"]},
                        "tool_call": true,
                        "status": "stable"
                    }
                }
            }
        }
        "#;

        let rows = parse_provider_rows(payload).expect("payload should parse");
        assert_eq!(rows.len(), 1);
        let (provider, id, contents) = &rows[0];
        assert_eq!(provider, "fixture-provider");
        assert_eq!(id, "fixture-model");

        let table: toml::Table = toml::from_str(contents).expect("row contents must be valid TOML");
        let resolved = super::super::resolve::resolve(&BTreeMap::new(), id, table).unwrap();
        let facts = to_facts(id, provider, resolved).unwrap();

        assert_eq!(facts.cost_input, Some(1.5));
        assert_eq!(facts.cost_output, Some(3.0));
        assert_eq!(facts.context, Some(50000));
        assert_eq!(facts.max_output, Some(4096));
        assert_eq!(
            facts.modalities,
            vec!["text".to_string(), "image".to_string()]
        );
        assert!(facts.tool_call);
        assert_eq!(facts.status.as_deref(), Some("stable"));
    }

    #[test]
    fn parse_provider_rows_omits_missing_cost_and_limit_rather_than_erroring() {
        let payload = r#"
        {
            "p": {
                "id": "p",
                "models": {
                    "m": {
                        "modalities": {"input": [], "output": []}
                    }
                }
            }
        }
        "#;

        let rows = parse_provider_rows(payload).expect("payload should parse");
        let (provider, id, contents) = &rows[0];
        let table: toml::Table = toml::from_str(contents).unwrap();
        let resolved = super::super::resolve::resolve(&BTreeMap::new(), id, table).unwrap();
        let facts = to_facts(id, provider, resolved).unwrap();

        assert_eq!(facts.cost_input, None);
        assert_eq!(facts.cost_output, None);
        assert_eq!(facts.context, None);
        assert_eq!(facts.max_output, None);
        assert!(facts.modalities.is_empty());
        assert!(!facts.tool_call);
        assert_eq!(facts.status, None);
    }

    #[test]
    fn parse_provider_rows_falls_back_to_key_when_provider_id_field_empty() {
        let payload = r#"
        {
            "provider-key": {
                "models": {
                    "m": {}
                }
            }
        }
        "#;
        let rows = parse_provider_rows(payload).expect("payload should parse");
        assert_eq!(rows[0].0, "provider-key");
    }

    #[test]
    fn from_models_dev_json_resolves_via_the_shared_build_pipeline() {
        let payload = r#"
        {
            "fixture-provider": {
                "id": "fixture-provider",
                "models": {
                    "fixture-model": {
                        "cost": {"input": 2.0, "output": 4.0},
                        "modalities": {"input": ["text"], "output": []},
                        "tool_call": true
                    }
                }
            }
        }
        "#;

        let registry = Registry::from_models_dev_json(payload).expect("payload should resolve");
        let facts = registry
            .get("fixture-provider", "fixture-model")
            .expect("fixture model present");
        assert_eq!(facts.cost_input, Some(2.0));
        assert_eq!(facts.cost_output, Some(4.0));
        assert_eq!(facts.modalities, vec!["text".to_string()]);
        assert!(facts.tool_call);
    }
}
