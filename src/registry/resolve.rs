//! `base_model` inheritance resolution for models.dev-shaped TOML tables.
//!
//! models.dev lets a model entry declare `base_model = "<id>"` to inherit
//! another entry's fields, with `base_model_omit = ["dot.path", ...]`
//! deleting specific keys from the merged result afterward. This module
//! replicates that resolution over raw `toml::Table`s, before any of it is
//! mapped into the gateway's isolated `ModelFacts` (see
//! `models_dev::to_facts`).

use std::collections::BTreeMap;

use thiserror::Error;
use toml::Value;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResolveError {
    #[error("cycle detected in base_model chain at '{0}'")]
    Cycle(String),
    #[error("base_model '{0}' not found")]
    MissingBase(String),
}

/// Resolve the `base_model` inheritance chain for one entry, producing the
/// fully merged `toml::Table`.
///
/// - `base_lookup` maps every known model id to its own *raw* (unresolved)
///   table, used to follow `base_model` references.
/// - Tables are deep-merged recursively: matching keys that are tables in
///   both parent and child are merged key-by-key; the child wins on leaf
///   conflicts.
/// - Arrays and primitives are replaced wholesale by the child — never
///   appended or merged element-wise.
/// - `base_model_omit` dot-path deletions are applied to the merged result
///   *after* inheritance is resolved (the entry's own base chain has
///   already had its own `base_model_omit` applied by the time it is merged
///   into this entry).
/// - `id` is always injected from the `id` argument (filename-derived),
///   never read from — or left over in — the table's own contents.
pub fn resolve(
    base_lookup: &BTreeMap<String, toml::Table>,
    id: &str,
    table: toml::Table,
) -> Result<toml::Table, ResolveError> {
    let mut visiting = Vec::new();
    resolve_inner(base_lookup, id, table, &mut visiting)
}

fn resolve_inner(
    base_lookup: &BTreeMap<String, toml::Table>,
    id: &str,
    mut table: toml::Table,
    visiting: &mut Vec<String>,
) -> Result<toml::Table, ResolveError> {
    if visiting.iter().any(|v| v == id) {
        return Err(ResolveError::Cycle(id.to_string()));
    }
    visiting.push(id.to_string());

    let base_model = table.remove("base_model").and_then(|v| match v {
        Value::String(s) => Some(s),
        _ => None,
    });
    let omit = table.remove("base_model_omit");

    let merge_result = if let Some(base_id) = base_model {
        base_lookup
            .get(&base_id)
            .cloned()
            .ok_or_else(|| ResolveError::MissingBase(base_id.clone()))
            .and_then(|base_table| resolve_inner(base_lookup, &base_id, base_table, visiting))
            .map(|resolved_base| deep_merge(resolved_base, table))
    } else {
        Ok(table)
    };

    visiting.pop();
    let mut merged = merge_result?;

    if let Some(Value::Array(paths)) = omit {
        for path in paths.into_iter().filter_map(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        }) {
            remove_dot_path(&mut merged, &path);
        }
    }

    merged.insert("id".to_string(), Value::String(id.to_string()));
    Ok(merged)
}

/// Deep-merge two tables: keys that are tables in both `base` and `child`
/// are merged recursively; everything else (arrays, strings, numbers,
/// booleans, or a type mismatch between base and child) is replaced
/// wholesale by `child`'s value.
fn deep_merge(base: toml::Table, child: toml::Table) -> toml::Table {
    let mut merged = base;
    for (key, child_value) in child {
        let combined = match (merged.remove(&key), child_value) {
            (Some(Value::Table(base_t)), Value::Table(child_t)) => {
                Value::Table(deep_merge(base_t, child_t))
            }
            (_, child_value) => child_value,
        };
        merged.insert(key, combined);
    }
    merged
}

/// Remove a dot-separated path (e.g. `"cost.output"`) from a table. A no-op
/// if any segment along the path is missing or is not itself a table.
fn remove_dot_path(table: &mut toml::Table, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    remove_dot_path_parts(table, &parts);
}

fn remove_dot_path_parts(table: &mut toml::Table, parts: &[&str]) {
    match parts {
        [] => {}
        [only] => {
            table.remove(*only);
        }
        [first, rest @ ..] => {
            if let Some(Value::Table(sub)) = table.get_mut(*first) {
                remove_dot_path_parts(sub, rest);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_merges_nested_tables_child_wins_on_leaf() {
        let mut base_lookup = BTreeMap::new();
        let base: toml::Table = toml::from_str(
            r#"
            [limit]
            context = 200000
            output = 8192
        "#,
        )
        .unwrap();
        base_lookup.insert("base-model".to_string(), base);

        let child: toml::Table = toml::from_str(
            r#"
            base_model = "base-model"
            [limit]
            output = 4096
        "#,
        )
        .unwrap();

        let resolved = resolve(&base_lookup, "child-model", child).unwrap();
        let limit = resolved.get("limit").unwrap().as_table().unwrap();
        assert_eq!(limit.get("context").unwrap().as_integer(), Some(200000));
        assert_eq!(limit.get("output").unwrap().as_integer(), Some(4096));
    }

    #[test]
    fn arrays_are_replaced_wholesale_not_appended() {
        let mut base_lookup = BTreeMap::new();
        let base: toml::Table = toml::from_str(r#"modalities = ["text"]"#).unwrap();
        base_lookup.insert("base-model".to_string(), base);

        let child: toml::Table = toml::from_str(
            r#"
            base_model = "base-model"
            modalities = ["text", "image"]
        "#,
        )
        .unwrap();

        let resolved = resolve(&base_lookup, "child-model", child).unwrap();
        let modalities: Vec<&str> = resolved
            .get("modalities")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(modalities, vec!["text", "image"]);
    }

    #[test]
    fn base_model_omit_removes_dot_path_after_merge() {
        let mut base_lookup = BTreeMap::new();
        let base: toml::Table = toml::from_str(
            r#"
            [cost]
            input = 1.0
            output = 2.0
        "#,
        )
        .unwrap();
        base_lookup.insert("base-model".to_string(), base);

        let child: toml::Table = toml::from_str(
            r#"
            base_model = "base-model"
            base_model_omit = ["cost.output"]
        "#,
        )
        .unwrap();

        let resolved = resolve(&base_lookup, "child-model", child).unwrap();
        let cost = resolved.get("cost").unwrap().as_table().unwrap();
        assert!(cost.get("input").is_some());
        assert!(cost.get("output").is_none());
    }

    #[test]
    fn multi_level_chain_resolves_transitively() {
        let mut base_lookup = BTreeMap::new();
        let a: toml::Table = toml::from_str(
            r#"
            [limit]
            context = 100000
            [cost]
            input = 1.0
        "#,
        )
        .unwrap();
        base_lookup.insert("a".to_string(), a);

        let b: toml::Table = toml::from_str(
            r#"
            base_model = "a"
            [cost]
            output = 2.0
        "#,
        )
        .unwrap();
        base_lookup.insert("b".to_string(), b);

        let c: toml::Table = toml::from_str(
            r#"
            base_model = "b"
            [limit]
            output = 4096
        "#,
        )
        .unwrap();

        let resolved = resolve(&base_lookup, "c", c).unwrap();
        let limit = resolved.get("limit").unwrap().as_table().unwrap();
        assert_eq!(limit.get("context").unwrap().as_integer(), Some(100000));
        assert_eq!(limit.get("output").unwrap().as_integer(), Some(4096));
        let cost = resolved.get("cost").unwrap().as_table().unwrap();
        assert_eq!(cost.get("input").unwrap().as_float(), Some(1.0));
        assert_eq!(cost.get("output").unwrap().as_float(), Some(2.0));
        assert_eq!(resolved.get("id").unwrap().as_str(), Some("c"));
    }

    #[test]
    fn cycle_between_two_models_is_detected() {
        let mut base_lookup = BTreeMap::new();
        let a: toml::Table = toml::from_str(r#"base_model = "b""#).unwrap();
        let b: toml::Table = toml::from_str(r#"base_model = "a""#).unwrap();
        base_lookup.insert("a".to_string(), a.clone());
        base_lookup.insert("b".to_string(), b);

        let err = resolve(&base_lookup, "a", a).unwrap_err();
        assert_eq!(err, ResolveError::Cycle("a".to_string()));
    }

    #[test]
    fn missing_base_model_is_an_error() {
        let base_lookup = BTreeMap::new();
        let table: toml::Table = toml::from_str(r#"base_model = "nonexistent""#).unwrap();
        let err = resolve(&base_lookup, "child", table).unwrap_err();
        assert_eq!(err, ResolveError::MissingBase("nonexistent".to_string()));
    }

    #[test]
    fn id_is_injected_from_filename_argument_not_contents() {
        let base_lookup = BTreeMap::new();
        let table: toml::Table = toml::from_str(r#"id = "wrong-id-from-file""#).unwrap();
        let resolved = resolve(&base_lookup, "correct-id", table).unwrap();
        assert_eq!(resolved.get("id").unwrap().as_str(), Some("correct-id"));
    }

    #[test]
    fn no_base_model_returns_table_unchanged_with_id_injected() {
        let base_lookup = BTreeMap::new();
        let table: toml::Table = toml::from_str(
            r#"
            status = "stable"
            [cost]
            input = 1.0
        "#,
        )
        .unwrap();
        let resolved = resolve(&base_lookup, "solo", table).unwrap();
        assert_eq!(resolved.get("status").unwrap().as_str(), Some("stable"));
        assert_eq!(resolved.get("id").unwrap().as_str(), Some("solo"));
        assert!(resolved.get("base_model").is_none());
    }
}
