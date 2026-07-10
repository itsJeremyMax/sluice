//! Diff between the currently loaded registry and a freshly resolved
//! models.dev-shaped source directory, as `sluice models diff` reports it.
//! Read-only: computing a [`RegistryDiff`] never writes anything — only
//! `sluice models update` does that, via [`super::Registry::write_json_file`].

use super::{ModelFacts, ModelKey, Registry};

/// A `(provider, id)` entry present in both registries whose operationally
/// relevant facts differ between `current` and `source`. Limited to the
/// fields `sluice models diff` is specified to report: context window and
/// per-token costs. Identified by the composite `(provider, id)` key, not
/// bare id — the same id served by two different providers is two distinct
/// entries.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelDiff {
    pub provider: String,
    pub id: String,
    pub context_changed: bool,
    pub cost_input_changed: bool,
    pub cost_output_changed: bool,
}

/// Added/removed/changed `(provider, id)` entries between `current` (the
/// already-loaded registry) and `source` (freshly resolved from a candidate
/// directory via [`super::Registry::from_dir`]). All three lists are sorted
/// by `(provider, id)` for deterministic output.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RegistryDiff {
    pub added: Vec<ModelKey>,
    pub removed: Vec<ModelKey>,
    pub changed: Vec<ModelDiff>,
}

impl RegistryDiff {
    /// Whether `source` and `current` are equivalent for the purposes of
    /// this diff (no additions, removals, or changed cost/context fields).
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// Compute the [`RegistryDiff`] of `source` against `current`. `current` is
/// typically `Registry::load()`; `source` is typically
/// `Registry::from_dir(<path>)` resolved from a candidate models.dev-shaped
/// directory. Pure computation — never touches the filesystem.
pub fn diff(current: &Registry, source: &Registry) -> RegistryDiff {
    let mut added: Vec<ModelKey> = source
        .models
        .keys()
        .filter(|key| !current.models.contains_key(*key))
        .cloned()
        .collect();
    added.sort();

    let mut removed: Vec<ModelKey> = current
        .models
        .keys()
        .filter(|key| !source.models.contains_key(*key))
        .cloned()
        .collect();
    removed.sort();

    let mut changed: Vec<ModelDiff> = current
        .models
        .iter()
        .filter_map(|(key, cur)| source.models.get(key).map(|new| (key, cur, new)))
        .filter_map(|((provider, id), cur, new)| field_diff(provider, id, cur, new))
        .collect();
    changed.sort_by(|a, b| (&a.provider, &a.id).cmp(&(&b.provider, &b.id)));

    RegistryDiff {
        added,
        removed,
        changed,
    }
}

/// Build a [`ModelDiff`] for `(provider, id)` if any of
/// context/cost_input/cost_output differ between `cur` and `new`, else
/// `None`.
fn field_diff(provider: &str, id: &str, cur: &ModelFacts, new: &ModelFacts) -> Option<ModelDiff> {
    let context_changed = cur.context != new.context;
    let cost_input_changed = cur.cost_input != new.cost_input;
    let cost_output_changed = cur.cost_output != new.cost_output;
    if context_changed || cost_input_changed || cost_output_changed {
        Some(ModelDiff {
            provider: provider.to_string(),
            id: id.to_string(),
            context_changed,
            cost_input_changed,
            cost_output_changed,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn facts(
        id: &str,
        context: Option<u64>,
        cost_input: Option<f64>,
        cost_output: Option<f64>,
    ) -> ModelFacts {
        ModelFacts {
            id: id.to_string(),
            provider: "test".to_string(),
            context,
            max_output: None,
            cost_input,
            cost_output,
            modalities: vec![],
            tool_call: false,
            status: None,
        }
    }

    fn registry(models: Vec<ModelFacts>) -> Registry {
        let mut map = HashMap::new();
        for m in models {
            map.insert((m.provider.clone(), m.id.clone()), m);
        }
        Registry { models: map }
    }

    #[test]
    fn detects_added_and_removed_ids() {
        let current = registry(vec![facts("a", Some(1), Some(1.0), Some(2.0))]);
        let source = registry(vec![facts("b", Some(1), Some(1.0), Some(2.0))]);
        let d = diff(&current, &source);
        assert_eq!(d.added, vec![("test".to_string(), "b".to_string())]);
        assert_eq!(d.removed, vec![("test".to_string(), "a".to_string())]);
        assert!(d.changed.is_empty());
        assert!(!d.is_empty());
    }

    #[test]
    fn detects_changed_context_and_one_cost_field() {
        let current = registry(vec![facts("a", Some(100), Some(1.0), Some(2.0))]);
        let source = registry(vec![facts("a", Some(200), Some(1.0), Some(3.0))]);
        let d = diff(&current, &source);
        assert!(d.added.is_empty());
        assert!(d.removed.is_empty());
        assert_eq!(d.changed.len(), 1);
        let c = &d.changed[0];
        assert_eq!(c.id, "a");
        assert_eq!(c.provider, "test");
        assert!(c.context_changed);
        assert!(!c.cost_input_changed);
        assert!(c.cost_output_changed);
    }

    #[test]
    fn identical_registries_yield_empty_diff() {
        let current = registry(vec![facts("a", Some(1), Some(1.0), Some(2.0))]);
        let source = registry(vec![facts("a", Some(1), Some(1.0), Some(2.0))]);
        assert!(diff(&current, &source).is_empty());
    }

    #[test]
    fn changed_ids_are_sorted() {
        let current = registry(vec![
            facts("z", Some(1), None, None),
            facts("a", Some(1), None, None),
        ]);
        let source = registry(vec![
            facts("z", Some(2), None, None),
            facts("a", Some(2), None, None),
        ]);
        let d = diff(&current, &source);
        let ids: Vec<&str> = d.changed.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "z"]);
    }

    #[test]
    fn same_id_different_provider_is_added_and_removed_not_changed() {
        // Two entries with the same bare id but different providers are
        // distinct composite-key entries: swapping which provider serves an
        // id must show up as one removed + one added, never as a "changed"
        // entry that conflates the two providers' facts.
        let mut a = facts("shared", Some(100), Some(1.0), Some(2.0));
        a.provider = "provider-a".to_string();
        let mut b = facts("shared", Some(100), Some(1.0), Some(2.0));
        b.provider = "provider-b".to_string();

        let current = registry(vec![a]);
        let source = registry(vec![b]);
        let d = diff(&current, &source);

        assert_eq!(
            d.added,
            vec![("provider-b".to_string(), "shared".to_string())]
        );
        assert_eq!(
            d.removed,
            vec![("provider-a".to_string(), "shared".to_string())]
        );
        assert!(d.changed.is_empty());
    }
}
