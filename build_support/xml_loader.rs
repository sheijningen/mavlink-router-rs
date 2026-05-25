// Generic include-graph walker with cycle detection.

#[derive(Debug)]
pub(crate) enum WalkError<K> {
    Cycle(K),
    LoadFailed { key: K, reason: String },
}

pub(crate) fn walk_includes<K, F>(root: K, mut load: F) -> Result<(), WalkError<K>>
where
    K: Clone + Eq + std::hash::Hash,
    F: FnMut(&K) -> Result<Vec<K>, String>,
{
    let mut visited: std::collections::HashSet<K> = std::collections::HashSet::new();
    let mut in_progress: std::collections::HashSet<K> = std::collections::HashSet::new();
    walk_inner(&root, &mut visited, &mut in_progress, &mut load)
}

fn walk_inner<K, F>(
    key: &K,
    visited: &mut std::collections::HashSet<K>,
    in_progress: &mut std::collections::HashSet<K>,
    load: &mut F,
) -> Result<(), WalkError<K>>
where
    K: Clone + Eq + std::hash::Hash,
    F: FnMut(&K) -> Result<Vec<K>, String>,
{
    // `in_progress` MUST be checked before `visited`: a diamond (same file
    // reached via two paths) inserts into `visited` on the first descent and
    // returns Ok on the second, but a cycle has the key in `in_progress`
    // while it's still mid-descent.
    if in_progress.contains(key) {
        return Err(WalkError::Cycle(key.clone()));
    }
    if !visited.insert(key.clone()) {
        return Ok(());
    }
    in_progress.insert(key.clone());

    let includes = load(key).map_err(|reason| WalkError::LoadFailed {
        key: key.clone(),
        reason,
    })?;
    for inc in includes {
        walk_inner(&inc, visited, in_progress, load)?;
    }

    in_progress.remove(key);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    type Graph = HashMap<&'static str, Vec<&'static str>>;

    fn loader_from(graph: Graph) -> impl FnMut(&&'static str) -> Result<Vec<&'static str>, String> {
        move |key: &&'static str| {
            graph
                .get(*key)
                .cloned()
                .ok_or_else(|| format!("not in graph: {key}"))
        }
    }

    #[test]
    fn leaf_only() {
        let graph: Graph = HashMap::from([("a", vec![])]);
        assert!(walk_includes("a", loader_from(graph)).is_ok());
    }

    #[test]
    fn linear_chain() {
        let graph: Graph = HashMap::from([("a", vec!["b"]), ("b", vec!["c"]), ("c", vec![])]);
        assert!(walk_includes("a", loader_from(graph)).is_ok());
    }

    #[test]
    fn self_include_is_cycle() {
        let graph: Graph = HashMap::from([("a", vec!["a"])]);
        let err = walk_includes("a", loader_from(graph)).unwrap_err();
        assert!(matches!(err, WalkError::Cycle(key) if key == "a"));
    }

    #[test]
    fn two_node_cycle() {
        let graph: Graph = HashMap::from([("a", vec!["b"]), ("b", vec!["a"])]);
        let err = walk_includes("a", loader_from(graph)).unwrap_err();
        assert!(matches!(err, WalkError::Cycle(_)));
    }

    #[test]
    fn longer_cycle() {
        let graph: Graph = HashMap::from([("a", vec!["b"]), ("b", vec!["c"]), ("c", vec!["a"])]);
        let err = walk_includes("a", loader_from(graph)).unwrap_err();
        assert!(matches!(err, WalkError::Cycle(_)));
    }

    #[test]
    fn diamond_visits_each_node_once() {
        let graph: Graph = HashMap::from([
            ("a", vec!["b", "c"]),
            ("b", vec!["d"]),
            ("c", vec!["d"]),
            ("d", vec![]),
        ]);
        let visited: RefCell<Vec<&'static str>> = RefCell::new(Vec::new());
        let result = walk_includes("a", |key: &&'static str| {
            visited.borrow_mut().push(*key);
            graph.get(*key).cloned().ok_or_else(String::new)
        });
        assert!(result.is_ok());
        let mut visited_keys = visited.borrow().clone();
        visited_keys.sort();
        assert_eq!(visited_keys, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn cycle_below_diamond_is_still_detected() {
        // a → {b, c}; b → d → e; c → d; e → d (cycle through diamond bottom)
        let graph: Graph = HashMap::from([
            ("a", vec!["b", "c"]),
            ("b", vec!["d"]),
            ("c", vec!["d"]),
            ("d", vec!["e"]),
            ("e", vec!["d"]),
        ]);
        let err = walk_includes("a", loader_from(graph)).unwrap_err();
        assert!(matches!(err, WalkError::Cycle(_)));
    }

    #[test]
    fn load_error_propagates() {
        let result: Result<(), WalkError<&'static str>> =
            walk_includes("root", |_| -> Result<Vec<&'static str>, String> {
                Err("simulated read failure".to_string())
            });
        match result {
            Err(WalkError::LoadFailed { key, reason }) => {
                assert_eq!(key, "root");
                assert!(reason.contains("simulated read failure"));
            }
            other => panic!("expected LoadFailed, got {other:?}"),
        }
    }
}
