//! Endpoint-group registry.
//!
//! Endpoints declaring `?group=NAME` share a single learn-set across all
//! members — CLAUDE.md "Endpoint groups: declared implicitly by
//! `?group=name` ... Members share *only* the learn-set; filters and stats
//! remain per-endpoint." This is what keeps redundant parallel uplinks
//! (e.g. LTE + RFD900 dialled by `tcpc:…?group=uplink`) from silencing each
//! other's learn table: a frame learned by one member's reader is visible
//! to every other member's per-destination decision.
//!
//! The router task is the sole owner of every group's `LearnTable`. There
//! is no synchronisation beyond the router's single-task ownership — every
//! mutation (`touch`, `join`, `leave`) happens inside `handle_event` /
//! `handle_frame`, both of which run on the router's loop.
//!
//! Every group's learn table is sized at admission of its first member.
//! The per-endpoint learn-table capacity is the hardcoded
//! [`crate::endpoint::identity_flags::LEARN_CAPACITY`], so every group
//! converges on the same size regardless of admission order.

use std::collections::HashMap;
use std::sync::Arc;

use super::learn::LearnTable;

/// Shared learn-set for one named group plus a refcount of currently
/// registered members. When `members` falls to zero the [`GroupRegistry`]
/// drops the entry so a future `tcps:` re-accept (or a new admitted UDP
/// peer) with the same name starts from an empty table — that matches
/// the per-endpoint default behaviour the operator chose.
pub struct GroupLearn {
    pub learn: LearnTable,
    members: usize,
}

impl GroupLearn {
    pub fn member_count(&self) -> usize {
        self.members
    }
}

/// Per-name registry of group learn-sets, owned by the router task.
#[derive(Default)]
pub struct GroupRegistry {
    map: HashMap<Arc<str>, GroupLearn>,
}

impl GroupRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new member of `name`, creating the group's `LearnTable`
    /// at `capacity` on the first join (first-member-wins). Returns the
    /// total member count after the join — useful for logging.
    pub fn join(&mut self, name: Arc<str>, capacity: usize) -> usize {
        let entry = self.map.entry(name).or_insert_with(|| GroupLearn {
            learn: LearnTable::new(capacity),
            members: 0,
        });
        entry.members += 1;
        entry.members
    }

    /// Unregister one member of `name`. When the last member leaves the
    /// group's table is dropped.
    pub fn leave(&mut self, name: &Arc<str>) {
        let Some(entry) = self.map.get_mut(name) else {
            return;
        };
        entry.members = entry.members.saturating_sub(1);
        if entry.members == 0 {
            self.map.remove(name);
        }
    }

    pub fn get(&self, name: &Arc<str>) -> Option<&GroupLearn> {
        self.map.get(name)
    }

    pub fn get_mut(&mut self, name: &Arc<str>) -> Option<&mut GroupLearn> {
        self.map.get_mut(name)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mavlink::frame::NodeId;
    use std::time::Duration;
    use tokio::time::Instant;

    fn now_at(ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(ms)
    }

    #[test]
    fn join_creates_group_with_first_member_capacity() {
        let mut registry = GroupRegistry::new();
        let name = Arc::<str>::from("uplink");
        assert_eq!(registry.join(name.clone(), 7), 1);
        let entry = registry.get(&name).expect("group present after join");
        assert_eq!(entry.learn.capacity(), 7);
        assert_eq!(entry.member_count(), 1);
    }

    #[test]
    fn join_existing_group_keeps_capacity_silently() {
        // First-member-wins: capacity is set on first join; later joiners
        // get the existing table regardless of the capacity they passed.
        let mut registry = GroupRegistry::new();
        let name = Arc::<str>::from("uplink");
        registry.join(name.clone(), 7);
        assert_eq!(registry.join(name.clone(), 99), 2);
        let entry = registry.get(&name).expect("group still present");
        assert_eq!(entry.learn.capacity(), 7, "capacity must not be resized");
        assert_eq!(entry.member_count(), 2);
    }

    #[test]
    fn leave_drops_group_when_last_member_leaves() {
        let mut registry = GroupRegistry::new();
        let name = Arc::<str>::from("uplink");
        registry.join(name.clone(), 4);
        registry.join(name.clone(), 4);
        assert_eq!(registry.len(), 1);
        registry.leave(&name);
        assert!(registry.get(&name).is_some(), "still has one member");
        registry.leave(&name);
        assert!(
            registry.get(&name).is_none(),
            "group should be removed at last leave"
        );
        assert!(registry.is_empty());
    }

    #[test]
    fn leave_unknown_group_is_a_noop() {
        let mut registry = GroupRegistry::new();
        let name = Arc::<str>::from("does-not-exist");
        registry.leave(&name);
        assert!(registry.is_empty());
    }

    #[test]
    fn shared_learn_is_observable_from_every_handle() {
        // Touching the group's table from one access path must be visible
        // when read back via the same registry — confirms the registry
        // returns the same underlying table on every `get_mut`.
        let mut registry = GroupRegistry::new();
        let name = Arc::<str>::from("uplink");
        registry.join(name.clone(), 4);
        let group_mut = registry.get_mut(&name).unwrap();
        group_mut.learn.touch(NodeId::new(7, 1), now_at(0));
        let group_ref = registry.get(&name).unwrap();
        assert!(group_ref.learn.contains(NodeId::new(7, 1)));
    }
}
