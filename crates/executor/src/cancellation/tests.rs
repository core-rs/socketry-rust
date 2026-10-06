// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;

#[test]
fn dropped_children_release_their_registrations_and_ancestors() {
    let root = Cancellation::new();
    let node = root.node.as_ref().unwrap();
    for _ in 0..100 {
        let child = root.child();
        let grandchild = child.child();
        drop(child);
        drop(grandchild);
        assert!(node.children.lock().unwrap().is_empty());
        assert_eq!(Arc::strong_count(node), 1);
    }
}

#[test]
fn an_exclusively_owned_deep_chain_is_released_without_recursive_destruction() {
    std::thread::Builder::new()
        .stack_size(64 * 1024)
        .spawn(|| {
            let root = Cancellation::new();
            let weak = Arc::downgrade(root.node.as_ref().unwrap());
            let mut leaf = root.clone();
            for _ in 0..10_000 {
                leaf = leaf.child();
            }
            drop(root);
            assert!(weak.upgrade().is_some());
            drop(leaf);
            assert!(weak.upgrade().is_none());
        })
        .unwrap()
        .join()
        .unwrap();
}
