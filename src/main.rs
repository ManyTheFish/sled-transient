use sled_transient::TransientTree;
use std::time::Duration;
use sled::Db;

fn main() {
    let db = Db::start_default("hello").unwrap();

    let tree = TransientTree::new(&db, Duration::from_secs(3), b"salut").unwrap();

    tree.set("lol", b"kero").unwrap();
    assert!(tree.get("lol").unwrap().is_some());
    std::thread::sleep(Duration::from_secs(1));

    tree.set("lol", b"kero").unwrap();
    assert!(tree.get("lol").unwrap().is_some());
    std::thread::sleep(Duration::from_secs(1));

    assert!(tree.get("lol").unwrap().is_some());

    tree.set("lul", b"kero").unwrap();
    let inner_tree = db.open_tree("salut").unwrap();
    assert!(inner_tree.get("lol").unwrap().is_some());
    assert!(inner_tree.get("lul").unwrap().is_some());
    std::thread::sleep(Duration::from_secs(5));
    assert!(inner_tree.get("lul").unwrap().is_none());
    assert!(inner_tree.get("lol").unwrap().is_none());
}