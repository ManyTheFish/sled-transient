use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sled::{Db, Tree, Event};

pub trait TransientExt {
    fn open_ttl_tree<V: AsRef<[u8]>>(&self, ttl: Duration, name: V) -> sled::Result<Arc<Tree>>;
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn expired_time(ttl: Duration) -> u64 {
    SystemTime::now()
        .checked_add(ttl)
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn concat<A: AsRef<[u8]>, B: AsRef<[u8]>>(a: A, b: B) -> Vec<u8> {
    [a.as_ref(), b.as_ref()].concat()
}

fn janitor(tree: Arc<Tree>, keys_times: Arc<Tree>, times_keys: Arc<Tree>) {
    loop {
        let limit = (now() + 1).to_be_bytes();
        let expired = times_keys.range(..limit);

        for result in expired {
            let (_, key) = result.unwrap();

            tree.del(&key).unwrap();
            let timestamp = keys_times.del(&key).unwrap();
            if let Some(timestamp) = timestamp.as_ref() {
                times_keys.del(concat(timestamp, key)).unwrap();
            }
        }

        thread::sleep(Duration::from_secs(1));
    }
}

fn reactor(tree: Arc<Tree>, keys_times: Arc<Tree>, times_keys: Arc<Tree>, ttl: Duration) {
    for event in tree.watch_prefix(vec![]) {
        match event {
            Event::Set(key, _) | Event::Merge(key, _) => {
                let end_timestamp = expired_time(ttl);
                times_keys.set::<_, &[u8]>(concat(end_timestamp.to_be_bytes(), &key), &key).unwrap();
                let old_time = keys_times.set(&key, &end_timestamp.to_be_bytes()).unwrap();

                if let Some(old_time) = old_time {
                    times_keys.del(concat(old_time, key)).unwrap();
                }
            },
            Event::Del(key) => {
                let timestamp = keys_times.del(&key).unwrap();
                if let Some(timestamp) = timestamp.as_ref() {
                    times_keys.del(concat(timestamp, key)).unwrap();
                }
            },
        }
    }
}

impl TransientExt for Db {
    fn open_ttl_tree<V: AsRef<[u8]>>(&self, ttl: Duration, name: V) -> sled::Result<Arc<Tree>> {
        let tree = self.open_tree(&name)?;
        let keys_times = self.open_tree([b"__transient_keys_", name.as_ref()].concat())?;
        let times_keys = self.open_tree([b"__transient_times_", name.as_ref()].concat())?;

        {
            let tree = tree.clone();
            let keys_times = keys_times.clone();
            let times_keys = times_keys.clone();
            thread::spawn(move || janitor(tree, keys_times, times_keys));
        }

        {
            let tree = tree.clone();
            thread::spawn(move || reactor(tree, keys_times, times_keys, ttl));
        }

        Ok(tree)
    }
}
