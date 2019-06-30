use sled::{Db, Tree, IVec};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;

#[derive(Debug, Clone)]
pub struct TransientTree {
    tree: Arc<Tree>,
    keys_times: Arc<Tree>,
    times_keys: Arc<Tree>,
    ttl: Duration,
}

fn janitor(tree: TransientTree) {
    loop {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let limit = (now + 1).to_be_bytes();
        let expired = tree.times_keys.range(..limit);
        for result in expired {
            let (_, key) = result.unwrap();
            tree.del(key).unwrap();
        }
        thread::sleep(Duration::from_secs(1));
    }
}

impl TransientTree {
    pub fn new(db: &Db, ttl: Duration, name: &[u8]) -> sled::Result<TransientTree> {
        let tree = db.open_tree(name)?;

        let keys_times = db.open_tree([b"__transient_times_", name].concat())?;
        let times_keys = db.open_tree([b"__transient_keys_", name].concat())?;
        let transient_tree = TransientTree { tree, keys_times, times_keys, ttl };

        let transient_tree_clone = transient_tree.clone();
        thread::spawn(move || janitor(transient_tree_clone));

        Ok(transient_tree)
    }

    pub fn set<K, V>(&self, key: K, value: V) -> sled::Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        IVec: From<V>,
    {
        let end_date = SystemTime::now().checked_add(self.ttl).unwrap();
        let end_timestamp = end_date.duration_since(UNIX_EPOCH).unwrap().as_secs();

        let old = self.tree.set(key.as_ref(), value)?;
        self.times_keys.set::<_, &[u8]>([&end_timestamp.to_be_bytes(), key.as_ref()].concat(), key.as_ref())?;
        let old_time = self.keys_times.set::<_, &[u8]>(key.as_ref(), &end_timestamp.to_be_bytes())?;
        if let Some(old_time) = old_time {
            self.times_keys.del([old_time.as_ref(), key.as_ref()].concat()).unwrap();
        }
        Ok(old)
    }

    pub fn get<K>(&self, key: K) -> sled::Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
    {
        let timestamp = self.keys_times.get(key.as_ref())?;
        let timestamp = timestamp.map(|bytes| {
            let array: [u8; 8] = bytes.as_ref().try_into().unwrap();
            u64::from_be_bytes(array)
        });

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        match timestamp {
            Some(timestamp) if timestamp <= now => {
                self.tree.del(key.as_ref())?;
                self.keys_times.del(key.as_ref())?;
                self.times_keys.del([&timestamp.to_be_bytes(), key.as_ref()].concat())?;
                Ok(None)
            },
            Some(_) => self.tree.get(key),
            None => Ok(None),
        }
    }

    pub fn del<K>(&self, key: K) -> sled::Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
    {
        let old = self.tree.del(key.as_ref())?;

        let timestamp = self.keys_times.del(key.as_ref())?;
        if let Some(timestamp) = timestamp.as_ref() {
            self.times_keys.del([timestamp, key.as_ref()].concat())?;
        }

        let timestamp = timestamp.map(|bytes| {
            let array: [u8; 8] = bytes.as_ref().try_into().unwrap();
            u64::from_be_bytes(array)
        });

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        match timestamp {
            Some(timestamp) if timestamp <= now => Ok(None),
            Some(_) => Ok(old),
            None => Ok(None),
        }
    }
}
