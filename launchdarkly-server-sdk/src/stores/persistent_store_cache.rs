use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;

use moka::sync::Cache;

use super::store_types::StorageItem;

pub(super) struct CachePair<T> {
    all: Cache<String, HashMap<String, StorageItem<T>>>,
    single: Cache<String, StorageItem<T>>,
    cache_name: String,
}

impl<T: 'static + Sync + Send + Clone> CachePair<T> {
    pub fn new(cache_name: String, cache_ttl: Option<Duration>) -> CachePair<T> {
        match cache_ttl {
            Some(ttl) => CachePair {
                all: Cache::builder().time_to_live(ttl).build(),
                single: Cache::builder().time_to_live(ttl).build(),
                cache_name,
            },
            None => CachePair {
                all: Cache::builder().build(),
                single: Cache::builder().build(),
                cache_name,
            },
        }
    }

    pub fn cache_is_infinite(&self) -> bool {
        self.all.policy().time_to_live().is_none()
    }

    pub fn get_all(&self) -> Option<HashMap<String, StorageItem<T>>> {
        self.all.get(&self.all_key())
    }

    pub fn get_one(&self, key: &str) -> Option<StorageItem<T>> {
        self.single.get(&self.single_key(key))
    }

    pub fn insert_single(&self, item: StorageItem<T>, key: &str) {
        self.insert_into_cache(&self.single, self.single_key(key), item)
    }

    pub fn insert_all(&self, data: HashMap<String, StorageItem<T>>) {
        self.insert_into_cache(&self.all, self.all_key(), data)
    }

    fn insert_into_cache<K, V>(&self, cache: &Cache<K, V>, key: K, value: V)
    where
        K: Hash + Eq + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        match cache.policy().time_to_live() {
            None => cache.insert(key, value),
            Some(duration) if !duration.is_zero() => cache.insert(key, value),
            _ => (),
        }
    }

    fn all_key(&self) -> String {
        format!("all:{}", self.cache_name)
    }

    fn single_key(&self, key: &str) -> String {
        format!("{}:{}", self.cache_name, key)
    }

    pub fn invalidate_all(&self) {
        self.all.invalidate_all();
    }

    pub fn invalidate_single(&self, key: &str) {
        self.single.invalidate(&self.single_key(key));
    }

    pub fn invalidate_everything(&self) {
        self.all.invalidate_all();
        self.single.invalidate_all();
    }
}
