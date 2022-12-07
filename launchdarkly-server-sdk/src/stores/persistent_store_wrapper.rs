use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;
use std::time::Duration;

use launchdarkly_server_sdk_evaluation::{Flag, Segment, Store};

use super::persistent_store::PersistentDataStore;
use super::persistent_store_cache::CachePair;
use super::store::{DataStore, UpdateError};
use super::store_types::{
    AllData, DataKind, PatchTarget, SerializeToSerializedItem, SerializedItem, StorageItem,
};

trait WithKind {
    const KIND: DataKind;
}

impl WithKind for StorageItem<Flag> {
    const KIND: DataKind = DataKind::Flag;
}

impl WithKind for StorageItem<Segment> {
    const KIND: DataKind = DataKind::Segment;
}

pub(super) struct PersistentDataStoreWrapper {
    store: Box<dyn PersistentDataStore>,
    flags: CachePair<Flag>,
    segments: CachePair<Segment>,
}

impl PersistentDataStoreWrapper {
    pub(super) fn new(store: Box<dyn PersistentDataStore>, cache_ttl: Option<Duration>) -> Self {
        Self {
            store,
            flags: CachePair::new(String::from("flags"), cache_ttl),
            segments: CachePair::new(String::from("segments"), cache_ttl),
        }
    }

    fn upsert_storage_item<T>(
        &mut self,
        key: &str,
        data: StorageItem<T>,
    ) -> Result<bool, UpdateError>
    where
        StorageItem<T>: WithKind,
        StorageItem<T>: SerializeToSerializedItem,
    {
        let serialized = data
            .serialize_to_serialized_item()
            .map_err(UpdateError::ParseError)?;
        let was_updated = self.store.upsert(StorageItem::<T>::KIND, key, serialized)?;

        Ok(was_updated)
    }

    fn add_to_cache<T: 'static + Sync + Send + Clone>(
        was_updated: bool,
        cache: &CachePair<T>,
        key: &str,
        data: StorageItem<T>,
    ) {
        if was_updated {
            cache.insert_single(data.clone(), key);
            // If the cache is infinite, we need to update the all flags cache. Otherwise, we can
            // just invalidate the cache and let it re-populate the next time it is required.
            if cache.cache_is_infinite() {
                if let Some(mut map) = cache.get_all() {
                    map.insert(key.to_string(), data);
                    cache.insert_all(map);
                }
            } else {
                cache.invalidate_all();
            }
        } else {
            cache.invalidate_all();
            cache.invalidate_single(key);
        }
    }

    fn upsert_flag(&mut self, flag_key: &str, data: StorageItem<Flag>) -> Result<(), UpdateError> {
        let was_updated = self.upsert_storage_item(flag_key, data.clone())?;

        Self::add_to_cache(was_updated, &self.flags, flag_key, data);
        if !was_updated {
            let _ = self.flag(flag_key); // Force repopulating the cache
        }

        Ok(())
    }

    fn upsert_segment(
        &mut self,
        segment_key: &str,
        data: StorageItem<Segment>,
    ) -> Result<(), UpdateError> {
        let was_updated = self.upsert_storage_item(segment_key, data.clone())?;

        Self::add_to_cache(was_updated, &self.segments, segment_key, data);
        if !was_updated {
            let _ = self.segment(segment_key); // Force repopulating the cache
        }

        Ok(())
    }

    fn cache_flags(&self, flags: HashMap<String, StorageItem<Flag>>) {
        self.flags.insert_all(flags.clone());

        flags.into_iter().for_each(|(key, flag)| {
            self.flags.insert_single(flag, &key);
        });
    }

    fn cache_segments(&self, segments: HashMap<String, StorageItem<Segment>>) {
        self.segments.insert_all(segments.clone());

        segments.into_iter().for_each(|(key, segment)| {
            self.segments.insert_single(segment, &key);
        });
    }

    fn cache_items(&self, all_data: AllData<StorageItem<Flag>, StorageItem<Segment>>) {
        self.cache_flags(all_data.flags);
        self.cache_segments(all_data.segments);
        debug!("flag and segment caches have been updated");
    }
}

impl Store for PersistentDataStoreWrapper {
    fn flag(&self, key: &str) -> Option<Flag> {
        if let Some(item) = self.flags.get_one(key) {
            return item.into();
        }

        match self.store.flag(key) {
            Ok(Some(serialized_item)) => {
                let storage_item: Result<StorageItem<Flag>, serde_json::Error> =
                    serialized_item.try_into();
                match storage_item {
                    Ok(item) => {
                        self.flags.insert_single(item.clone(), key);
                        item.into()
                    }
                    Err(e) => {
                        warn!("failed to convert serialized item into flag: {}", e);
                        None
                    }
                }
            }
            Ok(None) => None,
            Err(e) => {
                warn!("persistent store failed to retrieve flag: {}", e);
                None
            }
        }
    }

    fn segment(&self, key: &str) -> Option<Segment> {
        if let Some(item) = self.segments.get_one(key) {
            return item.into();
        }

        match self.store.segment(key) {
            Ok(Some(serialized_item)) => {
                let storage_item: Result<StorageItem<Segment>, serde_json::Error> =
                    serialized_item.try_into();
                match storage_item {
                    Ok(item) => {
                        self.segments.insert_single(item.clone(), key);
                        item.into()
                    }
                    Err(e) => {
                        warn!("failed to convert serialized item into segment: {}", e);
                        None
                    }
                }
            }
            Ok(None) => None,
            Err(e) => {
                warn!("persistent store failed to retrieve segment: {}", e);
                None
            }
        }
    }
}

impl DataStore for PersistentDataStoreWrapper {
    fn init(&mut self, all_data: AllData<Flag, Segment>) {
        self.flags.invalidate_everything();
        self.segments.invalidate_everything();

        let serialized_data = AllData::<SerializedItem, SerializedItem>::try_from(all_data.clone());

        match serialized_data {
            Err(e) => warn!(
                "failed to deserialize payload; cannot initialize store {}",
                e
            ),
            Ok(data) => {
                let result = self.store.init(data);

                match result {
                    Ok(()) => {
                        debug!("data store has been updated with new flag data");
                        self.cache_items(all_data.into());
                    }
                    Err(e) if self.flags.cache_is_infinite() => {
                        warn!("failed to init store. Updating non-expiring cache: {}", e);
                        self.cache_items(all_data.into())
                    }
                    _ => (),
                };
            }
        }
    }

    fn all_flags(&self) -> HashMap<String, Flag> {
        if let Some(flag_items) = self.flags.get_all() {
            let flag_iter = flag_items.into_iter().filter_map(|(key, item)| match item {
                StorageItem::Item(flag) => Some((key, flag)),
                StorageItem::Tombstone(_) => None,
            });
            return HashMap::from_iter(flag_iter);
        }

        match self.store.all_flags() {
            Ok(serialized_flags) => {
                let flags: Result<HashMap<String, StorageItem<Flag>>, serde_json::Error> =
                    serialized_flags
                        .into_iter()
                        .map(|(key, flag)| match flag.try_into() {
                            Ok(item) => Ok((key, item)),
                            Err(e) => Err(e),
                        })
                        .collect();

                match flags {
                    Ok(flags) => {
                        self.cache_flags(flags.clone());
                        let flag_iter = flags.into_iter().filter_map(|(key, item)| match item {
                            StorageItem::Item(flag) => Some((key, flag)),
                            StorageItem::Tombstone(_) => None,
                        });
                        HashMap::from_iter(flag_iter)
                    }
                    Err(e) => {
                        warn!("failed to convert serialized items into flags: {}", e);
                        HashMap::new()
                    }
                }
            }
            Err(e) => {
                warn!("persistent store failed to retrieve all flags: {}", e);
                HashMap::new()
            }
        }
    }

    fn upsert(&mut self, key: &str, data: PatchTarget) -> Result<(), UpdateError> {
        match data {
            PatchTarget::Flag(item) => self.upsert_flag(key, item),
            PatchTarget::Segment(item) => self.upsert_segment(key, item),
            PatchTarget::Other(v) => Err(UpdateError::InvalidTarget(
                "flag or segment".to_string(),
                format!("{:?}", v),
            )),
        }
    }

    fn to_store(&self) -> &dyn Store {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::stores::{
        persistent_store::tests::InMemoryPersistentDataStore,
        store::DataStore,
        store_types::{PatchTarget, StorageItem},
    };
    use launchdarkly_server_sdk_evaluation::Store;
    use maplit::hashmap;

    use crate::stores::{persistent_store::tests::NullPersistentDataStore, store_types::AllData};
    use crate::test_common::{basic_flag, basic_segment};
    use std::{collections::HashMap, time::Duration};

    use super::PersistentDataStoreWrapper;

    #[test]
    fn can_retrieve_flags_without_cache() {
        let store = InMemoryPersistentDataStore {
            data: AllData {
                flags: HashMap::new(),
                segments: HashMap::new(),
            },
            initialized: true,
        };

        let mut wrapper =
            PersistentDataStoreWrapper::new(Box::new(store), Some(Duration::from_secs(0)));

        let all_data = AllData {
            flags: hashmap!["flag".into() => basic_flag("flag")],
            segments: hashmap!["segment".into() => basic_segment("segment")],
        };

        wrapper.init(all_data);
        assert_eq!(1, wrapper.all_flags().len());

        let flag = wrapper.flag("flag").unwrap();
        assert_eq!(flag.key, "flag");

        let segment = wrapper.segment("segment").unwrap();
        assert_eq!(segment.key, "segment");
    }

    #[test]
    fn retrieving_flags_uses_cache() {
        let store = NullPersistentDataStore { initialized: false };

        let mut wrapper =
            PersistentDataStoreWrapper::new(Box::new(store), Some(Duration::from_secs(100)));

        let initial_flag = basic_flag("flag");

        let all_data = AllData {
            flags: hashmap!["flag".into() => initial_flag],
            segments: HashMap::new(),
        };

        wrapper.init(all_data);

        assert!(wrapper.flag("flag").is_some());

        let updated_flag = basic_flag("updated-flag");

        assert!(wrapper
            .upsert("flag", PatchTarget::Flag(StorageItem::Item(updated_flag)))
            .is_ok());

        let flag = wrapper.flag("flag").unwrap();
        assert_eq!(flag.key, "updated-flag");
    }

    #[test]
    fn retrieving_segments_uses_cache() {
        let store = NullPersistentDataStore { initialized: false };

        let mut wrapper =
            PersistentDataStoreWrapper::new(Box::new(store), Some(Duration::from_secs(100)));

        let initial_segment = basic_segment("segment");

        let all_data = AllData {
            flags: HashMap::new(),
            segments: hashmap!["segment".into() => initial_segment],
        };

        wrapper.init(all_data);

        assert!(wrapper.segment("segment").is_some());

        let updated_segment = basic_segment("updated-segment");

        assert!(wrapper
            .upsert(
                "segment",
                PatchTarget::Segment(StorageItem::Item(updated_segment))
            )
            .is_ok());

        let segment = wrapper.segment("segment").unwrap();
        assert_eq!(segment.key, "updated-segment");
    }

    #[test]
    fn cache_expires() {
        let store = NullPersistentDataStore { initialized: false };

        let mut wrapper =
            PersistentDataStoreWrapper::new(Box::new(store), Some(Duration::from_millis(100)));

        let initial_flag = basic_flag("flag");
        let initial_segment = basic_segment("segment");

        let all_data = AllData {
            flags: hashmap!["flag".into() => initial_flag],
            segments: hashmap!["segment".into() => initial_segment],
        };

        wrapper.init(all_data);

        assert!(wrapper.flag("flag").is_some());
        assert!(wrapper.segment("segment").is_some());

        std::thread::sleep(Duration::from_millis(750));

        assert!(wrapper.flag("flag").is_none());
        assert!(wrapper.segment("segment").is_none());
    }

    #[test]
    fn cache_that_never_expires_should_update_all_flags_cache_when_flag_is_updated() {
        let store = NullPersistentDataStore { initialized: false };

        let mut wrapper = PersistentDataStoreWrapper::new(Box::new(store), None);

        let mut initial_flag = basic_flag("flag");
        initial_flag.version = 1;
        let initial_segment = basic_segment("segment");

        let all_data = AllData {
            flags: hashmap!["flag".into() => initial_flag],
            segments: hashmap!["segment".into() => initial_segment],
        };

        wrapper.init(all_data);

        let mut updated_flag = basic_flag("flag");
        updated_flag.version = 2;

        let result = wrapper.upsert_flag("flag", StorageItem::Item(updated_flag));
        assert!(result.is_ok());

        let flags = wrapper.all_flags();
        let retrieved_flag = flags.get("flag").unwrap();

        assert_eq!(retrieved_flag.version, 2);
    }

    #[test]
    fn cache_that_never_expires_should_update_all_segments_cache_when_segment_is_updated() {
        let store = NullPersistentDataStore { initialized: false };

        let mut wrapper = PersistentDataStoreWrapper::new(Box::new(store), None);

        let initial_flag = basic_flag("flag");
        let mut initial_segment = basic_segment("segment");
        initial_segment.version = 1;

        let all_data = AllData {
            flags: hashmap!["flag".into() => initial_flag],
            segments: hashmap!["segment".into() => initial_segment],
        };

        wrapper.init(all_data);

        let mut updated_segment = basic_segment("segment");
        updated_segment.version = 2;

        let result = wrapper.upsert_segment("segment", StorageItem::Item(updated_segment));
        assert!(result.is_ok());

        let segments = wrapper.segments.get_all().unwrap();
        let retrieved_segment = segments.get("segment").unwrap();

        match retrieved_segment {
            StorageItem::Item(segment) => assert_eq!(segment.version, 2),
            _ => panic!("Failed to retrieve correct segment"),
        };
    }
}
