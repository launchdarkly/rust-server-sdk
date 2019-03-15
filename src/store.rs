pub struct FeatureStore {
    pub data: Option<serde_json::Value>,
}

impl FeatureStore {
    pub fn new() -> FeatureStore {
        FeatureStore { data: None }
    }
}
