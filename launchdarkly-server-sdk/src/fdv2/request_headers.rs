/// The HTTP headers attached to every FDv2 request.
#[derive(Clone)]
pub(crate) struct RequestHeaders {
    headers: Vec<(&'static str, String)>,
}

impl RequestHeaders {
    pub(crate) fn new(sdk_key: &str, tags: Option<&str>, instance_id: &str) -> Self {
        let mut headers = vec![
            ("Authorization", sdk_key.to_string()),
            ("User-Agent", crate::USER_AGENT.clone()),
            (
                crate::LAUNCHDARKLY_INSTANCE_ID_HEADER,
                instance_id.to_string(),
            ),
        ];
        if let Some(tags) = tags {
            headers.push((crate::LAUNCHDARKLY_TAGS_HEADER, tags.to_string()));
        }
        Self { headers }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&'static str, &str)> + '_ {
        self.headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
    }
}
