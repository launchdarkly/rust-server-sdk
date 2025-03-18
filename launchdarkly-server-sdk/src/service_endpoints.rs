use super::client::BuildError;

const DEFAULT_POLLING_BASE_URL: &str = "https://sdk.launchdarkly.com";
const DEFAULT_STREAM_BASE_URL: &str = "https://stream.launchdarkly.com";
const DEFAULT_EVENTS_BASE_URL: &str = "https://events.launchdarkly.com";

/// Specifies the base service URLs used by SDK components.
pub struct ServiceEndpoints {
    polling_base_url: String,
    streaming_base_url: String,
    events_base_url: String,
}

impl ServiceEndpoints {
    pub fn polling_base_url(&self) -> &str {
        self.polling_base_url.as_ref()
    }

    pub fn streaming_base_url(&self) -> &str {
        self.streaming_base_url.as_ref()
    }

    pub fn events_base_url(&self) -> &str {
        self.events_base_url.as_ref()
    }
}

/// Used for configuring the SDKs service URLs.
///
/// The default behavior, if you do not change any of these properties, is that the SDK will connect
/// to the standard endpoints in the LaunchDarkly production service. There are several use cases for
/// changing these properties:
///
/// - You are using the <a href="https://docs.launchdarkly.com/home/advanced/relay-proxy">LaunchDarkly
///   Relay Proxy</a>. In this case, use [ServiceEndpointsBuilder::relay_proxy] with the URL of the
///   relay proxy instance.
///
/// - You are connecting to a private instance of LaunchDarkly, rather than the standard production
///   services. In this case, there will be custom base URIs for each service. You need to configure
///   each endpoint with [ServiceEndpointsBuilder::polling_base_url],
///   [ServiceEndpointsBuilder::streaming_base_url], and [ServiceEndpointsBuilder::events_base_url].
///
/// - You are connecting to a test fixture that simulates the service endpoints. In this case, you
///   may set the base URLs to whatever you want, although the SDK will still set the URL paths to
///   the expected paths for LaunchDarkly services.
///
/// # Examples
///
/// Configure for a Relay Proxy instance.
/// ```
/// # use launchdarkly_server_sdk::{ServiceEndpointsBuilder, ConfigBuilder};
/// # fn main() {
///     ConfigBuilder::new("sdk-key").service_endpoints(ServiceEndpointsBuilder::new()
///         .relay_proxy("http://my-relay-hostname:8080"));
/// # }
/// ```
///
/// Configure for a private LaunchDarkly instance.
/// ```
/// # use launchdarkly_server_sdk::{ServiceEndpointsBuilder, ConfigBuilder};
/// # fn main() {
///    ConfigBuilder::new("sdk-key").service_endpoints(ServiceEndpointsBuilder::new()
///         .polling_base_url("https://sdk.my-private-instance.com")
///         .streaming_base_url("https://stream.my-private-instance.com")
///         .events_base_url("https://events.my-private-instance.com"));
/// # }
/// ```
#[derive(Clone)]
pub struct ServiceEndpointsBuilder {
    polling_base_url: Option<String>,
    streaming_base_url: Option<String>,
    events_base_url: Option<String>,
}

impl ServiceEndpointsBuilder {
    /// Create a new instance of [ServiceEndpointsBuilder] with no URLs specified.
    pub fn new() -> ServiceEndpointsBuilder {
        ServiceEndpointsBuilder {
            polling_base_url: None,
            streaming_base_url: None,
            events_base_url: None,
        }
    }

    /// Sets a custom base URL for the polling service.
    pub fn polling_base_url(&mut self, url: &str) -> &mut Self {
        self.polling_base_url = Some(String::from(url));
        self
    }

    /// Sets a custom base URL for the streaming service.
    pub fn streaming_base_url(&mut self, url: &str) -> &mut Self {
        self.streaming_base_url = Some(String::from(url));
        self
    }

    /// Sets a custom base URI for the events service.
    pub fn events_base_url(&mut self, url: &str) -> &mut Self {
        self.events_base_url = Some(String::from(url));
        self
    }

    /// Specifies a single base URL for a Relay Proxy instance.
    pub fn relay_proxy(&mut self, url: &str) -> &mut Self {
        self.polling_base_url = Some(String::from(url));
        self.streaming_base_url = Some(String::from(url));
        self.events_base_url = Some(String::from(url));
        self
    }

    /// Called internally by the SDK to create a configuration instance. Applications do not need
    /// to call this method.
    ///
    /// # Errors
    ///
    /// When using custom endpoints it is important that all of the URLs are set.
    /// If some URLs are set, but others are not, then this will return an error.
    /// If no URLs are set, then the default values will be used.
    /// This prevents a combination of custom and default values from being used.
    pub fn build(&self) -> Result<ServiceEndpoints, BuildError> {
        match (
            &self.polling_base_url,
            &self.streaming_base_url,
            &self.events_base_url,
        ) {
            (Some(polling_base_url), Some(streaming_base_url), Some(events_base_url)) => {
                Ok(ServiceEndpoints {
                    polling_base_url: polling_base_url.trim_end_matches('/').to_string(),
                    streaming_base_url: streaming_base_url.trim_end_matches('/').to_string(),
                    events_base_url: events_base_url.trim_end_matches('/').to_string(),
                })
            }
            (None, None, None) => Ok(ServiceEndpoints {
                polling_base_url: String::from(DEFAULT_POLLING_BASE_URL),
                streaming_base_url: String::from(DEFAULT_STREAM_BASE_URL),
                events_base_url: String::from(DEFAULT_EVENTS_BASE_URL),
            }),
            _ => Err(BuildError::InvalidConfig(
                "If you specify any endpoints,\
             then you must specify all endpoints."
                    .into(),
            )),
        }
    }
}

impl Default for ServiceEndpointsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case("localhost", "localhost"; "requires no trimming")]
    #[test_case("http://localhost", "http://localhost"; "requires no trimming with scheme")]
    #[test_case("localhost/", "localhost"; "trims trailing slash")]
    #[test_case("http://localhost/", "http://localhost"; "trims trailing slash with scheme")]
    #[test_case("localhost////////", "localhost"; "trims multiple trailing slashes")]
    fn service_endpoints_trims_base_urls(url: &str, expected: &str) {
        let endpoints = ServiceEndpointsBuilder::new()
            .polling_base_url(url)
            .streaming_base_url(url)
            .events_base_url(url)
            .build()
            .expect("Provided URLs should parse successfully");

        assert_eq!(expected, endpoints.events_base_url());
        assert_eq!(expected, endpoints.streaming_base_url());
        assert_eq!(expected, endpoints.polling_base_url());
    }

    #[test]
    fn default_configuration() -> Result<(), BuildError> {
        let endpoints = ServiceEndpointsBuilder::new().build()?;
        assert_eq!(
            "https://events.launchdarkly.com",
            endpoints.events_base_url()
        );
        assert_eq!(
            "https://stream.launchdarkly.com",
            endpoints.streaming_base_url()
        );
        assert_eq!("https://sdk.launchdarkly.com", endpoints.polling_base_url());
        Ok(())
    }

    #[test]
    fn full_custom_configuration() -> Result<(), BuildError> {
        let endpoints = ServiceEndpointsBuilder::new()
            .polling_base_url("https://sdk.my-private-instance.com")
            .streaming_base_url("https://stream.my-private-instance.com")
            .events_base_url("https://events.my-private-instance.com")
            .build()?;

        assert_eq!(
            "https://events.my-private-instance.com",
            endpoints.events_base_url()
        );
        assert_eq!(
            "https://stream.my-private-instance.com",
            endpoints.streaming_base_url()
        );
        assert_eq!(
            "https://sdk.my-private-instance.com",
            endpoints.polling_base_url()
        );
        Ok(())
    }

    #[test]
    fn partial_definition() {
        assert!(ServiceEndpointsBuilder::new()
            .polling_base_url("https://sdk.my-private-instance.com")
            .build()
            .is_err());
    }

    #[test]
    fn configure_relay_proxy() -> Result<(), BuildError> {
        let endpoints = ServiceEndpointsBuilder::new()
            .relay_proxy("http://my-relay-hostname:8080")
            .build()?;
        assert_eq!("http://my-relay-hostname:8080", endpoints.events_base_url());
        assert_eq!(
            "http://my-relay-hostname:8080",
            endpoints.streaming_base_url()
        );
        assert_eq!(
            "http://my-relay-hostname:8080",
            endpoints.polling_base_url()
        );
        Ok(())
    }
}
