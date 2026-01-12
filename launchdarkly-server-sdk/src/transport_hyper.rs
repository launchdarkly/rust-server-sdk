//! Hyper v1 transport implementation for LaunchDarkly SDK
//!
//! This module provides a production-ready [`HyperTransport`] implementation that
//! integrates hyper v1 with the LaunchDarkly SDK.

use crate::transport::{ByteStream, HttpTransport, ResponseFuture, TransportError};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;

/// A transport implementation using hyper v1.x
///
/// This struct wraps a hyper client and implements the [`HttpTransport`] trait
/// for use with the LaunchDarkly SDK.
///
/// # Default Configuration
///
/// By default, `HyperTransport` uses:
/// - HTTP-only connector (no TLS)
/// - Both HTTP/1.1 and HTTP/2 protocol support
/// - No timeout configuration
///
/// For HTTPS support, use [`HyperTransport::new_https()`] (requires the `rustls` feature)
/// or provide your own connector with [`HyperTransport::new_with_connector()`].
///
/// # Example
///
/// ```ignore
/// use launchdarkly_server_sdk::{HyperTransport, ConfigBuilder, EventProcessorBuilder};
///
/// # #[cfg(feature = "hyper-rustls")]
/// # {
/// // Use default HTTPS transport
/// let transport = HyperTransport::new_https();
///
/// let config = ConfigBuilder::new("sdk-key")
///     .event_processor(EventProcessorBuilder::new().transport(transport.clone()))
///     .build();
/// # }
/// ```
#[derive(Clone)]
pub struct HyperTransport<C = hyper_util::client::legacy::connect::HttpConnector> {
    client: HyperClient<C, BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>>,
}

impl HyperTransport {
    /// Create a new HyperTransport with default HTTP connector and no timeouts
    ///
    /// This creates a basic HTTP-only client that supports both HTTP/1 and HTTP/2.
    /// For HTTPS support, use [`HyperTransport::new_https()`] instead.
    ///
    /// # Example
    ///
    /// ```
    /// use launchdarkly_server_sdk::HyperTransport;
    ///
    /// let transport = HyperTransport::new();
    /// ```
    pub fn new() -> Self {
        let connector = hyper_util::client::legacy::connect::HttpConnector::new();
        let client = HyperClient::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }

    /// Create a new HyperTransport with HTTPS support using rustls
    ///
    /// This creates an HTTPS client that supports both HTTP/1 and HTTP/2 protocols
    /// with native certificate verification.
    ///
    /// This method is only available when the `rustls` feature is enabled.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[cfg(feature = "hyper-rustls")]
    /// # {
    /// use launchdarkly_server_sdk::HyperTransport;
    ///
    /// let transport = HyperTransport::new_https();
    /// # }
    /// ```
    #[cfg(feature = "hyper-rustls")]
    pub fn new_https() -> HyperTransport<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    > {
        use hyper_rustls::HttpsConnectorBuilder;

        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();

        let client = HyperClient::builder(TokioExecutor::new()).build(connector);
        HyperTransport { client }
    }
}

impl<C> HyperTransport<C> {
    /// Create a new HyperTransport with a custom connector
    ///
    /// This allows you to provide your own connector implementation, which is useful for:
    /// - Custom TLS configuration
    /// - Proxy support
    /// - Connection pooling customization
    /// - Custom DNS resolution
    ///
    /// # Example
    ///
    /// ```no_run
    /// use launchdarkly_server_sdk::HyperTransport;
    /// use hyper_util::client::legacy::connect::HttpConnector;
    ///
    /// let mut connector = HttpConnector::new();
    /// connector.set_nodelay(true);
    ///
    /// let transport = HyperTransport::new_with_connector(connector);
    /// ```
    pub fn new_with_connector(connector: C) -> Self
    where
        C: hyper_util::client::legacy::connect::Connect + Clone + Send + Sync + 'static,
    {
        let client = HyperClient::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }
}

impl Default for HyperTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> HttpTransport for HyperTransport<C>
where
    C: hyper_util::client::legacy::connect::Connect + Clone + Send + Sync + 'static,
{
    fn request(&self, request: Request<Bytes>) -> ResponseFuture {
        let (parts, body) = request.into_parts();

        // Convert Bytes to BoxBody for hyper
        let boxed_body: BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>> =
            if body.is_empty() {
                // Use Empty for requests with no body (e.g., GET requests)
                Empty::<Bytes>::new()
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    .boxed()
            } else {
                // Use Full for requests with a body
                Full::new(body)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    .boxed()
            };

        let hyper_req = hyper::Request::from_parts(parts, boxed_body);
        let client = self.client.clone();

        Box::pin(async move {
            // Make the request
            let resp = client
                .request(hyper_req)
                .await
                .map_err(TransportError::new)?;

            let (parts, body) = resp.into_parts();

            // Convert hyper's Incoming body to ByteStream
            let byte_stream: ByteStream = Box::pin(body_to_stream(body));

            Ok(Response::from_parts(parts, byte_stream))
        })
    }
}

/// Convert hyper's Incoming body to a Stream of Bytes
fn body_to_stream(
    body: Incoming,
) -> impl futures::Stream<Item = Result<Bytes, TransportError>> + Send {
    futures::stream::unfold(body, |mut body| async move {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    // Successfully got data frame
                    Some((Ok(data), body))
                } else {
                    // Skip non-data frames (trailers, etc.)
                    Some((
                        Err(TransportError::new(std::io::Error::other("non-data frame"))),
                        body,
                    ))
                }
            }
            Some(Err(e)) => {
                // Error reading frame
                Some((Err(TransportError::new(e)), body))
            }
            None => {
                // End of stream
                None
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyper_transport_new() {
        let transport = HyperTransport::new();
        // If we can create it without panic, the test passes
        // This verifies the default HTTP connector is set up correctly
        drop(transport);
    }

    #[test]
    fn test_hyper_transport_default() {
        let transport = HyperTransport::default();
        // Verify Default trait implementation
        drop(transport);
    }

    #[cfg(feature = "hyper-rustls")]
    #[test]
    fn test_hyper_transport_new_https() {
        let transport = HyperTransport::new_https();
        // If we can create it without panic, the test passes
        // This verifies the HTTPS connector with rustls is set up correctly
        drop(transport);
    }

    #[test]
    fn test_new_with_connector() {
        use hyper_util::client::legacy::connect::HttpConnector;

        let connector = HttpConnector::new();
        let transport = HyperTransport::new_with_connector(connector);
        // Verify we can build with a custom connector
        drop(transport);
    }
}
