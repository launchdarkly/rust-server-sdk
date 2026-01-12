//! HTTP transport abstraction for LaunchDarkly SDK
//!
//! This module defines the [`HttpTransport`] trait which allows users to plug in
//! their own HTTP client implementation (hyper, reqwest, or custom).

use bytes::Bytes;
use futures::Stream;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

// Re-export http crate types for convenience
pub use http::{Request, Response};

/// A pinned, boxed stream of bytes returned by HTTP transports.
///
/// This represents the streaming response body from an HTTP request.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, TransportError>> + Send>>;

/// A pinned, boxed future for an HTTP response.
///
/// This represents the future returned by [`HttpTransport::request`].
pub type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response<ByteStream>, TransportError>> + Send>>;

/// Error type for HTTP transport operations.
///
/// This wraps transport-specific errors (network failures, timeouts, etc.) in a
/// common error type that the SDK can handle uniformly.
#[derive(Debug)]
pub struct TransportError {
    inner: Box<dyn StdError + Send + Sync + 'static>,
}

impl TransportError {
    /// Create a new transport error from any error type.
    pub fn new(err: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            inner: Box::new(err),
        }
    }

    /// Get a reference to the inner error.
    pub fn inner(&self) -> &(dyn StdError + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transport error: {}", self.inner)
    }
}

impl StdError for TransportError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.inner)
    }
}

/// Trait for pluggable HTTP transport implementations.
///
/// Implement this trait to provide HTTP request/response functionality for the
/// SDK. The transport is responsible for:
/// - Establishing HTTP connections (with TLS if needed)
/// - Sending HTTP requests
/// - Returning streaming HTTP responses
/// - Handling timeouts (if desired)
///
/// The SDK normally uses [`crate::HyperTransport`] as the default implementation,
/// but you can provide your own implementation for custom requirements such as:
/// - Using a different HTTP client library (reqwest, custom, etc.)
/// - Adding request/response logging or metrics
/// - Implementing custom retry logic
/// - Using a proxy or custom TLS configuration
///
/// # Example
///
/// ```no_run
/// use launchdarkly_server_sdk::{HttpTransport, ResponseFuture, TransportError};
/// use bytes::Bytes;
/// use http::{Request, Response};
///
/// #[derive(Clone)]
/// struct LoggingTransport<T: HttpTransport> {
///     inner: T,
/// }
///
/// impl<T: HttpTransport> HttpTransport for LoggingTransport<T> {
///     fn request(&self, request: Request<Bytes>) -> ResponseFuture {
///         println!("Making request to: {}", request.uri());
///         self.inner.request(request)
///     }
/// }
/// ```
pub trait HttpTransport: Clone + Send + Sync + 'static {
    /// Execute an HTTP request and return a streaming response.
    ///
    /// # Arguments
    ///
    /// * `request` - The HTTP request to execute. The body type is `Bytes`
    ///   to support both binary content and empty bodies. Use `Bytes::new()`
    ///   for requests with no body (e.g., GET requests).
    ///
    /// # Returns
    ///
    /// A future that resolves to an HTTP response with a streaming body, or a
    /// transport error if the request fails.
    ///
    /// The response includes:
    /// - Status code
    /// - Response headers
    /// - A stream of body bytes
    ///
    /// # Notes
    ///
    /// - The transport should NOT follow redirects - the SDK handles this when needed
    /// - The transport should NOT retry requests - the SDK handles this
    /// - The transport MAY implement timeouts as desired
    fn request(&self, request: Request<Bytes>) -> ResponseFuture;
}
