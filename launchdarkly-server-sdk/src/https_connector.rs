use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
#[cfg(feature = "rustls")]
use hyper_rustls::HttpsConnectorBuilder;

/// Creates an HTTPS connector for secure HTTP requests.
///
/// This function configures and returns a connector that provides HTTPS capabilities to
/// HTTP client implementations.
///
/// # Features
///
/// This function has different implementations based on crate features:
///
/// - When the `webpki-roots` feature is enabled, it uses WebPKI roots for certificate verification.
///
/// - When `webpki-roots` is not enabled, it uses the system's native certificate store.
///
/// # Returns
///
/// Returns an `HttpsConnector<HttpConnector>` configured for secure HTTP connections.
///
/// # Examples
///
/// ```
/// use launchdarkly_server_sdk::https_connector::create_https_connector;
///
/// let connector = create_https_connector();
/// let client = hyper::Client::builder().build::<_, hyper::Body>(connector);
/// ```
#[cfg(feature = "webpki-roots")]
pub fn create_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build()
}

/// Creates an HTTPS connector for secure HTTP requests.
///
/// This function configures and returns a connector that provides HTTPS capabilities to
/// HTTP client implementations.
///
/// # Features
///
/// This function has different implementations based on crate features:
///
/// - When the `webpki-roots` feature is enabled, it uses WebPKI roots for certificate verification.
///
/// - When `webpki-roots` is not enabled, it uses the system's native certificate store.
///
/// # Returns
///
/// Returns an `HttpsConnector<HttpConnector>` configured for secure HTTP connections.
///
/// # Examples
///
/// ```
/// use launchdarkly_server_sdk::https_connector::create_https_connector;
///
/// let connector = create_https_connector();
/// let client = hyper::Client::builder().build::<_, hyper::Body>(connector);
/// ```
#[cfg(not(feature = "webpki-roots"))]
pub fn create_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build()
}
