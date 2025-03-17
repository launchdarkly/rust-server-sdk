use hyper::client::HttpConnector;
use hyper_rustls::builderstates::WantsSchemes;
use hyper_rustls::HttpsConnector;
use hyper_rustls::HttpsConnectorBuilder;

// Creates an HTTPS connector for secure HTTP requests.
//
// This function configures and returns a connector that provides HTTPS capabilities to
// HTTP client implementations.
//
// # Features
//
// By default, this function uses the system's native certificate store for certificate
// verification. However, if the `webpki-roots` feature is enabled, it will use the
// WebPKI library instead. This is useful in environments where the system's certificate
// store is not available or not reliable.
//
pub fn create_https_connector() -> HttpsConnector<HttpConnector> {
    builder()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build()
}

#[cfg(feature = "webpki-roots")]
fn builder() -> HttpsConnectorBuilder<WantsSchemes> {
    HttpsConnectorBuilder::new().with_webpki_roots()
}

#[cfg(not(feature = "webpki-roots"))]
fn builder() -> HttpsConnectorBuilder<WantsSchemes> {
    HttpsConnectorBuilder::new().with_native_roots()
}
