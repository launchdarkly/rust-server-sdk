use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
#[cfg(feature = "rustls")]
use hyper_rustls::HttpsConnectorBuilder;
#[cfg(feature = "native-certs")]
use rustls::ClientConfig;

/// Creates an HTTPS connector for secure HTTP requests.
///
/// This function configures and returns a connector that provides HTTPS capabilities to
/// HTTP client implementations.
///
/// # Features
///
/// This function has different implementations based on crate features:
///
/// - When the `native-certs` feature is enabled, it loads certificates from the system's
///   native certificate store and configures TLS with these certificates.
///
/// - When `native-certs` is not enabled, it uses the default configuration from
///   `HttpsConnectorBuilder` with native roots.
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
#[cfg(feature = "native-certs")]
pub fn create_https_connector() -> HttpsConnector<HttpConnector> {
    // Load native certs
    let mut root_store = rustls::RootCertStore::empty();
    match rustls_native_certs::load_native_certs() {
        Ok(certs) => {
            for cert in certs {
                root_store
                    .add(&rustls::Certificate(cert.0))
                    .unwrap_or_else(|e| {
                        eprintln!("Failed to add certificate: {}", e);
                    });
            }
            println!("Added {} certificates", root_store.len());
        }
        Err(e) => {
            eprintln!("Failed to load native certificates: {}", e);
            // Fall back to an empty store, which will likely fail connections
        }
    }

    // Create TLS config
    let tls_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    // Build the HTTPS connector
    HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
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
/// - When the `native-certs` feature is enabled, it loads certificates from the system's
///   native certificate store and configures TLS with these certificates.
///
/// - When `native-certs` is not enabled, it uses the default configuration from
///   `HttpsConnectorBuilder` with native roots.
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
#[cfg(not(feature = "native-certs"))]
pub fn create_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build()
}
