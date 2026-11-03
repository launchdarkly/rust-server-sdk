use crate::{HttpsConnector, TlsParameters};

/// Returns the connector for a new client. Without TLS options the shared connector is used,
/// which verifies peers against the default trust roots.
pub fn connector(
    tls: Option<&TlsParameters>,
    shared: &HttpsConnector,
) -> Result<HttpsConnector, String> {
    match tls {
        Some(tls) if tls.skip_verify_peer => imp::skip_verify_peer(),
        Some(TlsParameters {
            custom_ca_file: Some(file),
            ..
        }) if !file.is_empty() => imp::custom_ca(file),
        _ => Ok(shared.clone()),
    }
}

#[cfg(any(
    feature = "hyper-rustls-native-roots",
    feature = "hyper-rustls-webpki-roots"
))]
mod imp {
    use crate::HttpsConnector;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
    use std::sync::Arc;

    /// Accepts any server certificate. Handshake signatures are still checked against the
    /// certificate the server presented.
    #[derive(Debug)]
    struct AcceptAnyCertificate(Arc<CryptoProvider>);

    impl ServerCertVerifier for AcceptAnyCertificate {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    pub fn skip_verify_peer() -> Result<HttpsConnector, String> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = Arc::new(AcceptAnyCertificate(provider.clone()));
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        Ok(build(config))
    }

    /// Trusts only the certificates in the given PEM file.
    pub fn custom_ca(file: &str) -> Result<HttpsConnector, String> {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(file).map_err(|e| e.to_string())? {
            roots
                .add(cert.map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(build(config))
    }

    fn build(config: ClientConfig) -> HttpsConnector {
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build()
    }
}

#[cfg(feature = "native-tls")]
mod imp {
    use crate::HttpsConnector;
    use hyper_util::client::legacy::connect::HttpConnector;
    use tokio_native_tls::native_tls::{Certificate, TlsConnector, TlsConnectorBuilder};

    pub fn skip_verify_peer() -> Result<HttpsConnector, String> {
        let mut builder = TlsConnector::builder();
        builder.danger_accept_invalid_certs(true);
        build(builder)
    }

    /// Trusts only the certificate in the given PEM file.
    pub fn custom_ca(file: &str) -> Result<HttpsConnector, String> {
        let pem = std::fs::read(file).map_err(|e| e.to_string())?;
        let cert = Certificate::from_pem(&pem).map_err(|e| e.to_string())?;
        let mut builder = TlsConnector::builder();
        builder
            .disable_built_in_roots(true)
            .add_root_certificate(cert);
        build(builder)
    }

    fn build(builder: TlsConnectorBuilder) -> Result<HttpsConnector, String> {
        let tls = builder.build().map_err(|e| e.to_string())?;
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        Ok(HttpsConnector::from((http, tls.into())))
    }
}

#[cfg(not(any(
    feature = "hyper-rustls-native-roots",
    feature = "hyper-rustls-webpki-roots",
    feature = "native-tls"
)))]
mod imp {
    use crate::HttpsConnector;

    // Builds without a TLS stack do not declare any tls capability.
    pub fn skip_verify_peer() -> Result<HttpsConnector, String> {
        Err("this build has no TLS support".to_string())
    }

    pub fn custom_ca(_file: &str) -> Result<HttpsConnector, String> {
        Err("this build has no TLS support".to_string())
    }
}
