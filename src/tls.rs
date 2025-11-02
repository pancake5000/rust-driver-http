use std::fs::File;
use std::io::BufReader;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls_pemfile::certs;
use anyhow::Context;
use std::sync::Arc;

/// Attempts to load TLS root certificates from `certs/ca.crt`.
/// Returns Ok(Some(Arc<ClientConfig>)) when CA is present and parsed,
/// and Err on parse errors.
pub fn load_tls_config() -> anyhow::Result<Option<Arc<ClientConfig>>> {
    println!("Loading TLS root certificates from certs/ca.crt...");

    let mut root_store = RootCertStore::empty();

    let ca_file = File::open("certs/ca.crt").context("opening certs/ca.crt")?;
    let mut ca_reader = BufReader::new(ca_file);

    let mut cert_count = 0;
    for cert in certs(&mut ca_reader) {
        let cert = cert.context("Failed to parse certificate from ca.crt")?;
        root_store.add(cert)?;
        cert_count += 1;
    }

    if cert_count == 0 {
        anyhow::bail!("No certificates found in certs/ca.crt");
    }

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Some(Arc::new(config)))
}