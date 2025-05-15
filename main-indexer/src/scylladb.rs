use crate::*;
use num_traits::cast::ToPrimitive;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

use anyhow::anyhow;
use rustls::pki_types::{CertificateDer, PrivateKeyDer}; // Using types re-exported by rustls
use rustls::{ClientConfig, RootCertStore};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::{env, io};

pub struct ScyllaDb {
    chain_id: ChainId,

    insert_blob_query: PreparedStatement,
    insert_last_processed_block_height_query: PreparedStatement,
    select_last_processed_block_height_query: PreparedStatement,

    scylla_session: Session,
}

// Helper function to load certificates from a PEM file path
// (assuming rustls_pemfile::certs returns an iterator as per your information)
fn load_certs_from_path(path: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file =
        File::open(path).map_err(|e| anyhow!("Failed to open certificate file {}: {}", path, e))?;
    let mut reader = BufReader::new(file);

    // If rustls_pemfile::certs returns an iterator of Results:
    let certs_iter = rustls_pemfile::certs(&mut reader);

    // Collect the results from the iterator.
    // This will iterate through, and if any item is an Err, collect will return that Err.
    // If all items are Ok, it will collect them into a Vec.
    let certs: Vec<CertificateDer<'static>> = certs_iter
        .collect::<Result<Vec<_>, io::Error>>() // Collect into Result<Vec<CertificateDer>, io::Error>
        .map_err(|e| anyhow!("Failed to parse a certificate from {}: {}", path, e))?; // Map the io::Error to anyhow::Error

    if certs.is_empty() {
        anyhow::bail!("No PEM certificates found in {}", path);
    }
    Ok(certs)
}

// Helper function to load a private key from a PEM file path
fn load_private_key_from_path(path: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file =
        File::open(path).map_err(|e| anyhow!("Failed to open private key file {}: {}", path, e))?;
    let mut reader = BufReader::new(file);

    // rustls_pemfile::private_key attempts to parse the first valid PEM-encoded
    // PKCS#1, PKCS#8, or SEC1 private key.
    match rustls_pemfile::private_key(&mut reader)
        .map_err(|e| anyhow!("Failed to read private key from {}: {}", path, e))?
    {
        Some(key) => Ok(key),
        None => anyhow::bail!("No private key found in PEM format in {}", path),
    }
}

pub async fn create_rustls_client_config() -> anyhow::Result<Arc<ClientConfig>> {
    // 1. Read certificate paths from environment variables
    let ca_cert_path = env::var("SCYLLA_SSL_CA")
        .map_err(|e| anyhow!("SCYLLA_SSL_CA environment variable not set: {}", e))?;
    let client_cert_path = env::var("SCYLLA_SSL_CERT")
        .map_err(|e| anyhow!("SCYLLA_SSL_CERT environment variable not set: {}", e))?;
    let client_key_path = env::var("SCYLLA_SSL_KEY")
        .map_err(|e| anyhow!("SCYLLA_SSL_KEY environment variable not set: {}", e))?;

    // 2. Load CA certificates and populate the RootCertStore
    let ca_certs = load_certs_from_path(&ca_cert_path)?;
    let mut root_store = RootCertStore::empty();
    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|e| anyhow!("Failed to add CA certificate to root store: {}", e))?;
    }
    if root_store.is_empty() {
        // Should be caught by load_certs_from_path, but good to double check
        anyhow::bail!(
            "No CA certificates successfully loaded from {}",
            ca_cert_path
        );
    }

    // 3. Load client certificate chain
    let client_certs = load_certs_from_path(&client_cert_path)?;

    // 4. Load client private key
    let client_key = load_private_key_from_path(&client_key_path)?;

    // 5. Build the ClientConfig
    // ClientConfig::builder() by default (with 'std' feature) uses the default crypto provider
    // and supports safe TLS protocol versions.
    // .with_safe_defaults() explicitly sets up safe ciphersuites and protocol versions.
    // .with_root_certificates() sets the CAs to trust for server verification.
    // .with_client_auth_cert() provides the client's certificate and key for mutual TLS.
    // This call also implicitly verifies that the key matches the certificate.
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(client_certs, client_key)
        .map_err(|e| anyhow!("Failed to build rustls client config: {}", e))?;

    Ok(Arc::new(config))
}

impl ScyllaDb {
    // async fn create_ssl_context() -> anyhow::Result<openssl::ssl::SslContext> {
    //     // Initialize SslContextBuilder with TLS method
    //     let ca_cert_path = std::env::var("SCYLLA_SSL_CA")?;
    //     let client_cert_path = std::env::var("SCYLLA_SSL_CERT")?;
    //     let client_key_path = std::env::var("SCYLLA_SSL_KEY")?;
    //
    //     let mut builder = openssl::ssl::SslContextBuilder::new(openssl::ssl::SslMethod::tls())?;
    //     builder.set_ca_file(ca_cert_path)?;
    //     builder.set_certificate_file(client_cert_path, openssl::ssl::SslFiletype::PEM)?;
    //     builder.set_private_key_file(client_key_path, openssl::ssl::SslFiletype::PEM)?;
    //     builder.set_verify(openssl::ssl::SslVerifyMode::PEER);
    //     builder.check_private_key()?;
    //     Ok(builder.build())
    // }

    pub async fn new_scylla_session() -> anyhow::Result<Session> {
        let scylla_url = env::var("SCYLLA_URL").expect("SCYLLA_DB_URL must be set");
        let scylla_username = env::var("SCYLLA_USERNAME").expect("SCYLLA_USERNAME must be set");
        let scylla_password = env::var("SCYLLA_PASSWORD").expect("SCYLLA_PASSWORD must be set");

        let ssl_context = create_rustls_client_config().await?;

        let session: Session = SessionBuilder::new()
            .known_node(scylla_url)
            .tls_context(Some(ssl_context))
            .authenticator_provider(Arc::new(
                scylla::authentication::PlainTextAuthenticator::new(
                    scylla_username,
                    scylla_password,
                ),
            ))
            .build()
            .await?;

        Ok(session)
    }

    pub async fn test_connection(scylla_session: &Session) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await?;
        Ok(())
    }

    pub async fn new(chain_id: ChainId, scylla_session: Session) -> anyhow::Result<Self> {
        Self::create_keyspace(chain_id, &scylla_session).await?;
        scylla_session
            .use_keyspace(format!("fastdata_{chain_id}"), false)
            .await?;
        Self::create_tables(&scylla_session).await?;

        Ok(Self {
            chain_id,
            insert_blob_query: Self::prepare_query(
                &scylla_session,
                "INSERT INTO blobs (receipt_id, suffix, data, tx_hash, block_height, block_timestamp, shard_id, shard_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                scylla::frame::types::Consistency::LocalQuorum,
            )
            .await?,
            insert_last_processed_block_height_query: Self::prepare_query(
                &scylla_session,
                "INSERT INTO meta (suffix, last_processed_block_height) VALUES (?, ?)",
                scylla::frame::types::Consistency::LocalQuorum,
            ).await?,
            select_last_processed_block_height_query: Self::prepare_query(
                &scylla_session,
                "SELECT last_processed_block_height FROM meta WHERE suffix = ? LIMIT 1",
                scylla::frame::types::Consistency::LocalOne,
            )
            .await?,
            scylla_session,
        })
    }

    async fn prepare_query(
        scylla_db_session: &Session,
        query_text: &str,
        consistency: scylla::frame::types::Consistency,
    ) -> anyhow::Result<PreparedStatement> {
        let mut query = scylla::statement::Statement::new(query_text);
        query.set_consistency(consistency);
        Ok(scylla_db_session.prepare(query).await?)
    }

    pub async fn create_keyspace(
        chain_id: ChainId,
        scylla_session: &Session,
    ) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS fastdata_{chain_id}
                    WITH REPLICATION = {{
                        'class': 'NetworkTopologyStrategy',
                        'dc1': 3
                    }} AND TABLETS = {{'enabled': true}};"
                ),
                &[],
            )
            .await?;
        Ok(())
    }

    pub async fn create_tables(scylla_session: &Session) -> anyhow::Result<()> {
        let queries = [
            "CREATE TABLE IF NOT EXISTS blobs (
                receipt_id text PRIMARY KEY,
                suffix text NOT NULL,
                data blob NOT NULL,
                tx_hash text,
                block_height bigint NOT NULL,
                block_timestamp timestamp NOT NULL,
                shard_id int NOT NULL,
                shard_index int NOT NULL
            )",
            "CREATE INDEX IF NOT EXISTS idx_tx_hash ON blobs (tx_hash)",
            "CREATE INDEX IF NOT EXISTS idx_suffix_block_shard ON blobs ((suffix), block_height, shard_id, shard_index)",
            "CREATE INDEX IF NOT EXISTS idx_suffix_time_shard ON blobs ((suffix), timestamp, shard_id, shard_index)",
            "CREATE INDEX IF NOT EXISTS idx_block_shard ON blobs ((block_height), shard_id, shard_index)",
            "CREATE TABLE IF NOT EXISTS meta (
                suffix text primary key,
                last_processed_block_height bigint NOT NULL,
            )",
        ];
        for query in queries.iter() {
            scylla_session.query_unpaged(*query, &[]).await?;
        }
        Ok(())
    }

    pub async fn add_data(
        &self,
        receipt_id: CryptoHash,
        suffix: &str,
        data: Vec<u8>,
        tx_hash: Option<CryptoHash>,
        block_height: u64,
        block_timestamp_ns: u64,
        shard_id: u32,
        shard_index: u32,
    ) -> anyhow::Result<()> {
        self.scylla_session
            .execute_unpaged(
                &self.insert_blob_query,
                (
                    receipt_id.to_string(),
                    suffix.to_string(),
                    data,
                    tx_hash.map(|h| h.to_string()),
                    num_bigint::BigInt::from(block_height),
                    (block_timestamp_ns / 1_000_000) as i64,
                    shard_id as i32,
                    shard_index as i32,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn set_last_processed_block_height(
        &self,
        suffix: &str,
        last_processed_block_height: u64,
    ) -> anyhow::Result<()> {
        self.scylla_session
            .execute_unpaged(
                &self.insert_last_processed_block_height_query,
                (
                    suffix.to_string(),
                    num_bigint::BigInt::from(last_processed_block_height),
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn get_last_processed_block_height(&self, suffix: &str) -> anyhow::Result<u64> {
        let (last_processed_block_height,) = self
            .scylla_session
            .execute_unpaged(
                &self.select_last_processed_block_height_query,
                (suffix.to_string(),),
            )
            .await?
            .into_rows_result()?
            .single_row::<(num_bigint::BigInt,)>()?;
        last_processed_block_height.to_u64().ok_or(anyhow::anyhow!(
            "Failed to convert last_processed_block_height to u64"
        ))
    }
}
