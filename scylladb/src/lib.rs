use futures::StreamExt;
mod types;

pub use crate::types::{FastData, UNIVERSAL_SUFFIX};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

use crate::types::FastDataRow;
use fastnear_primitives::types::ChainId;
use futures::Stream;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};
use std::env;
use std::sync::Arc;

const SCYLLADB: &str = "scylladb";

pub struct ScyllaDb {
    insert_fastdata_query: PreparedStatement,
    select_fastdata_query_by_suffix_from: PreparedStatement,
    insert_last_processed_block_height_query: PreparedStatement,
    select_last_processed_block_height_query: PreparedStatement,

    scylla_session: Session,
}

pub fn create_rustls_client_config() -> Arc<ClientConfig> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install default provider");
    }
    let ca_cert_path =
        env::var("SCYLLA_SSL_CA").expect("SCYLLA_SSL_CA environment variable not set");
    let client_cert_path =
        env::var("SCYLLA_SSL_CERT").expect("SCYLLA_SSL_CERT environment variable not set");
    let client_key_path =
        env::var("SCYLLA_SSL_KEY").expect("SCYLLA_SSL_KEY environment variable not set");

    let ca_certs = rustls::pki_types::CertificateDer::from_pem_file(ca_cert_path)
        .expect("Failed to load CA certs");
    let client_certs = rustls::pki_types::CertificateDer::from_pem_file(client_cert_path)
        .expect("Failed to load client certs");
    let client_key = rustls::pki_types::PrivateKeyDer::from_pem_file(client_key_path)
        .expect("Failed to load client key");

    let mut root_store = RootCertStore::empty();
    root_store.add(ca_certs).expect("Failed to add CA certs");

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![client_certs], client_key)
        .expect("Failed to create client config");

    Arc::new(config)
}

impl ScyllaDb {
    pub async fn new_scylla_session() -> anyhow::Result<Session> {
        let scylla_url = env::var("SCYLLA_URL").expect("SCYLLA_DB_URL must be set");
        let scylla_username = env::var("SCYLLA_USERNAME").expect("SCYLLA_USERNAME must be set");
        let scylla_password = env::var("SCYLLA_PASSWORD").expect("SCYLLA_PASSWORD must be set");

        let session: Session = SessionBuilder::new()
            .known_node(scylla_url)
            .tls_context(Some(create_rustls_client_config()))
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
        // Self::create_keyspace(chain_id, &scylla_session).await?;
        scylla_session
            .use_keyspace(format!("fastdata_{chain_id}"), false)
            .await?;
        Self::create_tables(&scylla_session).await?;

        Ok(Self {
            insert_fastdata_query: Self::prepare_query(
                &scylla_session,
                "INSERT INTO blobs (receipt_id, action_index, suffix, data, tx_hash, signer_id, predecessor_id, current_account_id, block_height, block_timestamp, shard_id, receipt_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                scylla::frame::types::Consistency::LocalQuorum,
            )
            .await?,
            select_fastdata_query_by_suffix_from: Self::prepare_query(
                &scylla_session,
                "SELECT * FROM blobs WHERE suffix = ? AND block_height >= ? AND block_height <= ?",
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
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

    #[allow(unused)]
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
                receipt_id text,
                action_index int,
                suffix text,
                data blob,
                tx_hash text,
                signer_id text,
                predecessor_id text,
                current_account_id text,
                block_height bigint,
                block_timestamp bigint,
                shard_id int,
                receipt_index int,
                PRIMARY KEY ((suffix), block_height, shard_id, receipt_index, action_index, receipt_id)
            )",
            "CREATE INDEX IF NOT EXISTS idx_tx_hash ON blobs (tx_hash)",
            "CREATE INDEX IF NOT EXISTS idx_receipt_id ON blobs (receipt_id)",
            "CREATE TABLE IF NOT EXISTS meta (
                suffix text PRIMARY KEY,
                last_processed_block_height bigint
            )",
        ];
        for query in queries.iter() {
            tracing::debug!(target: SCYLLADB, "Creating table: {}", query);
            scylla_session.query_unpaged(*query, &[]).await?;
        }
        Ok(())
    }

    pub async fn add_data(&self, fastdata: FastData) -> anyhow::Result<()> {
        self.scylla_session
            .execute_unpaged(&self.insert_fastdata_query, FastDataRow::from(fastdata))
            .await?;
        Ok(())
    }

    /// Fetches all fast data for a given suffix and block height range (inclusive both ends).
    pub async fn get_suffix_data(
        &self,
        suffix: &str,
        from_block_height: u64,
        to_block_height: u64,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<FastData>>> {
        let rows_stream = self
            .scylla_session
            .execute_iter(
                self.select_fastdata_query_by_suffix_from.clone(),
                (
                    suffix.to_string(),
                    from_block_height as i64,
                    to_block_height as i64,
                ),
            )
            .await?
            .rows_stream::<FastDataRow>()?;
        // Making an iterator from the stream
        Ok(rows_stream.map(|row| {
            row.map(|row| row.into())
                .map_err(|e| anyhow::anyhow!("Failed to parse row: {:?}", e))
        }))
    }

    pub async fn set_last_processed_block_height(
        &self,
        suffix: &str,
        last_processed_block_height: u64,
    ) -> anyhow::Result<()> {
        self.scylla_session
            .execute_unpaged(
                &self.insert_last_processed_block_height_query,
                (suffix.to_string(), last_processed_block_height as i64),
            )
            .await?;
        Ok(())
    }

    pub async fn get_last_processed_block_height(
        &self,
        suffix: &str,
    ) -> anyhow::Result<Option<u64>> {
        let rows = self
            .scylla_session
            .execute_unpaged(
                &self.select_last_processed_block_height_query,
                (suffix.to_string(),),
            )
            .await?
            .into_rows_result()?;
        Ok(rows.single_row::<(i64,)>().ok().map(|(v,)| v as _))
    }
}
