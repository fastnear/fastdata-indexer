use crate::*;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use scylla::statement::prepared::PreparedStatement;
use scylla::{DeserializeRow, SerializeRow};
use scylladb::{ScyllaDb, SCYLLADB};

#[derive(Debug, Clone)]
pub struct FastfsFastData {
    pub receipt_id: CryptoHash,
    pub action_index: u32,
    pub tx_hash: Option<CryptoHash>,
    pub signer_id: AccountId,
    pub predecessor_id: AccountId,
    pub current_account_id: AccountId,
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    pub shard_id: u32,
    pub receipt_index: u32,
    pub mime_type: Option<String>,
    pub relative_path: String,
    pub content: Option<Vec<u8>>,
    pub offset: u32,
    pub full_size: u32,
    pub nonce: u32,
}

#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub(crate) struct FastfsFastDataRow {
    pub receipt_id: String,
    pub action_index: i32,
    pub tx_hash: Option<String>,
    pub signer_id: String,
    pub predecessor_id: String,
    pub current_account_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub shard_id: i32,
    pub receipt_index: i32,
    pub mime_type: Option<String>,
    pub relative_path: String,
    pub content: Option<Vec<u8>>,
    pub offset: i32,
    pub full_size: i32,
    pub nonce: i32,
}

impl From<FastfsFastDataRow> for FastfsFastData {
    fn from(row: FastfsFastDataRow) -> Self {
        Self {
            receipt_id: row.receipt_id.parse().unwrap(),
            action_index: row.action_index as u32,
            tx_hash: row.tx_hash.map(|h| h.parse().unwrap()),
            signer_id: row.signer_id.parse().unwrap(),
            predecessor_id: row.predecessor_id.parse().unwrap(),
            current_account_id: row.current_account_id.parse().unwrap(),
            block_height: row.block_height as u64,
            block_timestamp: row.block_timestamp as u64,
            shard_id: row.shard_id as u32,
            receipt_index: row.receipt_index as u32,
            mime_type: row.mime_type,
            relative_path: row.relative_path,
            content: row.content,
            offset: row.offset as u32,
            full_size: row.full_size as u32,
            nonce: row.nonce as u32,
        }
    }
}

impl From<FastfsFastData> for FastfsFastDataRow {
    fn from(data: FastfsFastData) -> Self {
        Self {
            receipt_id: data.receipt_id.to_string(),
            action_index: data.action_index as i32,
            tx_hash: data.tx_hash.map(|h| h.to_string()),
            signer_id: data.signer_id.to_string(),
            predecessor_id: data.predecessor_id.to_string(),
            current_account_id: data.current_account_id.to_string(),
            block_height: data.block_height as i64,
            block_timestamp: data.block_timestamp as i64,
            shard_id: data.shard_id as i32,
            receipt_index: data.receipt_index as i32,
            mime_type: data.mime_type,
            relative_path: data.relative_path,
            content: data.content,
            offset: data.offset as i32,
            full_size: data.full_size as i32,
            nonce: data.nonce as i32,
        }
    }
}

pub(crate) async fn create_tables(scylla_db: &ScyllaDb) -> anyhow::Result<()> {
    let queries = [
        "CREATE TABLE IF NOT EXISTS s_fastfs_v2 (
            receipt_id text,
            action_index int,
            tx_hash text,
            signer_id text,
            predecessor_id text,
            current_account_id text,
            block_height bigint,
            block_timestamp bigint,
            shard_id int,
            receipt_index int,
            mime_type text,
            relative_path text,
            content blob,
            offset int,
            full_size int,
            nonce int,
            PRIMARY KEY ((predecessor_id), current_account_id, relative_path, offset)
        )",
        "CREATE INDEX IF NOT EXISTS idx_s_fastfs_v2_tx_hash ON s_fastfs_v2 (tx_hash)",
        "CREATE INDEX IF NOT EXISTS idx_s_fastfs_v2_receipt_id ON s_fastfs_v2 (receipt_id)",
    ];
    for query in queries.iter() {
        tracing::debug!(target: SCYLLADB, "Creating table: {}", query);
        scylla_db.scylla_session.query_unpaged(*query, &[]).await?;
    }
    Ok(())
}

pub(crate) async fn prepare_insert_query(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
        "INSERT INTO s_fastfs_v2 (receipt_id, action_index, tx_hash, signer_id, predecessor_id, current_account_id, block_height, block_timestamp, shard_id, receipt_index, mime_type, relative_path, content, offset, full_size, nonce) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        scylla::frame::types::Consistency::LocalQuorum,
    )
        .await
}

pub(crate) async fn add_fastfs_fastdata(
    scylla_db: &ScyllaDb,
    insert_query: &PreparedStatement,
    fastdata: FastfsFastData,
) -> anyhow::Result<()> {
    scylla_db
        .scylla_session
        .execute_unpaged(insert_query, FastfsFastDataRow::from(fastdata))
        .await?;
    Ok(())
}
