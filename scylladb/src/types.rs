use fastnear_primitives::near_indexer_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::near_indexer_primitives::CryptoHash;

use scylla::{DeserializeRow, SerializeRow};

pub const UNIVERSAL_SUFFIX: &str = "*";

#[derive(Debug)]
pub struct FastData {
    pub receipt_id: CryptoHash,
    pub action_index: u32,
    pub suffix: String,
    pub data: Vec<u8>,
    pub tx_hash: Option<CryptoHash>,
    pub signer_id: AccountId,
    pub predecessor_id: AccountId,
    pub current_account_id: AccountId,
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    pub shard_id: u32,
    pub receipt_index: u32,
}

#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub(crate) struct FastDataRow {
    pub receipt_id: String,
    pub action_index: i32,
    pub suffix: String,
    pub data: Vec<u8>,
    pub tx_hash: Option<String>,
    pub signer_id: String,
    pub predecessor_id: String,
    pub current_account_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub shard_id: i32,
    pub receipt_index: i32,
}

impl From<FastDataRow> for FastData {
    fn from(row: FastDataRow) -> Self {
        Self {
            receipt_id: row.receipt_id.parse().unwrap(),
            action_index: row.action_index as u32,
            suffix: row.suffix,
            data: row.data,
            tx_hash: row.tx_hash.map(|h| h.parse().unwrap()),
            signer_id: row.signer_id.parse().unwrap(),
            predecessor_id: row.predecessor_id.parse().unwrap(),
            current_account_id: row.current_account_id.parse().unwrap(),
            block_height: row.block_height as u64,
            block_timestamp: row.block_timestamp as u64,
            shard_id: row.shard_id as u32,
            receipt_index: row.receipt_index as u32,
        }
    }
}

impl From<FastData> for FastDataRow {
    fn from(fastdata: FastData) -> Self {
        Self {
            receipt_id: fastdata.receipt_id.to_string(),
            action_index: fastdata.action_index as i32,
            suffix: fastdata.suffix,
            data: fastdata.data,
            tx_hash: fastdata.tx_hash.map(|h| h.to_string()),
            signer_id: fastdata.signer_id.to_string(),
            predecessor_id: fastdata.predecessor_id.to_string(),
            current_account_id: fastdata.current_account_id.to_string(),
            block_height: fastdata.block_height as i64,
            block_timestamp: fastdata.block_timestamp as i64,
            shard_id: fastdata.shard_id as i32,
            receipt_index: fastdata.receipt_index as i32,
        }
    }
}
