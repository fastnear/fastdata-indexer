use crate::*;

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
