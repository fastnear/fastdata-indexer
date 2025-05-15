use crate::*;

const MAX_RELATIVE_PATH_LENGTH: usize = 1024;

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub enum FastfsData {
    Simple(Box<SimpleFastfs>),
}

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub struct SimpleFastfs {
    pub relative_path: String,
    pub content: Option<FastfsFileContent>,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct FastfsFileContent {
    pub mime_type: String,
    #[serde_as(as = "Base64")]
    pub content: Vec<u8>,
}

pub fn fastfs_key(fastdata: &FastData, fastfs_data: &SimpleFastfs) -> Option<String> {
    if fastfs_data.relative_path.len() > MAX_RELATIVE_PATH_LENGTH {
        return None;
    }
    Some(format!(
        "fastfs:{}:{}:{}",
        fastdata.predecessor_id, fastdata.current_account_id, fastfs_data.relative_path
    ))
}
