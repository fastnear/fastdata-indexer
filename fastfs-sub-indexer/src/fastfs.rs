use borsh::{BorshDeserialize, BorshSerialize};

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

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct FastfsFileContent {
    pub mime_type: String,
    pub content: Vec<u8>,
}

impl SimpleFastfs {
    pub fn is_valid(&self) -> bool {
        if self.relative_path.len() > MAX_RELATIVE_PATH_LENGTH {
            return false;
        }
        if let Some(content) = &self.content {
            if content.mime_type.is_empty() {
                return false;
            }
        }
        true
    }
}
