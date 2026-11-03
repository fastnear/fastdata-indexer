use borsh::{BorshDeserialize, BorshSerialize};

const MAX_RELATIVE_PATH_LENGTH: usize = 1024;
const MAX_MIME_TYPE_LENGTH: usize = 256;
const OFFSET_ALIGNMENT: u32 = 1 << 20; // 1Mb
const MAX_FULL_SIZE: u32 = 32 * OFFSET_ALIGNMENT; // 32Mb

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub enum FastfsData {
    Simple(Box<SimpleFastfs>),
    Partial(Box<PartialFastfs>),
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

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub struct PartialFastfs {
    pub relative_path: String,
    /// The offset has to be aligned to 1Mb (1048576 bytes).
    pub offset: u32,
    /// The full content size in bytes up to 32Mb (33554432 bytes).
    pub full_size: u32,
    pub mime_type: String,
    /// The content chunk up to 1Mb (1048576 bytes). We assume zero bytes tail if the chunk is
    /// smaller than 1Mb, but inside the file.
    pub content_chunk: Vec<u8>,
    /// A nonce to differentiate different uploads at the same relative path.
    /// The nonce should match for all parts of the same file.
    /// Max value is i32::MAX.
    /// Min value should be 1, to differentiate from simple fastfs entries.
    pub nonce: u32,
}

impl PartialFastfs {
    pub fn is_valid(&self) -> bool {
        if self.relative_path.len() > MAX_RELATIVE_PATH_LENGTH {
            return false;
        }
        if self.offset % OFFSET_ALIGNMENT != 0 || self.offset >= MAX_FULL_SIZE {
            return false;
        }
        if self.full_size == 0 || self.full_size > MAX_FULL_SIZE {
            return false;
        }
        if self.mime_type.is_empty() || self.mime_type.len() > MAX_MIME_TYPE_LENGTH {
            return false;
        }
        if self.content_chunk.len() > OFFSET_ALIGNMENT as usize {
            return false;
        }
        if self.nonce == 0 || self.nonce > i32::MAX as u32 {
            return false;
        }
        true
    }
}

impl SimpleFastfs {
    pub fn is_valid(&self) -> bool {
        if self.relative_path.len() > MAX_RELATIVE_PATH_LENGTH {
            return false;
        }
        if let Some(content) = &self.content {
            if content.mime_type.is_empty() || content.mime_type.len() > MAX_MIME_TYPE_LENGTH {
                return false;
            }
        }
        true
    }
}
