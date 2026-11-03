use crate::FastData;

pub fn compute_order_id(fd: &FastData) -> u64 {
    ((fd.shard_id as u64) * 100000 + fd.receipt_index as u64) * 1000 + fd.action_index as u64
}
