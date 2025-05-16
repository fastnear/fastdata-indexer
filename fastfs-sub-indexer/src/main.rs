mod fastfs;
mod scylla_types;

use crate::fastfs::FastfsData;
use crate::scylla_types::{
    add_fastfs_fastdata, create_tables, prepare_insert_query, FastfsFastData,
};
use dotenv::dotenv;
use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::types::ChainId;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use suffix_fetcher::{SuffixFetcher, SuffixFetcherConfig, SuffixFetcherUpdate};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "fastfs-sub-indexer";
const SUFFIX: &str = "fastfs";

#[tokio::main]
async fn main() {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("fastfs-sub-indexer=info,scylladb=info,suffix-fetcher=info")
        .init();

    let chain_id: ChainId = env::var("CHAIN_ID")
        .expect("CHAIN_ID required")
        .try_into()
        .expect("Invalid chain id");

    let fetcher = SuffixFetcher::new(chain_id, None)
        .await
        .expect("Can't create suffix fetcher");

    let scylladb = fetcher.get_scylladb();

    create_tables(&scylladb)
        .await
        .expect("Error creating tables");

    let insert_query = prepare_insert_query(&scylladb)
        .await
        .expect("Error preparing insert query");

    let last_processed_block_height = scylladb
        .get_last_processed_block_height(SUFFIX)
        .await
        .expect("Error getting last processed block height");

    let start_block_height: BlockHeight = last_processed_block_height
        .map(|h| h + 1)
        .unwrap_or_else(|| {
            env::var("START_BLOCK_HEIGHT")
                .ok()
                .map(|start_block_height| start_block_height.parse().expect("Invalid block height"))
                .unwrap_or(0)
        });

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();

    ctrlc::set_handler(move || {
        ctrl_c_running.store(false, Ordering::SeqCst);
        tracing::info!(target: PROJECT_ID, "Received Ctrl+C, starting shutdown...");
    })
    .expect("Error setting Ctrl+C handler");

    tracing::info!(target: PROJECT_ID,
        "Starting {:?} {} fetcher from height {}",
        SUFFIX,
        chain_id,
        start_block_height,
    );

    let (sender, mut receiver) = mpsc::channel(100);
    tokio::spawn(fetcher.start(
        SuffixFetcherConfig {
            suffix: SUFFIX.to_string(),
            start_block_height: Some(start_block_height),
            sleep_duration: Duration::from_millis(500),
        },
        sender,
        is_running.clone(),
    ));

    while let Some(update) = receiver.recv().await {
        match update {
            SuffixFetcherUpdate::FastData(fastdata) => {
                tracing::info!(target: PROJECT_ID, "Received fastdata: {} {} {}", fastdata.block_height, fastdata.receipt_id, fastdata.action_index);
                if let Ok(FastfsData::Simple(simple_fastfs)) = borsh::from_slice(&fastdata.data) {
                    if simple_fastfs.is_valid() {
                        let (mime_type, content) = simple_fastfs
                            .content
                            .map(|c| (Some(c.mime_type), Some(c.content)))
                            .unwrap_or((None, None));
                        let fastfs_fastdata = FastfsFastData {
                            receipt_id: fastdata.receipt_id,
                            action_index: fastdata.action_index,
                            tx_hash: fastdata.tx_hash,
                            signer_id: fastdata.signer_id,
                            predecessor_id: fastdata.predecessor_id,
                            current_account_id: fastdata.current_account_id,
                            block_height: fastdata.block_height,
                            block_timestamp: fastdata.block_timestamp,
                            shard_id: fastdata.shard_id,
                            receipt_index: fastdata.receipt_index,
                            mime_type,
                            relative_path: simple_fastfs.relative_path,
                            content,
                        };
                        tracing::info!(target: PROJECT_ID, "FastFS data {} bytes: {}/{}/{}", fastfs_fastdata.content.as_ref().map(|v| v.len()).unwrap_or(0), fastfs_fastdata.predecessor_id, fastfs_fastdata.current_account_id, fastfs_fastdata.relative_path);
                        add_fastfs_fastdata(&scylladb, &insert_query, fastfs_fastdata)
                            .await
                            .expect("Error adding FastFS data to ScyllaDB");
                    }
                }
            }
            SuffixFetcherUpdate::EndOfRange(block_height) => {
                tracing::info!(target: PROJECT_ID, "Saving last processed block height: {}", block_height);
                scylladb
                    .set_last_processed_block_height(SUFFIX, block_height)
                    .await
                    .expect("Error setting last processed block height");
                if !is_running.load(Ordering::SeqCst) {
                    tracing::info!(target: PROJECT_ID, "Shutting down...");
                    break;
                }
            }
        };
    }

    tracing::info!(target: PROJECT_ID, "Successfully shut down");
}
