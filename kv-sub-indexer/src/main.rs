mod scylla_types;

use crate::scylla_types::{add_kv_rows, FastDataKv, INDEXER_ID, SUFFIX};
use dotenv::dotenv;
use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::types::ChainId;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use suffix_fetcher::{SuffixFetcher, SuffixFetcherConfig, SuffixFetcherUpdate};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "kv-sub-indexer";
const MAX_NUM_KEYS: usize = 256;
const MAX_KEY_LENGTH: usize = 1024;
const MAX_VALUE_LENGTH: usize = 262144;

#[tokio::main]
async fn main() {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("kv-sub-indexer=info,scylladb=info,suffix-fetcher=info")
        .init();

    let chain_id: ChainId = env::var("CHAIN_ID")
        .expect("CHAIN_ID required")
        .try_into()
        .expect("Invalid chain id");

    let fetcher = SuffixFetcher::new(chain_id, None)
        .await
        .expect("Can't create suffix fetcher");

    let scylladb = fetcher.get_scylladb();

    scylla_types::create_tables(&scylladb)
        .await
        .expect("Error creating tables");

    let kv_insert_query = scylla_types::prepare_kv_insert_query(&scylladb)
        .await
        .expect("Error preparing kv insert query");

    let kv_last_insert_query = scylla_types::prepare_kv_last_insert_query(&scylladb)
        .await
        .expect("Error preparing kv insert query");

    let last_processed_block_height = scylladb
        .get_last_processed_block_height(INDEXER_ID)
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

    let mut rows = vec![];
    while let Some(update) = receiver.recv().await {
        match update {
            SuffixFetcherUpdate::FastData(fastdata) => {
                tracing::info!(target: PROJECT_ID, "Received fastdata: {} {} {}", fastdata.block_height, fastdata.receipt_id, fastdata.action_index);
                if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&fastdata.data)
                {
                    if let Some(json_object) = json_value.as_object() {
                        if json_object.len() > MAX_NUM_KEYS {
                            tracing::warn!(target: PROJECT_ID, "Received Key-Value Fastdata with too many keys: {}", json_object.len());
                            continue;
                        }
                        for (key, value) in json_object {
                            if key.len() > MAX_KEY_LENGTH {
                                tracing::warn!(target: PROJECT_ID, "Received Key-Value Fastdata with invalid key: {}", key.len());
                                continue;
                            }
                            let serialized_value = serde_json::to_string(value)
                                .expect("Error serializing value in Key-Value Fastdata");
                            if serialized_value.len() > MAX_VALUE_LENGTH {
                                tracing::warn!(target: PROJECT_ID, "Skipping Key-Value with oversized value: key={}, value_len={}", key, serialized_value.len());
                                continue;
                            }
                            let order_id = scylladb::compute_order_id(&fastdata);
                            let row = FastDataKv {
                                receipt_id: fastdata.receipt_id,
                                action_index: fastdata.action_index,
                                tx_hash: fastdata.tx_hash,
                                signer_id: fastdata.signer_id.clone(),
                                predecessor_id: fastdata.predecessor_id.clone(),
                                current_account_id: fastdata.current_account_id.clone(),
                                block_height: fastdata.block_height,
                                block_timestamp: fastdata.block_timestamp,
                                shard_id: fastdata.shard_id,
                                receipt_index: fastdata.receipt_index,
                                order_id,

                                key: key.clone(),
                                value: serialized_value,
                            };
                            rows.push(row);
                        }
                        continue;
                    }
                }
                tracing::warn!(target: PROJECT_ID, "Received invalid Key-Value Fastdata");
            }
            SuffixFetcherUpdate::EndOfRange(block_height) => {
                tracing::info!(target: PROJECT_ID, "Saving last processed block height {} with {} rows", block_height, rows.len());
                let mut current_rows = vec![];
                std::mem::swap(&mut rows, &mut current_rows);
                add_kv_rows(
                    &scylladb,
                    &kv_insert_query,
                    &kv_last_insert_query,
                    current_rows,
                    block_height,
                )
                .await
                .expect("Error adding Key-Value rows to ScyllaDB");
                if !is_running.load(Ordering::SeqCst) {
                    tracing::info!(target: PROJECT_ID, "Shutting down...");
                    break;
                }
            }
        };
    }

    tracing::info!(target: PROJECT_ID, "Successfully shut down");
}
