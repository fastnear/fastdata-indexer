use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::near_primitives::views::{ActionView, ReceiptEnumView};
use fastnear_primitives::types::ChainId;
use scylladb::{FastData, ScyllaDb, UNIVERSAL_SUFFIX};
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

const FASTDATA_PREFIX: &str = "__fastdata_";
const PROJECT_ID: &str = "fastdata-indexer";

#[tokio::main]
async fn main() {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("neardata-fetcher=info,fastdata-indexer=info,scylladb=info")
        .init();

    let chain_id: ChainId = env::var("CHAIN_ID")
        .expect("CHAIN_ID required")
        .try_into()
        .expect("Invalid chain id");

    let scylla_session = ScyllaDb::new_scylla_session()
        .await
        .expect("Can't create scylla session");

    ScyllaDb::test_connection(&scylla_session)
        .await
        .expect("Can't connect to scylla");

    tracing::info!(target: PROJECT_ID, "Connected to Scylla");

    let scylladb = ScyllaDb::new(chain_id, scylla_session, true)
        .await
        .expect("Can't create scylla db");

    let last_processed_block_height = scylladb
        .get_last_processed_block_height(UNIVERSAL_SUFFIX)
        .await
        .expect("Error getting last processed block height");

    tracing::info!(target: PROJECT_ID, "Latest processed block height in DB: {:?}", last_processed_block_height);

    let num_threads = env::var("NUM_THREADS")
        .ok()
        .map(|num_threads| num_threads.parse().expect("Invalid number of threads"))
        .unwrap_or(8);

    let client = reqwest::Client::new();
    let last_block_height = fetcher::fetch_last_block(&client, chain_id)
        .await
        .unwrap()
        .block
        .header
        .height;

    tracing::info!(target: PROJECT_ID, "Last neardata block height: {}", last_block_height);

    let start_block_height: BlockHeight = last_processed_block_height
        .map(|h| h + 1)
        .unwrap_or_else(|| {
            env::var("START_BLOCK_HEIGHT")
                .ok()
                .map(|start_block_height| start_block_height.parse().expect("Invalid block height"))
                .unwrap_or(last_block_height)
        });

    let auth_bearer_token = env::var("FASTNEAR_AUTH_BEARER_TOKEN").ok();
    let mut config = fetcher::FetcherConfigBuilder::new()
        .start_block_height(start_block_height)
        .num_threads(num_threads)
        .chain_id(chain_id);
    if let Some(token) = auth_bearer_token.clone() {
        config = config.auth_bearer_token(token);
    }

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();

    ctrlc::set_handler(move || {
        ctrl_c_running.store(false, Ordering::SeqCst);
        tracing::info!(target: PROJECT_ID, "Received Ctrl+C, starting shutdown...");
    })
    .expect("Error setting Ctrl+C handler");

    let block_update_interval = std::time::Duration::from_millis(
        env::var("BLOCK_UPDATE_INTERVAL_MS")
            .ok()
            .map(|ms| ms.parse().expect("Invalid number of blocks"))
            .unwrap_or(5000),
    );

    tracing::info!(target: PROJECT_ID,
        "Starting {} fetcher with {} threads from height {}. Using auth token: {}",
        chain_id,
        num_threads,
        start_block_height,
        auth_bearer_token.is_some()
    );

    let (sender, mut receiver) = mpsc::channel((num_threads * 10) as _);
    tokio::spawn(fetcher::start_fetcher(
        config.build(),
        sender,
        is_running.clone(),
    ));

    let mut last_block_update = std::time::SystemTime::now();
    while let Some(block) = receiver.recv().await {
        let block_height = block.block.header.height;
        let block_timestamp = block.block.header.timestamp;
        tracing::info!(target: PROJECT_ID, "Received block: {}", block_height);

        let mut data = vec![];

        for shard in block.shards {
            for (receipt_index, reo) in shard.receipt_execution_outcomes.into_iter().enumerate() {
                let receipt = reo.receipt;
                let receipt_id = receipt.receipt_id;
                let predecessor_id = receipt.predecessor_id;
                let current_account_id = receipt.receiver_id;
                let tx_hash = reo.tx_hash;
                if let ReceiptEnumView::Action {
                    signer_id, actions, ..
                } = receipt.receipt
                {
                    for (action_index, action) in actions.into_iter().enumerate() {
                        if let ActionView::FunctionCall {
                            method_name, args, ..
                        } = action
                        {
                            if method_name.starts_with(FASTDATA_PREFIX) {
                                let suffix = method_name.strip_prefix(FASTDATA_PREFIX).unwrap();
                                data.push(FastData {
                                    receipt_id,
                                    action_index: action_index as _,
                                    suffix: suffix.to_string(),
                                    data: args.to_vec(),
                                    tx_hash,
                                    signer_id: signer_id.clone(),
                                    predecessor_id: predecessor_id.clone(),
                                    current_account_id: current_account_id.clone(),
                                    block_height,
                                    block_timestamp,
                                    shard_id: shard.shard_id.into(),
                                    receipt_index: receipt_index as _,
                                });
                            }
                        }
                    }
                }
            }
        }

        let current_time = std::time::SystemTime::now();
        let duration = current_time
            .duration_since(last_block_update)
            .expect("Time went backwards");
        let mut need_to_save_last_processed_block_height = duration >= block_update_interval;

        if !data.is_empty() {
            tracing::info!(target: PROJECT_ID, "Inserting {} fastdata rows into Scylla", data.len());
            let futures = futures::future::join_all(
                data.into_iter().map(|fastdata| scylladb.add_data(fastdata)),
            );
            // Wait for all futures to complete
            let result = futures.await.into_iter().collect::<anyhow::Result<()>>();
            if let Err(e) = result {
                tracing::error!(target: PROJECT_ID, "Error inserting data into Scylla: {:?}", e);
                panic!("TODO retry: {:?}", e);
            }
            need_to_save_last_processed_block_height = true;
        }

        if !is_running.load(Ordering::SeqCst) {
            tracing::info!(target: PROJECT_ID, "Shutting down fetcher");
            need_to_save_last_processed_block_height = true;
        }

        if need_to_save_last_processed_block_height {
            tracing::info!(target: PROJECT_ID, "Saving last processed block height: {}", block_height);
            scylladb
                .set_last_processed_block_height(UNIVERSAL_SUFFIX, block_height)
                .await
                .expect("Error setting last processed block height");
            last_block_update = current_time;
        }

        if !is_running.load(Ordering::SeqCst) {
            break;
        }
    }

    tracing::info!(target: PROJECT_ID, "Successfully shut down");
}
