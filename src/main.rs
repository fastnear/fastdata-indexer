mod redis_db;

use dotenv::dotenv;
use fastnear_neardata_fetcher::{fetcher, FetcherConfig};
use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use fastnear_primitives::near_primitives::types::AccountId;
use fastnear_primitives::near_primitives::views::ActionView;
use fastnear_primitives::types::ChainId;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

const FASTDATA_PREFIX: &str = "__fastdata_";
const PROJECT_ID: &str = "fastdata-indexer";

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct FastData {
    pub block_height: BlockHeight,
    pub tx_hash: CryptoHash,
    pub action_index: usize,
    pub predecessor_id: AccountId,
    pub current_account_id: AccountId,
    pub suffix: String,
    // Base64 encoded
    #[serde_as(as = "Base64")]
    pub data: Vec<u8>,
}

impl FastData {
    pub fn redis_key(&self) -> String {
        format!("raw:{}:{}", self.tx_hash, self.action_index)
    }
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("redis=info")
        .with_env_filter("neardata-fetcher=info")
        .with_env_filter("fastdata-indexer=info")
        .init();
    let mut redis_client = redis_db::RedisDB::new(None).await.unwrap();

    let latest_redis_block_height: redis::RedisResult<Option<BlockHeight>> =
        with_retries!(redis_client, |connection| async {
            redis::cmd("GET")
                .arg("meta:last_block_height")
                .query_async(connection)
                .await
        });
    let latest_redis_block_height =
        latest_redis_block_height.expect("Error getting last block height from redis");

    tracing::info!(target: PROJECT_ID, "Latest block height in redis: {:?}", latest_redis_block_height);

    let num_threads = env::var("NUM_THREADS")
        .ok()
        .map(|num_threads| num_threads.parse().expect("Invalid number of threads"))
        .unwrap_or(8);

    let chain_id: ChainId = serde_json::from_str(&format!(
        "{:?}",
        env::var("CHAIN_ID").ok().expect("CHAIN_ID is required")
    ))
    .expect("Invalid chain id");

    let client = reqwest::Client::new();
    let last_block_height = fetcher::fetch_last_block(&client, chain_id)
        .await
        .unwrap()
        .block
        .header
        .height;

    tracing::info!(target: PROJECT_ID, "Last block height: {}", last_block_height);

    let start_block_height: BlockHeight =
        latest_redis_block_height.map(|h| h + 1).unwrap_or_else(|| {
            env::var("START_BLOCK_HEIGHT")
                .ok()
                .map(|start_block_height| start_block_height.parse().expect("Invalid block height"))
                .unwrap_or(last_block_height)
        });

    let auth_bearer_token = env::var("FASTNEAR_AUTH_BEARER_TOKEN").ok();

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();

    ctrlc::set_handler(move || {
        ctrl_c_running.store(false, Ordering::SeqCst);
        println!("Received Ctrl+C, starting shutdown...");
    })
    .expect("Error setting Ctrl+C handler");

    tracing::info!(target: PROJECT_ID,
        "Starting {} fetcher with {} threads from height {}. Auth token: {}",
        chain_id,
        num_threads,
        start_block_height,
        auth_bearer_token.is_some()
    );

    let (sender, mut receiver) = mpsc::channel(320);
    tokio::spawn(fetcher::start_fetcher(
        None,
        FetcherConfig {
            num_threads,
            start_block_height,
            chain_id,
            timeout_duration: None,
            retry_duration: None,
            disable_archive_sync: false,
            auth_bearer_token,
        },
        sender,
        is_running.clone(),
    ));

    let mut last_block_height = None;
    while let Some(block) = receiver.recv().await {
        let block_height = block.block.header.height;
        println!("Received block: {}", block_height);
        if block_height < start_block_height {
            // Temporary fix for the fetcher not starting from the correct block
            continue;
        }

        let mut data = vec![];

        for shard in block.shards {
            if let Some(chunk) = shard.chunk {
                for tx in chunk.transactions {
                    let tx_hash = tx.transaction.hash;

                    for (action_index, action) in tx.transaction.actions.into_iter().enumerate() {
                        if let ActionView::FunctionCall {
                            method_name, args, ..
                        } = action
                        {
                            if method_name.starts_with(FASTDATA_PREFIX) {
                                let suffix = method_name.strip_prefix(FASTDATA_PREFIX).unwrap();
                                data.push(FastData {
                                    block_height,
                                    tx_hash,
                                    action_index,
                                    predecessor_id: tx.transaction.signer_id.clone(),
                                    current_account_id: tx.transaction.receiver_id.clone(),
                                    suffix: suffix.to_string(),
                                    data: args.to_vec(),
                                });
                            }
                        }
                    }
                }
            }
        }

        if data.is_empty() {
            last_block_height = Some(block_height);
            continue;
        }
        let res: Result<(), redis::RedisError> = with_retries!(redis_client, |connection| async {
            let mut pipe = redis::pipe();

            for d in &data {
                pipe.cmd("SET")
                    .arg(d.redis_key())
                    .arg(serde_json::to_string(&d).unwrap())
                    .ignore();
            }

            pipe.set("meta:last_block_height", block_height).ignore();

            pipe.query_async(connection).await
        });
        res.expect("Error setting data to redis");
        last_block_height = None;

        for d in data {
            println!("Data {} -> {} bytes", d.redis_key(), d.data.len());
        }
    }

    if let Some(last_block_height) = last_block_height {
        let res: Result<(), redis::RedisError> = with_retries!(redis_client, |connection| async {
            redis::cmd("SET")
                .arg("meta:last_block_height")
                .arg(last_block_height)
                .query_async(connection)
                .await
        });
        res.expect("Error setting last block height to redis");
    }
}
