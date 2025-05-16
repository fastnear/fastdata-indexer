use scylladb::{FastData, ScyllaDb, UNIVERSAL_SUFFIX};

use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::types::ChainId;
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const FETCHER: &str = "suffix-fetcher";

pub struct SuffixFetcher {
    pub scylladb: Arc<ScyllaDb>,
}

pub struct SuffixFetcherConfig {
    pub suffix: String,
    pub start_block_height: Option<BlockHeight>,
    pub sleep_duration: Duration,
}

impl SuffixFetcher {
    pub async fn new(chain_id: ChainId, scylladb: Option<Arc<ScyllaDb>>) -> anyhow::Result<Self> {
        let scylladb = match scylladb {
            Some(scylladb) => scylladb,
            None => {
                let scylla_session = ScyllaDb::new_scylla_session()
                    .await
                    .expect("Can't create scylla session");
                ScyllaDb::test_connection(&scylla_session)
                    .await
                    .expect("Can't connect to scylla");
                tracing::info!(target: FETCHER, "Connected to Scylla");

                Arc::new(ScyllaDb::new(chain_id, scylla_session).await?)
            }
        };
        Ok(Self { scylladb })
    }

    pub fn get_scylladb(&self) -> Arc<ScyllaDb> {
        self.scylladb.clone()
    }

    pub async fn start(
        self,
        config: SuffixFetcherConfig,
        sink: mpsc::Sender<FastData>,
        is_running: Arc<AtomicBool>,
    ) {
        let mut from_block_height = config.start_block_height.unwrap_or(0);
        tracing::info!(target: FETCHER, "Starting suffix fetcher with suffix {:?} from {}", config.suffix, from_block_height);
        while is_running.load(Ordering::SeqCst) {
            let last_block_height = self
                .scylladb
                .get_last_processed_block_height(UNIVERSAL_SUFFIX)
                .await
                .expect("Error getting last processed block height");
            if last_block_height.is_none() {
                tracing::info!(target: FETCHER, "No last processed block height found");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            let last_block_height = last_block_height.unwrap();
            if from_block_height > last_block_height {
                tracing::debug!(target: FETCHER, "Waiting for new blocks");
                tokio::time::sleep(config.sleep_duration).await;
                continue;
            }
            tracing::info!(target: FETCHER, "Fetching blocks from {} to {}", from_block_height, last_block_height);
            let mut stream = self
                .scylladb
                .get_suffix_data(&config.suffix, from_block_height, last_block_height)
                .await
                .expect("Error getting suffix data");
            while let Some(fastdata) = stream.next().await {
                if !is_running.load(Ordering::SeqCst) {
                    break;
                }
                match fastdata {
                    Ok(fastdata) => {
                        sink.send(fastdata)
                            .await
                            .expect("Error sending row to sink");
                    }
                    Err(e) => {
                        tracing::error!(target: FETCHER, "Error fetching row: {:?}", e);
                        panic!("TODO: Error fetching row: {:?}", e);
                    }
                }
            }
            from_block_height = last_block_height + 1;
        }
        tracing::info!(target: FETCHER, "Stopped suffix fetcher");
    }
}
