use crate::*;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use scylla::serialize::row::SerializeRow;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::{DeserializeRow, SerializeRow};
use scylladb::{ScyllaDb, SCYLLADB};
use std::collections::HashMap;

pub(crate) const SUFFIX: &str = "kv";
pub(crate) const INDEXER_ID: &str = "kv-1";

#[derive(Debug, Clone)]
pub struct FastDataKv {
    pub receipt_id: CryptoHash,
    pub action_index: u32,
    pub tx_hash: Option<CryptoHash>,
    pub signer_id: AccountId,
    pub predecessor_id: AccountId,
    pub current_account_id: AccountId,
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    pub shard_id: u32,
    pub receipt_index: u32,
    pub order_id: u64,

    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
pub(crate) struct FastDataKvRow {
    pub receipt_id: String,
    pub action_index: i32,
    pub tx_hash: Option<String>,
    pub signer_id: String,
    pub predecessor_id: String,
    pub current_account_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub shard_id: i32,
    pub receipt_index: i32,
    pub order_id: i64,

    pub key: String,
    pub value: String,
}

impl From<FastDataKvRow> for FastDataKv {
    fn from(row: FastDataKvRow) -> Self {
        Self {
            receipt_id: row.receipt_id.parse().unwrap(),
            action_index: row.action_index as u32,
            tx_hash: row.tx_hash.map(|h| h.parse().unwrap()),
            signer_id: row.signer_id.parse().unwrap(),
            predecessor_id: row.predecessor_id.parse().unwrap(),
            current_account_id: row.current_account_id.parse().unwrap(),
            block_height: row.block_height as u64,
            block_timestamp: row.block_timestamp as u64,
            shard_id: row.shard_id as u32,
            receipt_index: row.receipt_index as u32,
            order_id: row.order_id as u64,

            key: row.key,
            value: row.value,
        }
    }
}

impl From<FastDataKv> for FastDataKvRow {
    fn from(data: FastDataKv) -> Self {
        Self {
            receipt_id: data.receipt_id.to_string(),
            action_index: data.action_index as i32,
            tx_hash: data.tx_hash.map(|h| h.to_string()),
            signer_id: data.signer_id.to_string(),
            predecessor_id: data.predecessor_id.to_string(),
            current_account_id: data.current_account_id.to_string(),
            block_height: data.block_height as i64,
            block_timestamp: data.block_timestamp as i64,
            shard_id: data.shard_id as i32,
            receipt_index: data.receipt_index as i32,
            order_id: data.order_id as i64,
            key: data.key,
            value: data.value,
        }
    }
}

/// Row for s_kv_last with USING TIMESTAMP for idempotent re-processing.
/// The timestamp ensures newer block data always wins even if older blocks are re-processed.
#[derive(Debug, Clone, SerializeRow)]
#[scylla(flavor = "enforce_order", skip_name_checks)]
pub(crate) struct FastDataKvLastRow {
    pub receipt_id: String,
    pub action_index: i32,
    pub tx_hash: Option<String>,
    pub signer_id: String,
    pub predecessor_id: String,
    pub current_account_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub shard_id: i32,
    pub receipt_index: i32,
    pub order_id: i64,
    pub key: String,
    pub value: String,
    pub timestamp: i64,
}

impl From<FastDataKvRow> for FastDataKvLastRow {
    fn from(row: FastDataKvRow) -> Self {
        let timestamp = row.block_height * 1_000_000 + row.order_id;
        Self {
            receipt_id: row.receipt_id,
            action_index: row.action_index,
            tx_hash: row.tx_hash,
            signer_id: row.signer_id,
            predecessor_id: row.predecessor_id,
            current_account_id: row.current_account_id,
            block_height: row.block_height,
            block_timestamp: row.block_timestamp,
            shard_id: row.shard_id,
            receipt_index: row.receipt_index,
            order_id: row.order_id,
            key: row.key,
            value: row.value,
            timestamp,
        }
    }
}

// __fastdata_kv({ "foo/alex.near": "bar", "foo/bob.near": "moo"})
// PRIMARY KEY ((predecessor_id), current_account_id, key)
// "key" >= "foo/" and "key" < "foo/" + '\xff'
// INDEX KEY ((current_account_id), key)

pub(crate) async fn create_tables(scylla_db: &ScyllaDb) -> anyhow::Result<()> {
    let queries = [
        "CREATE TABLE IF NOT EXISTS s_kv (
            receipt_id text,
            action_index int,
            tx_hash text,
            signer_id text,
            predecessor_id text,
            current_account_id text,
            block_height bigint,
            block_timestamp bigint,
            shard_id int,
            receipt_index int,
            order_id bigint,

            key text,
            value text,
            PRIMARY KEY ((predecessor_id), current_account_id, key, block_height, order_id)
        )",
        "CREATE TABLE IF NOT EXISTS s_kv_last (
            receipt_id text,
            action_index int,
            tx_hash text,
            signer_id text,
            predecessor_id text,
            current_account_id text,
            block_height bigint,
            block_timestamp bigint,
            shard_id int,
            receipt_index int,
            order_id bigint,

            key text,
            value text,
            PRIMARY KEY ((predecessor_id), current_account_id, key)
        )",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kv_key AS
            SELECT * FROM s_kv
            WHERE key IS NOT NULL AND block_height IS NOT NULL AND order_id IS NOT NULL
              AND predecessor_id IS NOT NULL AND current_account_id IS NOT NULL
            PRIMARY KEY((key), block_height, order_id, predecessor_id, current_account_id)
        ",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kv_cur_key AS
            SELECT * FROM s_kv
            WHERE current_account_id IS NOT NULL AND key IS NOT NULL AND block_height IS NOT NULL
              AND order_id IS NOT NULL AND predecessor_id IS NOT NULL
            PRIMARY KEY((current_account_id), key, block_height, order_id, predecessor_id)
        ",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kv_last_cur_key AS
            SELECT * FROM s_kv_last
            WHERE current_account_id IS NOT NULL AND key IS NOT NULL AND predecessor_id IS NOT NULL
            PRIMARY KEY ((current_account_id), key, predecessor_id)
        ", //        "CREATE INDEX IF NOT EXISTS idx_s_kv_tx_hash ON s_kv (tx_hash)",
           //        "CREATE INDEX IF NOT EXISTS idx_s_kv_receipt_id ON s_kv (receipt_id)",
    ];
    for query in queries.iter() {
        tracing::debug!(target: SCYLLADB, "Creating table: {}", query);
        scylla_db.scylla_session.query_unpaged(*query, &[]).await?;
    }
    Ok(())
}

pub(crate) async fn prepare_kv_insert_query(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
    "INSERT INTO s_kv (receipt_id, action_index, tx_hash, signer_id, predecessor_id, current_account_id, block_height, block_timestamp, shard_id, receipt_index, order_id, key, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        scylla::frame::types::Consistency::LocalQuorum,
    )
    .await
}

pub(crate) async fn prepare_kv_last_insert_query(
    scylla_db: &ScyllaDb,
) -> anyhow::Result<PreparedStatement> {
    ScyllaDb::prepare_query(
        &scylla_db.scylla_session,
        "INSERT INTO s_kv_last (receipt_id, action_index, tx_hash, signer_id, predecessor_id, current_account_id, block_height, block_timestamp, shard_id, receipt_index, order_id, key, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
        scylla::frame::types::Consistency::LocalQuorum,
    )
        .await
}

const MAX_BATCH_ROWS: usize = 100;

/// Deduplicate rows by full primary key of s_kv_last: (predecessor_id, current_account_id, key).
/// When duplicates exist, the last occurrence in the input wins.
fn dedup_kv_last_rows(kv_rows: &[FastDataKvRow]) -> Vec<FastDataKvLastRow> {
    kv_rows
        .iter()
        .cloned()
        .map(|row| {
            (
                (
                    row.predecessor_id.clone(),
                    row.current_account_id.clone(),
                    row.key.clone(),
                ),
                row,
            )
        })
        .collect::<HashMap<_, _>>()
        .into_values()
        .map(FastDataKvLastRow::from)
        .collect()
}

pub(crate) async fn add_kv_rows(
    scylla_db: &ScyllaDb,
    kv_insert_query: &PreparedStatement,
    kv_last_insert_query: &PreparedStatement,
    rows: Vec<FastDataKv>,
    last_processed_block_height: BlockHeight,
) -> anyhow::Result<()> {
    let kv_rows: Vec<FastDataKvRow> = rows
        .into_iter()
        .map(FastDataKvRow::from)
        .collect();

    let kv_last_rows = dedup_kv_last_rows(&kv_rows);

    // Write s_kv rows in chunks
    for chunk in kv_rows.chunks(MAX_BATCH_ROWS) {
        let mut batch = Batch::new(BatchType::Logged);
        let mut values: Vec<&dyn SerializeRow> = Vec::with_capacity(chunk.len());
        for kv in chunk {
            batch.append_statement(kv_insert_query.clone());
            values.push(kv);
        }
        scylla_db.scylla_session.batch(&batch, values).await?;
    }

    // Write s_kv_last rows in chunks (with USING TIMESTAMP for idempotent re-processing)
    for chunk in kv_last_rows.chunks(MAX_BATCH_ROWS) {
        let mut batch = Batch::new(BatchType::Logged);
        let mut values: Vec<&dyn SerializeRow> = Vec::with_capacity(chunk.len());
        for kv_last in chunk {
            batch.append_statement(kv_last_insert_query.clone());
            values.push(kv_last);
        }
        scylla_db.scylla_session.batch(&batch, values).await?;
    }

    // Update checkpoint
    scylla_db
        .set_last_processed_block_height(INDEXER_ID, last_processed_block_height)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(
        predecessor_id: &str,
        current_account_id: &str,
        key: &str,
        value: &str,
        block_height: i64,
        order_id: i64,
    ) -> FastDataKvRow {
        FastDataKvRow {
            receipt_id: "receipt1".to_string(),
            action_index: 0,
            tx_hash: None,
            signer_id: "signer.near".to_string(),
            predecessor_id: predecessor_id.to_string(),
            current_account_id: current_account_id.to_string(),
            block_height,
            block_timestamp: 1_000_000_000,
            shard_id: 0,
            receipt_index: 0,
            order_id,
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn test_dedup_preserves_different_keys() {
        let rows = vec![
            make_row("alice.near", "contract.near", "foo", "1", 100, 1),
            make_row("alice.near", "contract.near", "bar", "2", 100, 2),
            make_row("alice.near", "contract.near", "baz", "3", 100, 3),
        ];
        let result = dedup_kv_last_rows(&rows);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_dedup_same_key_same_account_keeps_one() {
        let rows = vec![
            make_row("alice.near", "contract.near", "score", "\"old\"", 100, 1),
            make_row("alice.near", "contract.near", "score", "\"new\"", 100, 2),
        ];
        let result = dedup_kv_last_rows(&rows);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_dedup_same_key_different_predecessors_preserves_both() {
        // This is the bug that P0 fixed: different predecessors with the same key
        // must NOT be deduplicated against each other
        let rows = vec![
            make_row("alice.near", "contract.near", "score", "\"100\"", 100, 1),
            make_row("bob.near", "contract.near", "score", "\"200\"", 100, 2),
        ];
        let result = dedup_kv_last_rows(&rows);
        assert_eq!(
            result.len(),
            2,
            "different predecessors with same key must both survive dedup"
        );
    }

    #[test]
    fn test_dedup_same_key_different_accounts_preserves_both() {
        let rows = vec![
            make_row("alice.near", "app1.near", "score", "\"100\"", 100, 1),
            make_row("alice.near", "app2.near", "score", "\"200\"", 100, 2),
        ];
        let result = dedup_kv_last_rows(&rows);
        assert_eq!(
            result.len(),
            2,
            "different current_account_ids with same key must both survive dedup"
        );
    }

    #[test]
    fn test_dedup_empty_input() {
        let result = dedup_kv_last_rows(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_timestamp_computation() {
        let row = make_row("alice.near", "contract.near", "key", "\"val\"", 100, 42);
        let last_row = FastDataKvLastRow::from(row);
        assert_eq!(last_row.timestamp, 100 * 1_000_000 + 42);
    }

    #[test]
    fn test_timestamp_ordering_across_blocks() {
        let row_block_100 = make_row("a.near", "c.near", "k", "\"a\"", 100, 999);
        let row_block_101 = make_row("a.near", "c.near", "k", "\"b\"", 101, 0);

        let ts_100 = FastDataKvLastRow::from(row_block_100).timestamp;
        let ts_101 = FastDataKvLastRow::from(row_block_101).timestamp;

        assert!(
            ts_101 > ts_100,
            "block 101 timestamp ({ts_101}) must beat block 100 ({ts_100})"
        );
    }

    #[test]
    fn test_timestamp_ordering_within_block() {
        let row_early = make_row("a.near", "c.near", "k", "\"a\"", 100, 5);
        let row_late = make_row("a.near", "c.near", "k", "\"b\"", 100, 10);

        let ts_early = FastDataKvLastRow::from(row_early).timestamp;
        let ts_late = FastDataKvLastRow::from(row_late).timestamp;

        assert!(
            ts_late > ts_early,
            "higher order_id within same block must have higher timestamp"
        );
    }

    #[test]
    fn test_kv_row_roundtrip() {
        let original = FastDataKv {
            receipt_id: "Aw1MHvj3GHBon1GBiGMWqafBJCBMqXTPw4PoSaSEjnkQ"
                .parse()
                .unwrap(),
            action_index: 3,
            tx_hash: Some(
                "6i9brQEJzLqmLLoqCcPPQih7YeYmLZcLxjW8zGnx3LFh"
                    .parse()
                    .unwrap(),
            ),
            signer_id: "alice.near".parse().unwrap(),
            predecessor_id: "bob.near".parse().unwrap(),
            current_account_id: "contract.near".parse().unwrap(),
            block_height: 123456789,
            block_timestamp: 1_700_000_000_000,
            shard_id: 2,
            receipt_index: 5,
            order_id: 200005003,
            key: "my_key".to_string(),
            value: "\"my_value\"".to_string(),
        };

        let row = FastDataKvRow::from(original.clone());
        assert_eq!(row.predecessor_id, "bob.near");
        assert_eq!(row.block_height, 123456789);
        assert_eq!(row.order_id, 200005003);

        let back: FastDataKv = row.into();
        assert_eq!(back.receipt_id, original.receipt_id);
        assert_eq!(back.action_index, original.action_index);
        assert_eq!(back.tx_hash, original.tx_hash);
        assert_eq!(back.signer_id, original.signer_id);
        assert_eq!(back.predecessor_id, original.predecessor_id);
        assert_eq!(back.current_account_id, original.current_account_id);
        assert_eq!(back.block_height, original.block_height);
        assert_eq!(back.block_timestamp, original.block_timestamp);
        assert_eq!(back.key, original.key);
        assert_eq!(back.value, original.value);
    }

    /// Integration tests requiring a local ScyllaDB instance.
    /// Start ScyllaDB: docker compose up -d
    /// Run: cargo test -p kv-sub-indexer -- --ignored
    mod integration {
        use super::*;
        use scylla::client::session_builder::SessionBuilder;
        use std::sync::Arc;

        struct TestDb {
            scylladb: Arc<ScyllaDb>,
            kv_insert: PreparedStatement,
            kv_last_insert: PreparedStatement,
        }

        async fn setup() -> TestDb {
            let session = SessionBuilder::new()
                .known_node("localhost:9042")
                .build()
                .await
                .expect("Failed to connect to ScyllaDB at localhost:9042");

            session
                .query_unpaged(
                    "CREATE KEYSPACE IF NOT EXISTS fastdata_testnet \
                     WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                    &[],
                )
                .await
                .expect("Failed to create keyspace");

            let chain_id: fastnear_primitives::types::ChainId =
                "testnet".to_string().try_into().expect("Invalid chain id");
            let scylladb = Arc::new(
                ScyllaDb::new(chain_id, session, true)
                    .await
                    .expect("Failed to create ScyllaDb"),
            );

            create_tables(&scylladb)
                .await
                .expect("Failed to create KV tables");

            // Clean state
            for table in &["s_kv", "s_kv_last", "meta"] {
                let _ = scylladb
                    .scylla_session
                    .query_unpaged(format!("TRUNCATE {table}"), &[])
                    .await;
            }

            let kv_insert = prepare_kv_insert_query(&scylladb)
                .await
                .expect("Failed to prepare kv insert");
            let kv_last_insert = prepare_kv_last_insert_query(&scylladb)
                .await
                .expect("Failed to prepare kv_last insert");

            TestDb {
                scylladb,
                kv_insert,
                kv_last_insert,
            }
        }

        fn make_kv(
            predecessor: &str,
            account: &str,
            key: &str,
            value: &str,
            block_height: u64,
            order_id: u64,
        ) -> FastDataKv {
            FastDataKv {
                receipt_id: "Aw1MHvj3GHBon1GBiGMWqafBJCBMqXTPw4PoSaSEjnkQ"
                    .parse()
                    .unwrap(),
                action_index: 0,
                tx_hash: None,
                signer_id: predecessor.parse().unwrap(),
                predecessor_id: predecessor.parse().unwrap(),
                current_account_id: account.parse().unwrap(),
                block_height,
                block_timestamp: block_height * 1_000_000_000,
                shard_id: 0,
                receipt_index: 0,
                order_id,
                key: key.to_string(),
                value: value.to_string(),
            }
        }

        #[tokio::test]
        #[ignore]
        async fn test_roundtrip_and_checkpoint() {
            let db = setup().await;

            let rows = vec![
                make_kv("rt.near", "app.near", "k1", "\"v1\"", 100, 1),
                make_kv("rt.near", "app.near", "k2", "\"v2\"", 100, 2),
            ];
            add_kv_rows(&db.scylladb, &db.kv_insert, &db.kv_last_insert, rows, 100)
                .await
                .unwrap();

            // Verify s_kv has both rows
            let result = db
                .scylladb
                .scylla_session
                .query_unpaged(
                    "SELECT key, value FROM s_kv WHERE predecessor_id = 'rt.near'",
                    &[],
                )
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
            let kv_rows: Vec<_> = result
                .rows::<(String, String)>()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(kv_rows.len(), 2);

            // Verify s_kv_last has both rows
            let result = db
                .scylladb
                .scylla_session
                .query_unpaged(
                    "SELECT key, value FROM s_kv_last WHERE predecessor_id = 'rt.near'",
                    &[],
                )
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
            let last_rows: Vec<_> = result
                .rows::<(String, String)>()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(last_rows.len(), 2);

            // Verify checkpoint was written (exact value depends on parallel test execution)
            let height = db
                .scylladb
                .get_last_processed_block_height(INDEXER_ID)
                .await
                .unwrap();
            assert!(height.is_some(), "checkpoint must be set after add_kv_rows");
        }

        #[tokio::test]
        #[ignore]
        async fn test_dedup_preserves_different_predecessors() {
            let db = setup().await;

            // Same key "score" from two different predecessors in one batch
            let rows = vec![
                make_kv("dd-alice.near", "app.near", "score", "\"100\"", 200, 1),
                make_kv("dd-bob.near", "app.near", "score", "\"200\"", 200, 2),
            ];
            add_kv_rows(&db.scylladb, &db.kv_insert, &db.kv_last_insert, rows, 200)
                .await
                .unwrap();

            // Both must exist in s_kv_last
            let alice = db
                .scylladb
                .scylla_session
                .query_unpaged(
                    "SELECT value FROM s_kv_last \
                     WHERE predecessor_id = 'dd-alice.near' \
                     AND current_account_id = 'app.near' AND key = 'score'",
                    &[],
                )
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
            assert_eq!(alice.single_row::<(String,)>().unwrap().0, "\"100\"");

            let bob = db
                .scylladb
                .scylla_session
                .query_unpaged(
                    "SELECT value FROM s_kv_last \
                     WHERE predecessor_id = 'dd-bob.near' \
                     AND current_account_id = 'app.near' AND key = 'score'",
                    &[],
                )
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
            assert_eq!(bob.single_row::<(String,)>().unwrap().0, "\"200\"");
        }

        #[tokio::test]
        #[ignore]
        async fn test_idempotent_reprocessing() {
            let db = setup().await;

            // First: process block 301
            let rows_301 = vec![make_kv(
                "idem.near",
                "app.near",
                "score",
                "\"new\"",
                301,
                1,
            )];
            add_kv_rows(
                &db.scylladb,
                &db.kv_insert,
                &db.kv_last_insert,
                rows_301,
                301,
            )
            .await
            .unwrap();

            // Then: re-process block 300 (simulating restart from earlier checkpoint)
            let rows_300 = vec![make_kv(
                "idem.near",
                "app.near",
                "score",
                "\"old\"",
                300,
                1,
            )];
            add_kv_rows(
                &db.scylladb,
                &db.kv_insert,
                &db.kv_last_insert,
                rows_300,
                300,
            )
            .await
            .unwrap();

            // s_kv_last must retain block 301's value (USING TIMESTAMP ensures newer wins)
            let result = db
                .scylladb
                .scylla_session
                .query_unpaged(
                    "SELECT value, block_height FROM s_kv_last \
                     WHERE predecessor_id = 'idem.near' \
                     AND current_account_id = 'app.near' AND key = 'score'",
                    &[],
                )
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
            let (value, height) = result.single_row::<(String, i64)>().unwrap();
            assert_eq!(
                value, "\"new\"",
                "block 301 value must survive re-processing of block 300"
            );
            assert_eq!(height, 301);

            // s_kv must have both blocks (history is preserved)
            let result = db
                .scylladb
                .scylla_session
                .query_unpaged(
                    "SELECT block_height FROM s_kv \
                     WHERE predecessor_id = 'idem.near' \
                     AND current_account_id = 'app.near' AND key = 'score'",
                    &[],
                )
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
            let heights: Vec<i64> = result
                .rows::<(i64,)>()
                .unwrap()
                .map(|r| r.unwrap().0)
                .collect();
            assert_eq!(heights.len(), 2);
            assert!(heights.contains(&300));
            assert!(heights.contains(&301));
        }

        #[tokio::test]
        #[ignore]
        async fn test_mv_kv_last_queryable_by_account() {
            let db = setup().await;

            let rows = vec![make_kv(
                "mv.near",
                "mv-app.near",
                "level",
                "\"42\"",
                400,
                1,
            )];
            add_kv_rows(&db.scylladb, &db.kv_insert, &db.kv_last_insert, rows, 400)
                .await
                .unwrap();

            // Query via the materialized view (by current_account_id instead of predecessor_id)
            let result = db
                .scylladb
                .scylla_session
                .query_unpaged(
                    "SELECT key, value, predecessor_id FROM mv_kv_last_cur_key \
                     WHERE current_account_id = 'mv-app.near'",
                    &[],
                )
                .await
                .unwrap()
                .into_rows_result()
                .unwrap();
            let (key, value, predecessor) =
                result.single_row::<(String, String, String)>().unwrap();
            assert_eq!(key, "level");
            assert_eq!(value, "\"42\"");
            assert_eq!(predecessor, "mv.near");
        }

        /// Seed a richer dataset and test various retrieval patterns across all tables/MVs.
        async fn seed_retrieval_data(db: &TestDb) {
            // Two predecessors writing to the same contract with overlapping keys.
            // All keys prefixed with "q/" to avoid collisions with other parallel tests.
            let rows = vec![
                // alice writes settings and scores to game.near
                make_kv("q-alice.near", "q-game.near", "q/settings/volume", "\"80\"", 500, 1),
                make_kv("q-alice.near", "q-game.near", "q/settings/lang", "\"en\"", 500, 2),
                make_kv("q-alice.near", "q-game.near", "q/score", "\"1500\"", 500, 3),
                // bob writes to the same contract
                make_kv("q-bob.near", "q-game.near", "q/score", "\"2400\"", 500, 4),
                make_kv("q-bob.near", "q-game.near", "q/settings/volume", "\"50\"", 500, 5),
                // alice also writes to a different contract
                make_kv("q-alice.near", "q-shop.near", "q/cart", "\"[1,2,3]\"", 500, 6),
            ];
            add_kv_rows(&db.scylladb, &db.kv_insert, &db.kv_last_insert, rows, 500)
                .await
                .unwrap();

            // alice updates her score at a later block
            let rows2 = vec![
                make_kv("q-alice.near", "q-game.near", "q/score", "\"1800\"", 501, 1),
            ];
            add_kv_rows(&db.scylladb, &db.kv_insert, &db.kv_last_insert, rows2, 501)
                .await
                .unwrap();
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_s_kv_by_predecessor_and_prefix() {
            let db = setup().await;
            seed_retrieval_data(&db).await;

            // s_kv: get all keys alice wrote to game.near with prefix "settings/"
            let result = db.scylladb.scylla_session.query_unpaged(
                "SELECT key, value FROM s_kv \
                 WHERE predecessor_id = 'q-alice.near' AND current_account_id = 'q-game.near' \
                 AND key >= 'q/settings/' AND key < 'q/settings0'",
                &[],
            ).await.unwrap().into_rows_result().unwrap();
            let rows: Vec<(String, String)> = result.rows::<(String, String)>().unwrap()
                .collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(rows.len(), 2, "alice has 2 q/settings/ keys in s_kv");
            let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
            assert!(keys.contains(&"q/settings/lang"));
            assert!(keys.contains(&"q/settings/volume"));
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_s_kv_history_for_key() {
            let db = setup().await;
            seed_retrieval_data(&db).await;

            // s_kv: get full history of alice's "score" key (blocks 500 and 501)
            let result = db.scylladb.scylla_session.query_unpaged(
                "SELECT block_height, value FROM s_kv \
                 WHERE predecessor_id = 'q-alice.near' AND current_account_id = 'q-game.near' \
                 AND key = 'q/score'",
                &[],
            ).await.unwrap().into_rows_result().unwrap();
            let rows: Vec<(i64, String)> = result.rows::<(i64, String)>().unwrap()
                .collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(rows.len(), 2, "alice's score has 2 history entries");
            let values_by_height: std::collections::HashMap<i64, String> =
                rows.into_iter().collect();
            assert_eq!(values_by_height[&500], "\"1500\"");
            assert_eq!(values_by_height[&501], "\"1800\"");
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_s_kv_last_latest_value() {
            let db = setup().await;
            seed_retrieval_data(&db).await;

            // s_kv_last: alice's score should be the updated value from block 501
            let result = db.scylladb.scylla_session.query_unpaged(
                "SELECT value, block_height FROM s_kv_last \
                 WHERE predecessor_id = 'q-alice.near' AND current_account_id = 'q-game.near' \
                 AND key = 'q/score'",
                &[],
            ).await.unwrap().into_rows_result().unwrap();
            let (value, height) = result.single_row::<(String, i64)>().unwrap();
            assert_eq!(value, "\"1800\"", "s_kv_last must have the updated score");
            assert_eq!(height, 501);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_mv_kv_key_global_key_lookup() {
            let db = setup().await;
            seed_retrieval_data(&db).await;

            // mv_kv_key: find ALL writes to key "score" across all predecessors
            let result = db.scylladb.scylla_session.query_unpaged(
                "SELECT predecessor_id, value, block_height FROM mv_kv_key \
                 WHERE key = 'q/score'",
                &[],
            ).await.unwrap().into_rows_result().unwrap();
            let rows: Vec<(String, String, i64)> = result
                .rows::<(String, String, i64)>().unwrap()
                .collect::<Result<Vec<_>, _>>().unwrap();
            // alice wrote score at blocks 500 and 501, bob at block 500
            assert_eq!(rows.len(), 3, "3 total score writes across all predecessors");
            let predecessors: Vec<&str> = rows.iter().map(|(p, _, _)| p.as_str()).collect();
            assert!(predecessors.contains(&"q-alice.near"));
            assert!(predecessors.contains(&"q-bob.near"));
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_mv_kv_cur_key_by_account() {
            let db = setup().await;
            seed_retrieval_data(&db).await;

            // mv_kv_cur_key: find all KV history for game.near contract
            let result = db.scylladb.scylla_session.query_unpaged(
                "SELECT predecessor_id, key, value FROM mv_kv_cur_key \
                 WHERE current_account_id = 'q-game.near'",
                &[],
            ).await.unwrap().into_rows_result().unwrap();
            let rows: Vec<(String, String, String)> = result
                .rows::<(String, String, String)>().unwrap()
                .collect::<Result<Vec<_>, _>>().unwrap();
            // alice: settings/volume, settings/lang, score(x2); bob: score, settings/volume
            assert_eq!(rows.len(), 6, "6 total KV entries for q-game.near");
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_mv_kv_cur_key_with_key_prefix() {
            let db = setup().await;
            seed_retrieval_data(&db).await;

            // mv_kv_cur_key: get all "settings/*" keys written to game.near by anyone
            let result = db.scylladb.scylla_session.query_unpaged(
                "SELECT predecessor_id, key, value FROM mv_kv_cur_key \
                 WHERE current_account_id = 'q-game.near' \
                 AND key >= 'q/settings/' AND key < 'q/settings0'",
                &[],
            ).await.unwrap().into_rows_result().unwrap();
            let rows: Vec<(String, String, String)> = result
                .rows::<(String, String, String)>().unwrap()
                .collect::<Result<Vec<_>, _>>().unwrap();
            // alice: settings/lang, settings/volume; bob: settings/volume
            assert_eq!(rows.len(), 3, "3 q/settings/ entries across all predecessors");
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_mv_kv_last_cur_key_latest_by_account() {
            let db = setup().await;
            seed_retrieval_data(&db).await;

            // mv_kv_last_cur_key: get all latest values for game.near
            let result = db.scylladb.scylla_session.query_unpaged(
                "SELECT predecessor_id, key, value FROM mv_kv_last_cur_key \
                 WHERE current_account_id = 'q-game.near'",
                &[],
            ).await.unwrap().into_rows_result().unwrap();
            let rows: Vec<(String, String, String)> = result
                .rows::<(String, String, String)>().unwrap()
                .collect::<Result<Vec<_>, _>>().unwrap();
            // alice: settings/volume, settings/lang, score; bob: score, settings/volume
            assert_eq!(rows.len(), 5, "5 unique (predecessor, key) combos for game.near");

            // Verify alice's score is the updated value
            let alice_score = rows.iter()
                .find(|(p, k, _)| p == "q-alice.near" && k == "q/score")
                .expect("alice's score must be in results");
            assert_eq!(alice_score.2, "\"1800\"", "latest score from MV must match s_kv_last");
        }
    }
}
