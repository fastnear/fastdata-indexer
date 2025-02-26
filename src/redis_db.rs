use redis::aio::MultiplexedConnection;
use redis::Client;
use std::env;

pub struct RedisDB {
    pub client: Client,
    pub connection: MultiplexedConnection,
}

#[allow(dead_code)]
impl RedisDB {
    pub async fn new(redis_url: Option<String>) -> redis::RedisResult<Self> {
        let client = Client::open(
            redis_url.unwrap_or_else(|| env::var("REDIS_URL").expect("Missing REDIS_URL env var")),
        )?;
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self { client, connection })
    }

    pub async fn reconnect(&mut self) -> redis::RedisResult<()> {
        self.connection = self.client.get_multiplexed_async_connection().await?;
        Ok(())
    }
}

#[allow(dead_code)]
impl RedisDB {
    pub async fn set(&mut self, key: &str, value: &str) -> redis::RedisResult<String> {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async(&mut self.connection)
            .await
    }

    pub async fn get(&mut self, key: &str) -> redis::RedisResult<String> {
        redis::cmd("GET")
            .arg(key)
            .query_async(&mut self.connection)
            .await
    }
}

#[macro_export]
macro_rules! with_retries {
    ($db: expr, $f_async: expr) => {
        {
            let mut delay = tokio::time::Duration::from_millis(100);
            let max_retries = 10;
            let mut i = 0;
            loop {
                match $f_async(&mut $db.connection).await {
                    Ok(v) => break Ok(v),
                    Err(err) => {
                        tracing::log::error!(target: "redis", "Attempt #{}: {}", i, err);
                        tokio::time::sleep(delay).await;
                        let _ = $db.reconnect().await;
                        delay *= 2;
                        if i == max_retries - 1 {
                            break Err(err);
                        }
                    }
                };
                i += 1;
            }
        }
    };
}
