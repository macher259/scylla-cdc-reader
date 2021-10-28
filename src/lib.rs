use futures::stream::StreamExt;
use scylla::frame::value::Timestamp;
use scylla::transport::errors::NewSessionError;
use scylla::{Session, SessionBuilder};
use std::env;
use std::time::Duration;

const QUERY: &str = r#"
SELECT "cdc$time", "cdc$batch_seq_no", "cdc$operation", pk, ck, v
FROM ks.t_scylla_cdc_log
WHERE "cdc$stream_id" = ? AND
"cdc$time" >= maxTimeuuid(?) AND "cdc$time" < minTimeuuid(?);
"#;

pub async fn parse_stream_id(mut args: env::Args) -> Result<Vec<u8>, &'static str> {
    args.next();
    return match args.next() {
        None => Err("Not enough parameters provided."),
        Some(id) => match hex::decode(
            id.strip_prefix("0x").unwrap_or_else(|| id.as_str())
        ) {
            Ok(id) => Ok(id),
            Err(_) => Err("Error while decoding"),
        },
    };
}

// Create a new session with one node at given address.
pub async fn create_session(uri: String) -> std::result::Result<Session, NewSessionError> {
    SessionBuilder::new().known_node(uri).build().await
}

// Execute a single query in an endless loop.
pub async fn do_query_loop(session: Session, stream_id: &Vec<u8>) {
    type QueryType = (uuid::Uuid, i32, i8, i32, i32, i32);

    let prepared = session.prepare(QUERY).await.unwrap();

    let mut min_time = 0;
    let time_delta = 3;

    loop {
        let max_time = chrono::Local::now().timestamp_millis();
        let mut rows_stream = session
            .execute_iter(
                prepared.clone(),
                (
                    stream_id,
                    Timestamp(chrono::Duration::milliseconds(min_time)),
                    Timestamp(chrono::Duration::milliseconds(max_time)),
                ),
            )
            .await
            .unwrap()
            .into_typed::<QueryType>();

        while let Some(next_row_result) = rows_stream.next().await {
            let (time, batch, operation, pk, ck, v) = next_row_result.unwrap();
            println!(
                "[CDC]:time={} batch={} operation={} pk={} ck={} v={}",
                time, batch, operation, pk, ck, v
            );
        }
        min_time = max_time;
        tokio::time::sleep(Duration::from_secs(time_delta)).await;
    }
}
