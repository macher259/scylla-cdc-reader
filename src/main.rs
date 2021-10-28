use scylla_cdc_reader::{create_session, do_query_loop, parse_stream_id};
use std::{env, process};

const DEFAULT_SCYLLA_URI: &str = "172.17.0.2:9042";

#[tokio::main]
async fn main() {
    let stream_id = parse_stream_id(env::args()).await.unwrap_or_else(|e| {
        println!("{}", e);
        process::exit(1);
    });

    let session =
        create_session(std::env::var("SCYLLA_URI")
            .unwrap_or_else(|_| DEFAULT_SCYLLA_URI.to_string()))
            .await
            .unwrap_or_else(|e| {
                eprintln!("Couldn't create a session: {}.", e.to_string());
                process::exit(1);
            });

    do_query_loop(session, &stream_id).await;
}
