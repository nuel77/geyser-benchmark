mod types;

use crate::types::{Identity, WsResponse};
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use solana_client::nonblocking::pubsub_client;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::connect_async;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterSlots};
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;

#[tokio::main]
async fn main() {
    //let grpc_latency_task = grpc_latency("http://rpc:10000".to_string(), Duration::from_secs(10));
    //let ws_latency_task = geyser_websocket_latency("ws://rpc:9005".to_string(), Duration::from_secs(10));
    let solana_rpc_latency_task = solana_rpc_latency("wss://api.mainnet-beta.solana.com".to_string(), Duration::from_secs(10));
    let astralane_rpc_latency_task = solana_rpc_latency("wss://api.mainnet-beta.solana.com".to_string(), Duration::from_secs(10));
    //run both task in different threads

    let jh = tokio::task::spawn(solana_rpc_latency_task);

    let ws_latency = astralane_rpc_latency_task.await;
    let grpc_latency = jh.await.unwrap();

    let mut diff_vec = vec![];
    match (&grpc_latency, &ws_latency) {
        (Ok(grpc), Ok(ws)) => {
            for (slot_id, grpc_time) in grpc.iter() {
                if let Some(ws_time) = ws.get(slot_id) {
                    // ws_time - grpc_time
                    let ws_latency = calculate_latency_in_millis(*ws_time, *grpc_time);
                    println!("ws_latency: {:?}", ws_latency);
                    diff_vec.push(ws_latency as f64)
                }
            }
        }
        _ => {
            eprintln!("cannot run {:?} {:?}", ws_latency, grpc_latency);
        }
    }

    //print average diff
    let avg_diff = diff_vec.iter().sum::<f64>() / diff_vec.len() as f64;
    println!("on average ws_took: {} millis more time", avg_diff);
}

// basically doing (ws_time - grpc_time)
fn calculate_latency_in_millis(ws: Instant, grpc: Instant) -> f64 {
    let ws_slower_in_ms = ws.checked_duration_since(grpc).map(|v| v.as_millis());
    let grpc_slower_in_ms = grpc.checked_duration_since(ws).map(|v| v.as_millis());
    match (ws_slower_in_ms, grpc_slower_in_ms) {
        (Some(ws_latency), _) => ws_latency as f64,
        (_, Some(grpc_latency)) => -(grpc_latency as f64),
        _ => 0.0,
    }
}

async fn geyser_websocket_latency(
    endpoint: String,
    run_duration: Duration,
) -> anyhow::Result<HashMap<String, Instant>> {
    let mut latencies = HashMap::new();
    let (mut stream, _response) = connect_async(endpoint).await?;

    //send subscription payload to websocket
    let sub_payload = r#"{"method":"slot_subscribe","params":[]}"#;
    stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            sub_payload.to_string(),
        ))
        .await?;

    //listen to websocket messages and store the timestamp in map
    let sleep = tokio::time::sleep(run_duration);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => {
                break;
            }
            msg = stream.next() => {
                let msg = msg.ok_or(anyhow::anyhow!("stream closed"))??;
                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                    let now = tokio::time::Instant::now();
                    let response: WsResponse = serde_json::from_str(&text)?;
                    if response.result.commitment == solana_sdk::commitment_config::CommitmentLevel::Processed {
                        latencies.insert(response.result.slot.to_string(), now);
                    }
                }
            }
        }
    }

    Ok(latencies)
}

async fn grpc_latency(
    endpoint: String,
    run_duration: Duration,
) -> anyhow::Result<HashMap<String, Instant>> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint)
        //.and_then(|builder| builder.x_token(None))
        .map(|builder| builder.connect_timeout(Duration::from_secs(10)))
        .map(|builder| builder.timeout(Duration::from_secs(10)))
        .map(|builder| builder.max_decoding_message_size(64 * 1024 * 1024))
        .map_err(|e| anyhow::Error::msg(format!("Failed to create builder: {}", e)))?
        .connect()
        .await
        .context("cannot connect to grpc")?;

    let mut latencies = HashMap::default();
    //subscribe to slot updates and store the timestamp in map
    let mut slots = HashMap::new();
    slots.insert(
        "client".to_owned(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
        },
    );
    //get slot updates for the duration of run_duration
    let sub_request = SubscribeRequest {
        accounts: Default::default(),
        slots,
        transactions: Default::default(),
        transactions_status: Default::default(),
        blocks: Default::default(),
        blocks_meta: Default::default(),
        entry: Default::default(),
        commitment: None,
        accounts_data_slice: vec![],
        ping: None,
    };

    let (_sink, mut stream) = client
        .subscribe_with_request(Some(sub_request))
        .await
        .context("cannot subscribe to slot updates")?;

    let sleep = tokio::time::sleep(run_duration);
    tokio::pin!(sleep); // Pin the sleep future so it can be reused
    loop {
        tokio::select! {
            _ = &mut sleep  => {
                break;
            }
            update = stream.next() => {
                let update = update.ok_or_else(|| anyhow::anyhow!("stream closed"))??;
                if let Some(UpdateOneof::Slot(slot)) = update.update_oneof {
                    let now = tokio::time::Instant::now();
                    if slot.status == yellowstone_grpc_proto::prelude::CommitmentLevel::Processed as i32 {
                        latencies.insert(slot.slot.to_string(), now);
                    }
                }
            }
        }
    }

    Ok(latencies)
}

async fn solana_rpc_latency(endpoint: String, run_duration: Duration) -> anyhow::Result<HashMap<String, Instant>> {
    let mut latencies = HashMap::new();
    let client = pubsub_client::PubsubClient::new(&endpoint).await?;
    let pub_key = solana_program::pubkey::Pubkey::from_str("AbSR1An4izhrUkAx8CLpewdCJbMmnMgL1CDwwBG7465g").unwrap();
    //gives back processed blocks
    //https://solana.com/docs/rpc/websocket/slotsubscribe
    let (mut stream, unsub) = client.account_subscribe(&pub_key, None).await?;

    let sleep = tokio::time::sleep(run_duration);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            _ = &mut sleep => {
                println!("task time out");
                break;
            }
            response = stream.next() => {
                println!("response: {:?}", response);
                let now = tokio::time::Instant::now();
                if let Some(resp) = response{
                    let data_str = serde_json::to_string(&resp.value).unwrap();
                    let hex = hex::encode(data_str.into_bytes());
                    latencies.insert(hex, now);
                }else{
                    println!("no response");
                }
            }
        }
    }

    Ok(latencies)
}
