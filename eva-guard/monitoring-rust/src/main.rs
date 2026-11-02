
use chrono::Utc;
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use sysinfo::System;
use tokio::time::sleep;

const RPC_URL: &str = "http://127.0.0.1:8545";
const NODE_ID: &str = "geth-node-1";
const INTERVAL_SECONDS: u64 = 5;

struct GethMonitor {
    client: Client,
    rpc_url: String,
}

impl GethMonitor {
    fn new(rpc_url: &str) -> Self {
        Self {
            client: Client::new(),
            rpc_url: rpc_url.to_string(),
        }
    }

    async fn call_rpc(&self, method: &str) -> Result<Value, String> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": [],
            "id": 1
        });

        let response = self.client
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("RPC connection error: {}", e))?;

        let body: Value = response
            .json()
            .await
            .map_err(|e| format!("Invalid RPC response: {}", e))?;

        if let Some(error) = body.get("error") {
            return Err(error.to_string());
        }

        body.get("result")
            .cloned()
            .ok_or_else(|| "Missing RPC result".to_string())
    }

    async fn get_peer_count(&self) -> Result<u64, String> {
        let result = self.call_rpc("net_peerCount").await?;
        parse_hex(&result)
    }

    async fn get_block_number(&self) -> Result<u64, String> {
        let result = self.call_rpc("eth_blockNumber").await?;
        parse_hex(&result)
    }

    async fn get_sync_status(&self) -> Result<bool, String> {
        let result = self.call_rpc("eth_syncing").await?;

        // false means the node is fully synchronized.
        Ok(result == Value::Bool(false))
    }

    async fn get_network_status(&self) -> Result<bool, String> {
        let result = self.call_rpc("net_listening").await?;

        result.as_bool()
            .ok_or_else(|| "Invalid net_listening result".to_string())
    }

    async fn get_admin_peers_count(&self) -> Result<u64, String> {
        let result = self.call_rpc("admin_peers").await?;

        result.as_array()
            .map(|peers| peers.len() as u64)
            .ok_or_else(|| "Invalid admin_peers result".to_string())
    }
}

fn parse_hex(value: &Value) -> Result<u64, String> {
    let hex = value
        .as_str()
        .ok_or_else(|| "Expected hexadecimal string".to_string())?;

    let clean = hex.strip_prefix("0x").unwrap_or(hex);

    u64::from_str_radix(clean, 16)
        .map_err(|e| format!("Hexadecimal conversion error: {}", e))
}

#[derive(Serialize)]
struct Observation {
    node_id: String,
    timestamp: String,

    // P2P metrics
    peer_count: Option<u64>,
    admin_peer_count: Option<u64>,
    network_listening: Option<bool>,

    // RPC / blockchain metrics
    block_number: Option<u64>,
    is_synced: Option<bool>,
    rpc_available: bool,

    // System metrics
    cpu_usage_percent: f32,
    memory_used_mb: u64,
    memory_total_mb: u64,
}

fn collect_system_metrics(system: &mut System) -> (f32, u64, u64) {
    system.refresh_cpu_usage();
    system.refresh_memory();

    let cpu = system.global_cpu_usage();

    let used_mb = system.used_memory() / 1024 / 1024;
    let total_mb = system.total_memory() / 1024 / 1024;

    (cpu, used_mb, total_mb)
}

fn save_observation(observation: &Observation) {
    let serialized = match serde_json::to_string(observation) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("Serialization error: {}", error);
            return;
        }
    };

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("observations.jsonl");

    match file {
        Ok(mut file) => {
            if let Err(error) = writeln!(file, "{}", serialized) {
                eprintln!("File write error: {}", error);
            }
        }
        Err(error) => eprintln!("File open error: {}", error),
    }

    println!("{}", serialized);
}

#[tokio::main]
async fn main() {
    println!("==================================");
    println!("      EVA-GUARD RUST MONITOR");
    println!("==================================");

    let monitor = GethMonitor::new(RPC_URL);
    let mut system = System::new_all();

    loop {
        let peer_count = monitor.get_peer_count().await.ok();
        let block_number = monitor.get_block_number().await.ok();
        let is_synced = monitor.get_sync_status().await.ok();
        let network_listening = monitor.get_network_status().await.ok();

        // admin_peers may be unavailable if the admin namespace
        // is not enabled on the Geth RPC endpoint.
        let admin_peer_count = monitor.get_admin_peers_count().await.ok();

        let rpc_available =
            peer_count.is_some() && block_number.is_some();

        let (cpu, memory_used, memory_total) =
            collect_system_metrics(&mut system);

        let observation = Observation {
            node_id: NODE_ID.to_string(),
            timestamp: Utc::now().to_rfc3339(),

            peer_count,
            admin_peer_count,
            network_listening,

            block_number,
            is_synced,
            rpc_available,

            cpu_usage_percent: cpu,
            memory_used_mb: memory_used,
            memory_total_mb: memory_total,
        };

        save_observation(&observation);

        sleep(Duration::from_secs(INTERVAL_SECONDS)).await;
    }
}
