mod network_monitor;

use chrono::Utc;
use network_monitor::{NetworkMetrics, NetworkMonitor};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::time::sleep;

// ==========================================
// CONFIGURATION
// ==========================================

const RPC_URL: &str = "http://127.0.0.1:8545";
const NODE_ID: &str = "geth-node-1";
const INTERVAL_SECONDS: u64 = 5;
const OUTPUT_FILE: &str = "observations.jsonl";

// ==========================================
// GETH MONITOR
// ==========================================

struct GethMonitor {
    client: Client,
    rpc_url: String,
}

impl GethMonitor {
    fn new(rpc_url: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            rpc_url: rpc_url.to_string(),
        }
    }

    // Generic JSON-RPC request
    async fn call_rpc(&self, method: &str) -> Result<Value, String> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": [],
            "id": 1
        });

        let response = self
            .client
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("RPC connection error: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()));
        }

        let body: Value = response
            .json()
            .await
            .map_err(|e| format!("Invalid RPC response: {}", e))?;

        if let Some(error) = body.get("error") {
            return Err(format!("RPC error: {}", error));
        }

        body.get("result")
            .cloned()
            .ok_or_else(|| "Missing RPC result".to_string())
    }

    // Get connected peer count
    async fn get_peer_count(&self) -> Result<u64, String> {
        let result = self.call_rpc("net_peerCount").await?;
        parse_hex(&result)
    }

    // Get peer identities
    async fn get_peer_ids(&self) -> Result<HashSet<String>, String> {
        let result = self.call_rpc("admin_peers").await?;

        let peers = result.as_array().ok_or("Invalid admin_peers result")?;

        let mut peer_ids = HashSet::new();

        for peer in peers {
            if let Some(id) = peer["id"].as_str() {
                peer_ids.insert(id.to_string());
            }
        }

        Ok(peer_ids)
    }

    // Get blockchain block number
    async fn get_block_number(&self) -> Result<u64, String> {
        let result = self.call_rpc("eth_blockNumber").await?;
        parse_hex(&result)
    }

    // Get synchronization status
    async fn get_sync_status(&self) -> Result<bool, String> {
        let result = self.call_rpc("eth_syncing").await?;

        if result == Value::Bool(false) {
            return Ok(true);
        }

        if result.is_object() {
            return Ok(false);
        }

        Err("Invalid eth_syncing result".to_string())
    }

    // Check whether Geth is listening for P2P connections
    async fn get_network_status(&self) -> Result<bool, String> {
        let result = self.call_rpc("net_listening").await?;

        result
            .as_bool()
            .ok_or_else(|| "Invalid net_listening result".to_string())
    }

    // Measure HTTP JSON-RPC response latency
    async fn measure_rpc_latency(&self) -> Option<f64> {
        let start = Instant::now();

        match self.call_rpc("net_peerCount").await {
            Ok(_) => Some(start.elapsed().as_secs_f64() * 1000.0),
            Err(error) => {
                eprintln!("RPC latency error: {}", error);
                None
            }
        }
    }
}

// ==========================================
// HEX PARSER
// ==========================================

fn parse_hex(value: &Value) -> Result<u64, String> {
    let hex = value
        .as_str()
        .ok_or_else(|| "Expected hexadecimal string".to_string())?;

    let clean = hex.strip_prefix("0x").unwrap_or(hex);

    u64::from_str_radix(clean, 16).map_err(|e| format!("Hexadecimal conversion error: {}", e))
}

// ==========================================
// PEER CHURN DETECTION
// ==========================================

fn detect_peer_churn(
    previous: &HashSet<String>,
    current: &HashSet<String>,
) -> (Vec<String>, Vec<String>) {
    let mut new_peers: Vec<String> = current.difference(previous).cloned().collect();

    let mut disconnected_peers: Vec<String> = previous.difference(current).cloned().collect();

    new_peers.sort();
    disconnected_peers.sort();

    (new_peers, disconnected_peers)
}

// ==========================================
// OBSERVATION STRUCTURE
// ==========================================

#[derive(Serialize)]
struct Observation {
    node_id: String,
    timestamp: String,

    // P2P metrics
    peer_count: Option<u64>,
    admin_peer_count: Option<u64>,
    peer_details_available: bool,
    new_peers: Vec<String>,
    disconnected_peers: Vec<String>,
    peer_churn_count: usize,
    network_listening: Option<bool>,

    // Blockchain metrics
    block_number: Option<u64>,
    is_synced: Option<bool>,

    // RPC metrics
    rpc_available: bool,
    rpc_latency_ms: Option<f64>,

    // System metrics
    cpu_usage_percent: f32,
    memory_used_mb: u64,
    memory_total_mb: u64,

    // Network metrics
    network_rx_bytes_total: Option<u64>,
    network_tx_bytes_total: Option<u64>,
    network_rx_bytes_per_sec: Option<f64>,
    network_tx_bytes_per_sec: Option<f64>,
    tcp_established_connections: Option<usize>,
}

// ==========================================
// SYSTEM MONITORING
// ==========================================

fn collect_system_metrics(system: &mut System) -> (f32, u64, u64) {
    system.refresh_cpu_usage();
    system.refresh_memory();

    let cpu = system.global_cpu_usage();

    let used_mb = system.used_memory() / (1024 * 1024);
    let total_mb = system.total_memory() / (1024 * 1024);

    (cpu, used_mb, total_mb)
}

// ==========================================
// SAVE JSONL OBSERVATIONS
// ==========================================

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
        .open(OUTPUT_FILE);

    match file {
        Ok(mut file) => {
            if let Err(error) = writeln!(file, "{}", serialized) {
                eprintln!("File write error: {}", error);
            } else {
                println!("Observation saved.");
            }
        }

        Err(error) => {
            eprintln!("File open error: {}", error);
        }
    }

    println!("{}", serialized);
}

// ==========================================
// MAIN
// ==========================================

#[tokio::main]
async fn main() {
    println!("==================================");
    println!("      EVA-GUARD RUST MONITOR");
    println!("==================================");

    println!("Geth RPC: {}", RPC_URL);

    let monitor = GethMonitor::new(RPC_URL);

    let mut system = System::new_all();

    let mut network_monitor = NetworkMonitor::new();

    // Peer history: None means no successful baseline yet.
    let mut previous_peer_ids: Option<HashSet<String>> = None;

    loop {
        println!("\n========== NEW MONITORING CYCLE ==========");

        // ----------------------------------
        // 1. GETH METRICS
        // ----------------------------------

        let peer_count = monitor.get_peer_count().await.ok();

        let block_number = monitor.get_block_number().await.ok();

        let is_synced = monitor.get_sync_status().await.ok();

        let network_listening = monitor.get_network_status().await.ok();

        // ----------------------------------
        // 2. PEER CHURN
        // ----------------------------------

        let mut new_peers = Vec::new();
        let mut disconnected_peers = Vec::new();

        let current_peer_ids = monitor.get_peer_ids().await.ok();

        let peer_details_available = current_peer_ids.is_some();

        let admin_peer_count = current_peer_ids.as_ref().map(|peers| peers.len() as u64);

        if let Some(current) = current_peer_ids {
            if let Some(previous) = &previous_peer_ids {
                let (new, disconnected) = detect_peer_churn(previous, &current);

                new_peers = new;
                disconnected_peers = disconnected;
            } else {
                println!("Initial peer snapshot: {} peers", current.len());
            }

            // Update history only after a successful RPC call.
            previous_peer_ids = Some(current);
        } else {
            eprintln!("Could not retrieve peer identities.");
        }

        let peer_churn_count = new_peers.len() + disconnected_peers.len();

        println!("New peers: {}", new_peers.len());
        println!("Disconnected peers: {}", disconnected_peers.len());

        // ----------------------------------
        // 3. SYSTEM METRICS
        // ----------------------------------

        let (cpu, memory_used, memory_total) = collect_system_metrics(&mut system);

        // ----------------------------------
        // 4. NETWORK METRICS
        // ----------------------------------

        let network_metrics: NetworkMetrics = match network_monitor.sample() {
            Ok(metrics) => metrics,
            Err(error) => {
                eprintln!("Network monitoring error: {}", error);
                NetworkMetrics::default()
            }
        };

        println!("\n========== NETWORK MONITORING ==========");

        println!("RX total: {:?} bytes", network_metrics.rx_bytes_total);

        println!("TX total: {:?} bytes", network_metrics.tx_bytes_total);

        println!(
            "RX throughput: {:?} bytes/sec",
            network_metrics.rx_bytes_per_sec
        );

        println!(
            "TX throughput: {:?} bytes/sec",
            network_metrics.tx_bytes_per_sec
        );

        println!(
            "Established TCP connections: {:?}",
            network_metrics.tcp_established_connections
        );

        // ----------------------------------
        // 5. RPC LATENCY
        // ----------------------------------

        let rpc_latency_ms = monitor.measure_rpc_latency().await;

        println!("RPC latency: {:?} ms", rpc_latency_ms);

        // ----------------------------------
        // 6. RPC AVAILABILITY
        // ----------------------------------

        let rpc_available = peer_count.is_some() || block_number.is_some();

        // ----------------------------------
        // 7. BUILD OBSERVATION
        // ----------------------------------

        let observation = Observation {
            node_id: NODE_ID.to_string(),
            timestamp: Utc::now().to_rfc3339(),

            // P2P
            peer_count,
            admin_peer_count,
            peer_details_available,
            new_peers,
            disconnected_peers,
            peer_churn_count,
            network_listening,

            // Blockchain
            block_number,
            is_synced,

            // RPC
            rpc_available,
            rpc_latency_ms,

            // System
            cpu_usage_percent: cpu,
            memory_used_mb: memory_used,
            memory_total_mb: memory_total,

            // Network
            network_rx_bytes_total: network_metrics.rx_bytes_total,

            network_tx_bytes_total: network_metrics.tx_bytes_total,

            network_rx_bytes_per_sec: network_metrics.rx_bytes_per_sec,

            network_tx_bytes_per_sec: network_metrics.tx_bytes_per_sec,

            tcp_established_connections: network_metrics.tcp_established_connections,
        };

        // ----------------------------------
        // 8. SAVE OBSERVATION
        // ----------------------------------

        save_observation(&observation);

        // ----------------------------------
        // 9. WAIT
        // ----------------------------------

        sleep(Duration::from_secs(INTERVAL_SECONDS)).await;
    }
}
