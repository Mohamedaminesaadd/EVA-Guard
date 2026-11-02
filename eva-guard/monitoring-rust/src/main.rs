
use chrono::Utc;
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use sysinfo::System;

// ==========================================
// CONFIGURATION
// ==========================================

const RPC_URL: &str = "http://127.0.0.1:8545";
const NODE_ID: &str = "geth-node-1";
const COLLECTION_INTERVAL: u64 = 5;
const OUTPUT_FILE: &str = "observations.jsonl";

// ==========================================
// OBSERVATION STRUCTURE
// ==========================================

#[derive(Serialize)]
struct Observation {
    node_id: String,
    timestamp: String,

    // P2P metrics
    peer_count: Option<u64>,
    admin_peer_count: Option<usize>,
    peer_details_available: bool,
    new_peers: Vec<String>,
    disconnected_peers: Vec<String>,
    peer_churn_count: usize,

    // Network metrics
    network_listening: Option<bool>,

    // Blockchain metrics
    block_number: Option<u64>,
    is_synced: Option<bool>,

    // RPC metrics
    rpc_available: bool,

    // System metrics
    cpu_usage_percent: f32,
    memory_used_mb: u64,
    memory_total_mb: u64,
}

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

    // --------------------------------------
    // Generic JSON-RPC call
    // --------------------------------------

    async fn call_rpc(&self, method: &str) -> Result<Value, String> {
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": [],
            "id": 1
        });

        let response = self.client
            .post(&self.rpc_url)
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            return Err(format!(
                "HTTP error: {}",
                response.status()
            ));
        }

        let body: Value = response
            .json()
            .await
            .map_err(|e| format!("Invalid JSON response: {}", e))?;

        if let Some(error) = body.get("error") {
            return Err(format!("RPC error: {}", error));
        }

        body.get("result")
            .cloned()
            .ok_or_else(|| "Missing RPC result".to_string())
    }

    // --------------------------------------
    // Convert hexadecimal value to u64
    // --------------------------------------

    fn hex_to_u64(value: &Value) -> Result<u64, String> {
        let hex = value
            .as_str()
            .ok_or("Expected hexadecimal string")?;

        let hex = hex
            .strip_prefix("0x")
            .unwrap_or(hex);

        u64::from_str_radix(hex, 16)
            .map_err(|e| format!("Hex conversion failed: {}", e))
    }

    // --------------------------------------
    // Get peer count
    // --------------------------------------

    async fn get_peer_count(&self) -> Result<u64, String> {
        let result = self.call_rpc("net_peerCount").await?;
        Self::hex_to_u64(&result)
    }

    // --------------------------------------
    // Get peer identities
    // --------------------------------------

    async fn get_peer_ids(&self) -> Result<HashSet<String>, String> {
        let result = self.call_rpc("admin_peers").await?;

        let peers = result
            .as_array()
            .ok_or("Invalid admin_peers response")?;

        let mut peer_ids = HashSet::new();

        for peer in peers {
            if let Some(id) = peer["id"].as_str() {
                peer_ids.insert(id.to_string());
            }
        }

        Ok(peer_ids)
    }

    // --------------------------------------
    // Get network listening status
    // --------------------------------------

    async fn get_network_status(&self) -> Result<bool, String> {
        let result = self.call_rpc("net_listening").await?;

        result
            .as_bool()
            .ok_or_else(|| "Invalid net_listening response".to_string())
    }

    // --------------------------------------
    // Get blockchain block number
    // --------------------------------------

    async fn get_block_number(&self) -> Result<u64, String> {
        let result = self.call_rpc("eth_blockNumber").await?;
        Self::hex_to_u64(&result)
    }

    // --------------------------------------
    // Get synchronization status
    // --------------------------------------

    async fn get_sync_status(&self) -> Result<bool, String> {
        let result = self.call_rpc("eth_syncing").await?;

        // false means the node is synchronized
        if result == Value::Bool(false) {
            return Ok(true);
        }

        // An object means synchronization is ongoing
        if result.is_object() {
            return Ok(false);
        }

        Err("Invalid eth_syncing response".to_string())
    }
}

// ==========================================
// PEER CHURN DETECTOR
// ==========================================

fn detect_peer_churn(
    previous: &HashSet<String>,
    current: &HashSet<String>,
) -> (Vec<String>, Vec<String>) {

    let mut new_peers: Vec<String> = current
        .difference(previous)
        .cloned()
        .collect();

    let mut disconnected_peers: Vec<String> = previous
        .difference(current)
        .cloned()
        .collect();

    new_peers.sort();
    disconnected_peers.sort();

    (new_peers, disconnected_peers)
}

// ==========================================
// SAVE OBSERVATION TO JSONL
// ==========================================

fn save_observation(observation: &Observation) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(OUTPUT_FILE)
        .map_err(|e| format!("File opening failed: {}", e))?;

    let json_line = serde_json::to_string(observation)
        .map_err(|e| format!("Serialization failed: {}", e))?;

    writeln!(file, "{}", json_line)
        .map_err(|e| format!("File writing failed: {}", e))?;

    Ok(())
}

// ==========================================
// MAIN
// ==========================================

#[tokio::main]
async fn main() {
    println!("==================================");
    println!("       EVA-GUARD RUST MONITOR");
    println!("==================================");

    println!("Connecting to Geth RPC: {}", RPC_URL);

    let monitor = GethMonitor::new(RPC_URL);

    // System monitoring
    let mut system = System::new_all();

    // Peer history for churn detection
    let mut previous_peer_ids: HashSet<String> = HashSet::new();
    let mut first_snapshot = true;

    loop {
        // ==================================
        // 1. COLLECT GETH METRICS
        // ==================================

        let peer_count = match monitor.get_peer_count().await {
            Ok(value) => Some(value),
            Err(error) => {
                eprintln!("Peer count error: {}", error);
                None
            }
        };

        let network_listening = match monitor.get_network_status().await {
            Ok(value) => Some(value),
            Err(error) => {
                eprintln!("Network status error: {}", error);
                None
            }
        };

        let block_number = match monitor.get_block_number().await {
            Ok(value) => Some(value),
            Err(error) => {
                eprintln!("Block number error: {}", error);
                None
            }
        };

        let is_synced = match monitor.get_sync_status().await {
            Ok(value) => Some(value),
            Err(error) => {
                eprintln!("Sync status error: {}", error);
                None
            }
        };

        // ==================================
        // 2. DETECT PEER CHURN
        // ==================================

        let mut new_peers = Vec::new();
        let mut disconnected_peers = Vec::new();

        let current_peer_ids = match monitor.get_peer_ids().await {
            Ok(current) => {
                if first_snapshot {
                    println!(
                        "Initial peer snapshot: {}",
                        current.len()
                    );

                    first_snapshot = false;
                } else {
                    let (new, disconnected) =
                        detect_peer_churn(&previous_peer_ids, &current);

                    new_peers = new;
                    disconnected_peers = disconnected;
                }

                // Update history only after a successful query
                previous_peer_ids = current.clone();

                Some(current)
            }

            Err(error) => {
                eprintln!("Peer monitoring error: {}", error);
                None
            }
        };

        let peer_details_available = current_peer_ids.is_some();

        let admin_peer_count = current_peer_ids
            .as_ref()
            .map(|peers| peers.len());

        let peer_churn_count =
            new_peers.len() + disconnected_peers.len();

        println!("\n========== PEER CHURN ==========");
        println!("New peers: {}", new_peers.len());
        println!(
            "Disconnected peers: {}",
            disconnected_peers.len()
        );

        for peer in &new_peers {
            println!("NEW PEER: {}", peer);
        }

        for peer in &disconnected_peers {
            println!("DISCONNECTED PEER: {}", peer);
        }

        // ==================================
        // 3. COLLECT SYSTEM METRICS
        // ==================================

        system.refresh_cpu_usage();
        system.refresh_memory();

        let cpu_usage_percent = system.global_cpu_usage();

        // sysinfo reports memory in bytes
        let memory_used_mb = system.used_memory() / (1024 * 1024);
        let memory_total_mb = system.total_memory() / (1024 * 1024);

        // ==================================
        // 4. RPC AVAILABILITY
        // ==================================

        let rpc_available = peer_count.is_some()
            || block_number.is_some()
            || network_listening.is_some();

        // ==================================
        // 5. BUILD OBSERVATION
        // ==================================

        let observation = Observation {
            node_id: NODE_ID.to_string(),
            timestamp: Utc::now().to_rfc3339(),

            peer_count,
            admin_peer_count,
            peer_details_available,
            new_peers,
            disconnected_peers,
            peer_churn_count,

            network_listening,

            block_number,
            is_synced,

            rpc_available,

            cpu_usage_percent,
            memory_used_mb,
            memory_total_mb,
        };

        // ==================================
        // 6. DISPLAY OBSERVATION
        // ==================================

        match serde_json::to_string_pretty(&observation) {
            Ok(json) => println!("\n{}", json),
            Err(error) => eprintln!("Display error: {}", error),
        }

        // ==================================
        // 7. SAVE OBSERVATION
        // ==================================

        if let Err(error) = save_observation(&observation) {
            eprintln!("Saving error: {}", error);
        } else {
            println!("Observation saved to {}", OUTPUT_FILE);
        }

        // ==================================
        // 8. WAIT BEFORE NEXT COLLECTION
        // ==================================

        tokio::time::sleep(
            Duration::from_secs(COLLECTION_INTERVAL)
        ).await;
    }
}
