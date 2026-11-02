use std::fs;
use std::time::Instant;

#[derive(Default)]
pub struct NetworkMetrics {
    pub rx_bytes_total: Option<u64>,
    pub tx_bytes_total: Option<u64>,
    pub rx_bytes_per_sec: Option<f64>,
    pub tx_bytes_per_sec: Option<f64>,
    pub tcp_established_connections: Option<usize>,
}

pub struct NetworkMonitor {
    previous_sample: Option<(u64, u64, Instant)>,
}

impl NetworkMonitor {
    pub fn new() -> Self {
        Self {
            previous_sample: None,
        }
    }

    // Read received and transmitted bytes from Linux interfaces.
    // Loopback is excluded to focus on external network traffic.
    fn read_network_bytes() -> Result<(u64, u64), String> {
        let content = fs::read_to_string("/proc/net/dev")
            .map_err(|e| format!("Cannot read /proc/net/dev: {}", e))?;

        let mut total_rx = 0u64;
        let mut total_tx = 0u64;

        for line in content.lines().skip(2) {
            let (interface, stats) = line.split_once(':').ok_or("Invalid /proc/net/dev line")?;

            if interface.trim() == "lo" {
                continue;
            }

            let values: Vec<u64> = stats
                .split_whitespace()
                .filter_map(|value| value.parse::<u64>().ok())
                .collect();

            if values.len() < 9 {
                continue;
            }

            // First field: received bytes
            total_rx += values[0];

            // Ninth field: transmitted bytes
            total_tx += values[8];
        }

        Ok((total_rx, total_tx))
    }

    // Count established TCP connections (IPv4 + IPv6).
    fn count_tcp_connections() -> Result<usize, String> {
        let mut count = 0usize;

        for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
            let content =
                fs::read_to_string(path).map_err(|e| format!("Cannot read {}: {}", path, e))?;

            for line in content.lines().skip(1) {
                let fields: Vec<&str> = line.split_whitespace().collect();

                // TCP state is the fourth column.
                // 01 = ESTABLISHED
                if fields.get(3) == Some(&"01") {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    // Collect network metrics and calculate throughput.
    pub fn sample(&mut self) -> Result<NetworkMetrics, String> {
        let (rx_bytes, tx_bytes) = Self::read_network_bytes()?;
        let tcp_connections = Self::count_tcp_connections()?;

        let now = Instant::now();

        let (rx_rate, tx_rate) = match self.previous_sample {
            Some((previous_rx, previous_tx, previous_time)) => {
                let elapsed = now.duration_since(previous_time).as_secs_f64();

                let rx_rate = if elapsed > 0.0 && rx_bytes >= previous_rx {
                    Some((rx_bytes - previous_rx) as f64 / elapsed)
                } else {
                    None
                };

                let tx_rate = if elapsed > 0.0 && tx_bytes >= previous_tx {
                    Some((tx_bytes - previous_tx) as f64 / elapsed)
                } else {
                    None
                };

                (rx_rate, tx_rate)
            }

            None => (None, None),
        };

        self.previous_sample = Some((rx_bytes, tx_bytes, now));

        Ok(NetworkMetrics {
            rx_bytes_total: Some(rx_bytes),
            tx_bytes_total: Some(tx_bytes),
            rx_bytes_per_sec: rx_rate,
            tx_bytes_per_sec: tx_rate,
            tcp_established_connections: Some(tcp_connections),
        })
    }
}
