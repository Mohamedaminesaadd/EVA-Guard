
# =========================
# Rust / Cargo
# =========================
**/target/
**/Cargo.lock

# Keep Cargo.lock for applications if you want reproducible builds
# Uncomment the next line if you want to track Cargo.lock:
# !**/Cargo.lock

# Rust generated files
**/*.rs.bk

# =========================
# Monitoring outputs
# =========================
**/observations.jsonl
**/*.log
**/logs/

# =========================
# Environment variables
# =========================
.env
.env.*
!.env.example

# =========================
# IDE
# =========================
.vscode/
.idea/
*.swp
*.swo

# =========================
# OS
# =========================
.DS_Store
Thumbs.db
