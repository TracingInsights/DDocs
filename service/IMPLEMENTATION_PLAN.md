# F1 Document Scraper - Continuous Monitoring Implementation Plan

> **Goal**: Deploy a continuous monitoring service on Hetzner VPS (4GB RAM, 2vCPU) that checks for new F1 documents every 1-2 minutes, processes them immediately, and commits directly to GitHub.

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Component Specifications](#2-component-specifications)
3. [Systemd Configuration](#3-systemd-configuration)
4. [Setup Script](#4-setup-script)
5. [Error Handling & Recovery](#5-error-handling--recovery)
6. [Monitoring & Observability](#6-monitoring--observability)
7. [Deployment Checklist](#7-deployment-checklist)
8. [Maintenance & Operations](#8-maintenance--operations)
9. [Security Considerations](#9-security-considerations)
10. [Success Metrics](#10-success-metrics)
11. [Future Enhancements](#11-future-enhancements)

---

## 1. Project Structure

```
service/
├── README.md                      # Deployment guide and documentation
├── config.example.yaml            # Configuration template (committed)
├── .gitignore                     # Ignore config.yaml and logs
├── monitor.py                     # Main monitoring orchestrator
├── resource_tracker.py            # Resource monitoring with psutil
├── notifier.py                    # Telegram notification handler
├── stats.py                       # Metrics analysis and dashboard
├── requirements.txt               # Python dependencies (psutil, pyyaml, requests)
├── systemd/
│   ├── f1-scraper.service        # Systemd service unit
│   ├── f1-scraper.timer          # Systemd timer (every 2 minutes)
│   └── secrets.env.example       # Environment variables template
└── setup.sh                       # Automated VPS setup script
```

---

## 2. Component Specifications

### 2.1 Configuration (`config.example.yaml`)

```yaml
# F1 Document Scraper - Service Configuration
# Copy to config.yaml and customize for your deployment

monitoring:
  check_interval_seconds: 120        # 2 minutes
  lock_timeout_seconds: 300          # 5 minutes max run time
  
scraping:
  output_dir: "documents"
  extraction_limit: 200              # Match GH Actions
  force_refresh: true                # Bypass discovery cache for immediate detection
  
git:
  repo_url: "https://github.com/TracingInsights/DDocs.git"
  branch: "main"
  commit_user: "f1-scraper-bot"
  commit_email: "bot@tracinginsights.com"
  
notifications:
  enabled: true
  failure_threshold: 5               # Alert after 5 consecutive failures (~10 min)
  telegram:
    enabled: true
    # Token and chat_id loaded from environment variables
    
resource_monitoring:
  enabled: true
  sample_interval_seconds: 10       # Sample CPU/memory every 10 seconds
  metrics_file: "logs/metrics.jsonl" # Append-only metrics log
  retention_days: 30
  
logging:
  level: "INFO"                      # DEBUG, INFO, WARNING, ERROR
  file: "logs/scraper.log"
  max_size_mb: 100
  backup_count: 5
```

### 2.2 Main Monitor (`monitor.py`)

**Responsibilities:**
- Orchestrate the scraping pipeline
- Manage file locks (flock) for concurrency control
- Handle temp directory lifecycle
- Execute git operations with sparse checkout
- Track resource usage via `resource_tracker.py`
- Handle errors with exponential backoff
- Send notifications on persistent failures

**Key Functions:**
```python
async def main():
    """Main entry point - called by systemd timer"""
    
async def acquire_lock():
    """Acquire flock on /tmp/f1-scraper.lock with timeout"""
    
async def run_scraping_pipeline():
    """Execute: scraper.py → transcript_scraper.py → extract.py"""
    
async def manage_temp_directory():
    """Create temp dir, yield, cleanup on success"""
    
async def git_operations():
    """Sparse checkout → disable → add → commit → push → re-enable"""
    
async def handle_failure():
    """Track failures, exponential backoff, notify on threshold"""
```

**Workflow:**
1. Acquire file lock with timeout (exit if locked)
2. Validate PID in lock file (clean stale locks)
3. Start resource monitoring
4. Create temp directory: `/tmp/f1-scraper-{timestamp}/`
5. Run scraper.py with `--force-refresh --output-dir /tmp/...`
6. Run transcript_scraper.py with `--super-aggressive --output-dir /tmp/...`
7. Copy new files from temp to repo working tree
8. Run extract.py with `--limit 200`
9. Git sparse-checkout disable
10. Git add documents/ extracted/
11. Git commit with formatted message
12. Git push with verification
13. Git sparse-checkout re-enable
14. Delete temp directory
15. Log metrics and release lock

### 2.3 Resource Tracker (`resource_tracker.py`)

**Responsibilities:**
- Sample CPU, memory, disk I/O, network I/O every 10 seconds
- Track peak and average usage per run
- Write metrics to JSONL file
- Provide context manager for easy integration

**Metrics Collected:**
```python
{
    "timestamp": "2026-05-19T10:30:00Z",
    "run_id": "uuid",
    "duration_seconds": 45.2,
    "cpu_percent_avg": 35.5,
    "cpu_percent_peak": 78.2,
    "memory_mb_avg": 450.3,
    "memory_mb_peak": 680.1,
    "disk_read_mb": 12.5,
    "disk_write_mb": 8.3,
    "network_sent_mb": 15.2,
    "network_recv_mb": 45.8,
    "success": true,
    "new_documents": 3,
    "new_transcripts": 2,
    "extracted_count": 5,
    "error": null
}
```

**Usage:**
```python
async with ResourceTracker(config) as tracker:
    result = await run_scraping_pipeline()
    tracker.set_result(result)
```

### 2.4 Notifier (`notifier.py`)

**Responsibilities:**
- Send Telegram messages via Bot API
- Track consecutive failure count
- Format rich notifications with markdown
- Handle rate limiting

**Notification Types:**

1. **Persistent Failure Alert** (after 5 failures)
   ```
   🚨 F1 Scraper Alert
   
   Status: 5 consecutive failures
   Duration: ~10 minutes
   Last Error: Connection timeout to FIA website
   
   Action Required: Check VPS and FIA website status
   ```

2. **Recovery Notification** (after failures resolve)
   ```
   ✅ F1 Scraper Recovered
   
   Status: Back online
   Downtime: 12 minutes
   New Documents: 2 found after recovery
   ```

3. **Daily Summary** (optional, configurable)
   ```
   📊 F1 Scraper Daily Report
   
   Runs: 720 (every 2 min)
   Success Rate: 99.7%
   New Documents: 15
   Avg CPU: 25%, Avg Memory: 380MB
   ```

### 2.5 Stats Dashboard (`stats.py`)

**Responsibilities:**
- Parse metrics.jsonl
- Generate reports and summaries
- Identify optimization opportunities

**CLI Commands:**
```bash
# Last 24 hours summary
uv run python service/stats.py --last-24h

# Last 7 days trend
uv run python service/stats.py --last-7d

# Resource usage analysis
uv run python service/stats.py --resources

# Failure analysis
uv run python service/stats.py --failures

# Export to CSV
uv run python service/stats.py --export metrics.csv
```

**Output Example:**
```
F1 Scraper Statistics (Last 24 Hours)
=====================================
Total Runs:        720
Successful:        718 (99.7%)
Failed:            2 (0.3%)

Documents Found:   12
Transcripts:       3
Extracted PDFs:    15

Resource Usage:
  CPU (avg):       28.5%
  CPU (peak):      85.2%
  Memory (avg):    420 MB
  Memory (peak):   780 MB
  Network (avg):   18.5 MB/run

Avg Run Duration:  38.2 seconds
Recommendation:    Current 2-minute interval is optimal
```

---

## 3. Systemd Configuration

### 3.1 Service Unit (`f1-scraper.service`)

```ini
[Unit]
Description=F1 Document Scraper Service
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=%YOUR_USERNAME%
WorkingDirectory=/home/%YOUR_USERNAME%/DDocs
EnvironmentFile=/home/%YOUR_USERNAME%/DDocs/service/secrets.env

# Use uv to run the monitor script
ExecStart=/home/%YOUR_USERNAME%/.local/bin/uv run python service/monitor.py

# Timeout after 5 minutes (should never hit this with proper locking)
TimeoutStartSec=300

# Logging
StandardOutput=append:/home/%YOUR_USERNAME%/DDocs/service/logs/systemd.log
StandardError=append:/home/%YOUR_USERNAME%/DDocs/service/logs/systemd-error.log

# Resource limits (4GB RAM VPS)
MemoryMax=1G
CPUQuota=150%

# Don't restart on failure - let timer handle next run
Restart=no

[Install]
WantedBy=multi-user.target
```

### 3.2 Timer Unit (`f1-scraper.timer`)

```ini
[Unit]
Description=F1 Document Scraper Timer (Every 2 Minutes)
Requires=f1-scraper.service

[Timer]
# Run every 2 minutes
OnBootSec=1min
OnUnitActiveSec=2min

# Prevent overlapping runs
Unit=f1-scraper.service

# Randomize by up to 10 seconds to avoid thundering herd
RandomizedDelaySec=10

# Persistent timer (catch up missed runs after reboot)
Persistent=true

[Install]
WantedBy=timers.target
```

### 3.3 Secrets Environment (`secrets.env.example`)

```bash
# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here

# Optional: Override config values
# SCRAPER_CHECK_INTERVAL=120
# SCRAPER_EXTRACTION_LIMIT=200
```

---

## 4. Setup Script (`setup.sh`)

**Automated VPS deployment script:**

```bash
#!/bin/bash
set -e

echo "🚀 F1 Document Scraper - VPS Setup"
echo "===================================="

# 1. Check prerequisites
command -v git >/dev/null 2>&1 || { echo "❌ git not found"; exit 1; }
command -v uv >/dev/null 2>&1 || { echo "❌ uv not found. Install: curl -LsSf https://astral.sh/uv/install.sh | sh"; exit 1; }

# 2. Clone repository with sparse checkout
echo "📦 Cloning repository..."
if [ ! -d "$HOME/DDocs" ]; then
    git clone --depth=1 --filter=blob:none --sparse https://github.com/TracingInsights/DDocs.git "$HOME/DDocs"
    cd "$HOME/DDocs"
    git sparse-checkout set scraper.py transcript_scraper.py extract.py config.py shared_utils.py pyproject.toml uv.lock service/ documents/manifest.json documents/discovery_cache.json documents/transcript_discovery_cache.json extracted/manifest.json
else
    echo "✅ Repository already exists"
    cd "$HOME/DDocs"
    git pull
fi

# 3. Install system dependencies
echo "📦 Installing system dependencies..."
sudo apt-get update
sudo apt-get install -y tesseract-ocr pandoc flock

# 4. Install Python dependencies
echo "🐍 Installing Python dependencies..."
uv sync

# 5. Create service directories
echo "📁 Creating directories..."
mkdir -p service/logs
mkdir -p /tmp/f1-scraper-temp

# 6. Copy and configure
echo "⚙️  Setting up configuration..."
if [ ! -f "service/config.yaml" ]; then
    cp service/config.example.yaml service/config.yaml
    echo "✏️  Edit service/config.yaml with your settings"
fi

if [ ! -f "service/secrets.env" ]; then
    cp service/systemd/secrets.env.example service/secrets.env
    echo "✏️  Edit service/secrets.env with your Telegram credentials"
fi

# 7. Install systemd units
echo "🔧 Installing systemd units..."
sed "s/%YOUR_USERNAME%/$USER/g" service/systemd/f1-scraper.service > /tmp/f1-scraper.service
sed "s/%YOUR_USERNAME%/$USER/g" service/systemd/f1-scraper.timer > /tmp/f1-scraper.timer

sudo cp /tmp/f1-scraper.service /etc/systemd/system/
sudo cp /tmp/f1-scraper.timer /etc/systemd/system/
sudo systemctl daemon-reload

# 8. Enable and start timer
echo "▶️  Enabling timer..."
sudo systemctl enable f1-scraper.timer
sudo systemctl start f1-scraper.timer

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit service/config.yaml"
echo "2. Edit service/secrets.env with Telegram credentials"
echo "3. Test manually: uv run python service/monitor.py"
echo "4. Check timer status: systemctl status f1-scraper.timer"
echo "5. View logs: journalctl -u f1-scraper.service -f"
echo "6. View metrics: uv run python service/stats.py --last-24h"
```

---

## 5. Error Handling & Recovery

### 5.1 Failure Scenarios

| Scenario | Detection | Recovery | Notification |
|----------|-----------|----------|--------------|
| FIA website down | HTTP timeout/500 | Retry with backoff (1s, 2s, 4s) | After 5 failures |
| Git push conflict | Push rejected | Pull with rebase, retry push | After 3 failures |
| Disk full | OSError on write | Clean old logs, alert immediately | Immediate |
| Memory exhausted | OOM killer | Systemd restart, reduce limits | Immediate |
| Lock timeout | Flock timeout | Skip run, log warning | After 5 skips |
| Extraction failure | extract.py error | Continue without extraction | Log only |
| Network partition | All requests fail | Exponential backoff up to 5 min | After 5 failures |

### 5.2 Exponential Backoff

```python
async def retry_with_backoff(func, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt  # 1s, 2s, 4s
            await asyncio.sleep(wait)
```

### 5.3 Graceful Degradation

- If scraper.py fails, still try transcript_scraper.py
- If transcript_scraper.py fails, still try extract.py
- If extraction fails, still commit PDFs
- If git push fails, keep files for next run

---

## 6. Monitoring & Observability

### 6.1 Log Files

```
service/logs/
├── scraper.log              # Application logs (rotated)
├── systemd.log              # Systemd stdout
├── systemd-error.log        # Systemd stderr
└── metrics.jsonl            # Resource metrics (append-only)
```

### 6.2 Systemd Commands

```bash
# Check timer status
systemctl status f1-scraper.timer

# Check service status
systemctl status f1-scraper.service

# View recent logs
journalctl -u f1-scraper.service -n 50

# Follow logs in real-time
journalctl -u f1-scraper.service -f

# Restart timer
sudo systemctl restart f1-scraper.timer

# Stop timer
sudo systemctl stop f1-scraper.timer

# Manual trigger
sudo systemctl start f1-scraper.service
```

### 6.3 Health Checks

```bash
# Check if timer is active
systemctl is-active f1-scraper.timer

# Check last run time
systemctl show f1-scraper.timer | grep LastTrigger

# Check next run time
systemctl show f1-scraper.timer | grep NextElapseUSecRealtime

# Check lock file
ls -lh /tmp/f1-scraper.lock

# Check metrics
tail -n 20 service/logs/metrics.jsonl | jq
```

---

## 7. Deployment Checklist

### 7.1 Pre-Deployment (Local)

- [ ] Create `service/` folder structure
- [ ] Implement `monitor.py`
- [ ] Implement `resource_tracker.py`
- [ ] Implement `notifier.py`
- [ ] Implement `stats.py`
- [ ] Create systemd units
- [ ] Create `setup.sh`
- [ ] Test locally in WSL
- [ ] Update `.gitignore` (add `service/config.yaml`, `service/secrets.env`, `service/logs/`)
- [ ] Commit and push to GitHub

### 7.2 VPS Deployment

- [ ] SSH into Hetzner VPS
- [ ] Install uv: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- [ ] Authenticate with GitHub: `gh auth login`
- [ ] Download and run setup script
- [ ] Edit `service/config.yaml`
- [ ] Create Telegram bot (talk to @BotFather)
- [ ] Edit `service/secrets.env` with Telegram credentials
- [ ] Test manual run: `uv run python service/monitor.py`
- [ ] Verify git push works
- [ ] Enable timer: `sudo systemctl enable --now f1-scraper.timer`
- [ ] Monitor first few runs: `journalctl -u f1-scraper.service -f`

### 7.3 Post-Deployment

- [ ] Verify timer is running: `systemctl status f1-scraper.timer`
- [ ] Check for new documents in GitHub repo
- [ ] Monitor resource usage: `uv run python service/stats.py --last-24h`
- [ ] Test failure notification (temporarily break config)
- [ ] Set up daily summary notification (optional)
- [ ] Document VPS access and credentials
- [ ] Add monitoring to your ops dashboard

---

## 8. Maintenance & Operations

### 8.1 Regular Maintenance

**Weekly:**
- Review metrics: `uv run python service/stats.py --last-7d`
- Check disk usage: `df -h`
- Review failure logs

**Monthly:**
- Rotate old logs (automated by config)
- Update dependencies: `uv sync --upgrade`
- Review and adjust check interval based on metrics

**Quarterly:**
- Review Telegram bot token expiry
- Audit git credentials
- Review resource limits

### 8.2 Troubleshooting

**Problem: No new documents detected**
```bash
# Check if timer is running
systemctl status f1-scraper.timer

# Check recent runs
journalctl -u f1-scraper.service -n 20

# Manual test
uv run python scraper.py --force-refresh
```

**Problem: High memory usage**
```bash
# Check metrics
uv run python service/stats.py --resources

# Reduce extraction limit in config.yaml
# Restart timer
sudo systemctl restart f1-scraper.timer
```

**Problem: Git push failures**
```bash
# Check git status
cd ~/DDocs && git status

# Check remote
git remote -v

# Test push manually
git push origin main

# Re-authenticate if needed
gh auth login
```

---

## 9. Security Considerations

### 9.1 Secrets Management
- Never commit `secrets.env` or `config.yaml`
- Use environment variables for sensitive data
- Restrict file permissions: `chmod 600 service/secrets.env`

### 9.2 Git Credentials
- Use `gh` CLI authentication (OAuth)
- Credentials stored in `~/.config/gh/`
- Rotate tokens periodically

### 9.3 Systemd Isolation
- Service runs as your user (not root)
- Memory and CPU limits enforced
- No network access restrictions (needs internet)

### 9.4 File Permissions
- Lock file: world-readable for debugging
- Logs: user-only readable
- Config: user-only readable

---

## 10. Success Metrics

### 10.1 Operational Metrics
- **Uptime**: >99.5%
- **Detection latency**: <2 minutes
- **False positive rate**: <0.1%
- **Resource usage**: <50% CPU avg, <800MB RAM peak

### 10.2 Business Metrics
- **Documents detected**: Track daily/weekly trends
- **Time to publish**: Compare to 3-hour GH Actions baseline
- **User satisfaction**: Faster updates on tracinginsights.com

---

## 11. Future Enhancements

### Phase 2 (Optional)

- [ ] Adaptive interval (2 min during race weekends, 15 min otherwise)
- [ ] Discord webhook support
- [ ] Prometheus metrics export
- [ ] Web dashboard for real-time monitoring
- [ ] Multi-region deployment (failover)
- [ ] Classification scraping integration
- [ ] Automatic interval tuning based on metrics

---

## Design Decisions Summary

### Key Decisions Made

1. **Deployment Strategy**: VPS does everything (fetch + commit + push directly to GitHub)
2. **Check Interval**: Every 1-2 minutes for near real-time detection
3. **Concurrency Control**: File lock (flock) with timeout + PID validation + systemd conflict prevention
4. **Authentication**: Use existing `gh` CLI credentials, service runs as user account
5. **Error Handling**: Retry with exponential backoff + detailed logging
6. **Notifications**: Telegram bot for persistent failure alerts (after 5 failures)
7. **Repository Strategy**: Sparse checkout + temp files (download → commit → push → delete)
8. **Process Management**: Systemd timer + service for reliability
9. **Resource Monitoring**: psutil with 10-second sampling intervals
10. **Configuration**: YAML config file + environment variables for secrets
11. **Extraction**: Full pipeline with `--limit 200` (matches GH Actions)
12. **Transcripts**: Full `--super-aggressive` mode
13. **GitHub Actions**: Keep existing 3-hour workflow as backup

### Assumptions

- 4GB RAM / 2vCPU Hetzner VPS is sufficient
- Telegram is accessible and reliable
- `gh` CLI authentication persists across systemd runs
- Manifest-based deduplication prevents conflicts
- FIA website can handle 2-minute request intervals

---

## Quick Start

```bash
# On VPS
curl -LsSf https://astral.sh/uv/install.sh | sh
gh auth login
cd ~ && git clone https://github.com/TracingInsights/DDocs.git
cd DDocs
bash service/setup.sh

# Edit configuration
nano service/config.yaml
nano service/secrets.env

# Test and deploy
uv run python service/monitor.py
sudo systemctl enable --now f1-scraper.timer
journalctl -u f1-scraper.service -f
```

---

**Status**: Ready for implementation  
**Last Updated**: 2026-05-19  
**Version**: 1.0
