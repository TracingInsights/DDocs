# Configuration Guide

## Cache Configuration

The scrapers use discovery caches to avoid re-crawling the same data within a short time window. The cache TTL (Time To Live) can be configured via environment variables.

### Environment Variables

- `DISCOVERY_CACHE_TTL_HOURS`: Cache TTL in hours (default: 2.5)
  - Set to match your scraping frequency
  - Default of 2.5 hours works well with 3-hour cron intervals
  - Set to 0 to disable caching entirely

### Examples

```bash
# Use 1-hour cache TTL
export DISCOVERY_CACHE_TTL_HOURS=1.0
python scraper.py

# Disable caching entirely
export DISCOVERY_CACHE_TTL_HOURS=0
python scraper.py --force-refresh

# Use default (2.5 hours)
python scraper.py
```

## File Safety

The scrapers use file locking to prevent data corruption when multiple processes access the same files:

- **Unix/Linux**: Uses `fcntl` for proper file locking
- **Windows**: Uses threading locks (process-local only)

### Best Practices

1. **Sequential Execution**: Run scrapers one at a time to avoid conflicts
2. **Force Refresh**: Use `--force-refresh` to bypass caches when needed
3. **Monitor Logs**: Check for file locking warnings in the logs

## GitHub Actions Configuration

The workflow runs scrapers sequentially to prevent manifest file corruption:

1. Main scraper runs first
2. Transcript scraper runs after (regardless of main scraper outcome)
3. Both outputs are combined for commit and release

### Workflow Environment Variables

```yaml
env:
  DISCOVERY_CACHE_TTL_HOURS: "2.5"  # Configurable cache TTL
```

## Cache Files

- `documents/discovery_cache.json`: Main scraper discovery cache
- `documents/transcript_discovery_cache.json`: Transcript scraper discovery cache
- `documents/manifest.json`: Combined manifest of all documents

All cache files include version numbers and timestamps for validation.