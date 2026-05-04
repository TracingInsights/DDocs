"""Shared utilities for FIA F1 scrapers."""

import json
import logging
import re
import time
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    # Windows doesn't have fcntl, use threading locks instead
    HAS_FCNTL = False

from config import CACHE_VERSION, DISCOVERY_CACHE_TTL_SECONDS

log = logging.getLogger(__name__)

# Global lock for file operations on Windows
_file_locks = {}
_lock_lock = threading.Lock()


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug."""
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def _get_file_lock(path: Path) -> threading.Lock:
    """Get or create a lock for a specific file path."""
    path_str = str(path.resolve())
    with _lock_lock:
        if path_str not in _file_locks:
            _file_locks[path_str] = threading.Lock()
        return _file_locks[path_str]


def load_manifest_with_lock(path: Path) -> Dict[str, Any]:
    """Load manifest with file locking to prevent corruption."""
    if not path.exists():
        return {}
    
    try:
        if HAS_FCNTL:
            # Unix-style file locking
            with open(path, 'r', encoding='utf-8') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)  # Shared lock for reading
                return json.load(f)
        else:
            # Windows-style threading locks
            file_lock = _get_file_lock(path)
            with file_lock:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
    except Exception as e:
        log.warning("Could not load manifest from %s: %s", path, e)
        return {}


def save_manifest_with_lock(path: Path, manifest: Dict[str, Any]) -> None:
    """Save manifest with file locking to prevent corruption."""
    path.parent.mkdir(parents=True, exist_ok=True)
    
    if HAS_FCNTL:
        # Unix-style file locking with atomic rename
        temp_path = path.with_suffix('.tmp')
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # Exclusive lock for writing
                json.dump(manifest, f, indent=2, sort_keys=True, ensure_ascii=False)
            
            # Atomic rename
            temp_path.replace(path)
            log.debug("Saved manifest to %s", path)
        except Exception as e:
            log.error("Failed to save manifest to %s: %s", path, e)
            if temp_path.exists():
                temp_path.unlink()
            raise
    else:
        # Windows-style threading locks with direct write
        file_lock = _get_file_lock(path)
        try:
            with file_lock:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(manifest, f, indent=2, sort_keys=True, ensure_ascii=False)
            log.debug("Saved manifest to %s", path)
        except Exception as e:
            log.error("Failed to save manifest to %s: %s", path, e)
            raise


def load_discovery_cache(
    path: Path, 
    cache_type: str = "discovery",
    force_refresh: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Load discovery cache with comprehensive validation.
    
    Args:
        path: Path to cache file
        cache_type: Type of cache for logging ("discovery" or "transcript")
        force_refresh: If True, skip cache and force refresh
    
    Returns:
        Cache data if valid, None if invalid or expired
    """
    if force_refresh:
        log.info("Force refresh requested, skipping %s cache", cache_type)
        return None
    
    if not path.exists():
        log.debug("%s cache file does not exist: %s", cache_type.title(), path)
        return None
    
    try:
        if HAS_FCNTL:
            # Unix-style file locking
            with open(path, 'r', encoding='utf-8') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)  # Shared lock for reading
                data = json.load(f)
        else:
            # Windows-style threading locks
            file_lock = _get_file_lock(path)
            with file_lock:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
        
        # Validate cache version
        if data.get("version") != CACHE_VERSION:
            log.info("%s cache version mismatch (expected %d, got %s), re-crawling", 
                    cache_type.title(), CACHE_VERSION, data.get("version"))
            return None
        
        # Validate timestamp
        if "timestamp" not in data:
            log.warning("%s cache missing timestamp, re-crawling", cache_type.title())
            return None
        
        # Check TTL
        age = time.time() - data["timestamp"]
        if age >= DISCOVERY_CACHE_TTL_SECONDS:
            log.info("%s cache expired (%.0fs old, TTL=%.0fs), re-crawling", 
                    cache_type.title(), age, DISCOVERY_CACHE_TTL_SECONDS)
            return None
        
        log.info("Using %s cache (%.0fs old, TTL=%.0fs)", 
                cache_type, age, DISCOVERY_CACHE_TTL_SECONDS)
        return data
        
    except Exception as e:
        log.warning("Could not read %s cache from %s: %s", cache_type, path, e)
        return None


def save_discovery_cache(
    path: Path, 
    data: Dict[str, Any], 
    cache_type: str = "discovery"
) -> None:
    """
    Save discovery cache with version and timestamp.
    
    Args:
        path: Path to cache file
        data: Cache data to save
        cache_type: Type of cache for logging ("discovery" or "transcript")
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Add metadata
        cache_data = {
            **data,
            "version": CACHE_VERSION,
            "timestamp": time.time()
        }
        
        if HAS_FCNTL:
            # Unix-style file locking with atomic rename
            temp_path = path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # Exclusive lock for writing
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            # Atomic rename
            temp_path.replace(path)
        else:
            # Windows-style threading locks with direct write
            file_lock = _get_file_lock(path)
            with file_lock:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, indent=2, ensure_ascii=False)
        log.info("%s cache saved (%s)", cache_type.title(), 
                f"{len(data.get('events', []))} events" if 'events' in data else "data cached")
        
    except Exception as e:
        log.warning("Failed to save %s cache to %s: %s", cache_type, path, e)


def validate_cache_structure(data: Dict[str, Any], required_keys: List[str]) -> bool:
    """
    Validate that cache data has required structure.
    
    Args:
        data: Cache data to validate
        required_keys: List of required keys
    
    Returns:
        True if valid, False otherwise
    """
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        log.warning("Cache missing required keys: %s", missing_keys)
        return False
    return True