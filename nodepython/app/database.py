import threading
import logging

logger = logging.getLogger(__name__)

class LocalCache:
    def __init__(self):
        # Internal storage dictionary and a lock to ensure thread safety
        self._storage = {}
        self._timestamps = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        """Retrieve a value from the cache by its key."""
        with self._lock:
            return self._storage.get(key)

    def put(self, key: str, value: str, timestamp: int = 0):
        """Insert or update a key-value pair in the cache."""
        with self._lock:
            effective_timestamp = timestamp if timestamp > 0 else 0
            current_timestamp = self._timestamps.get(key, 0)
            if effective_timestamp and current_timestamp > effective_timestamp:
                logger.info(
                    "Local Cache (Python): Ignored stale update for '%s'. Current timestamp=%s, incoming timestamp=%s",
                    key,
                    current_timestamp,
                    effective_timestamp,
                )
                return False

            self._storage[key] = value
            if effective_timestamp:
                self._timestamps[key] = effective_timestamp
            logger.info(f"Local Cache (Python): Updated '{key}' = '{value}'")
            return True

    def get_all(self):
        """Return a copy of the entire cache storage."""
        with self._lock:
            return dict(self._storage)

# Global instance of the cache to be imported across the app
local_cache = LocalCache()
