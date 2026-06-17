import threading
import logging

logger = logging.getLogger(__name__)

class LocalCache:
    def __init__(self):
        # Internal storage dictionary and a lock to ensure thread safety
        self._storage = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        """Retrieve a value from the cache by its key."""
        with self._lock:
            return self._storage.get(key)

    def put(self, key: str, value: str):
        """Insert or update a key-value pair in the cache."""
        with self._lock:
            self._storage[key] = value
            logger.info(f"Local Cache (Python): Updated '{key}' = '{value}'")

    def get_all(self):
        """Return a copy of the entire cache storage."""
        with self._lock:
            return dict(self._storage)

# Global instance of the cache to be imported across the app
local_cache = LocalCache()