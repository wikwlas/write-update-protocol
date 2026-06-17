import asyncio
import logging
import threading
import time

logger = logging.getLogger(__name__)


class DirectoryManager:
    def __init__(self):
        self._presence_list = {}
        self._main_memory = {}
        self._variable_write_locks = {}
        self._variable_write_timestamps = {}
        self._lock = threading.RLock()

    def get_variable_write_lock(self, variable_name: str):
        with self._lock:
            lock = self._variable_write_locks.get(variable_name)
            if lock is None:
                lock = asyncio.Lock()
                self._variable_write_locks[variable_name] = lock
            return lock

    def next_write_timestamp(self, variable_name: str):
        with self._lock:
            now = int(time.time() * 1000)
            last_timestamp = self._variable_write_timestamps.get(variable_name, 0)
            timestamp = now if now > last_timestamp else last_timestamp + 1
            self._variable_write_timestamps[variable_name] = timestamp
            return timestamp

    def register_variable_presence(self, variable_name: str, node_id: int):
        with self._lock:
            owners = self._presence_list.setdefault(variable_name, set())
            owners.add(node_id)
            logger.debug(
                "Directory Manager: registered '%s' presence on Node %s.",
                variable_name,
                node_id,
            )

    def update_main_memory_value(self, variable_name: str, value: str):
        with self._lock:
            self._main_memory[variable_name] = value
            logger.debug(
                "Directory Manager: updated main memory '%s' = '%s'.",
                variable_name,
                value,
            )

    def get_owners_of(self, variable_name: str):
        with self._lock:
            return set(self._presence_list.get(variable_name, set()))

    def get_value_from_main_memory(self, variable_name: str):
        with self._lock:
            return self._main_memory.get(variable_name)

    def clear(self):
        with self._lock:
            self._presence_list.clear()
            self._main_memory.clear()
            self._variable_write_locks.clear()
            self._variable_write_timestamps.clear()
            logger.info("Directory Manager: cleared state before reconstruction.")

    def snapshot(self):
        with self._lock:
            return {
                "presenceList": {
                    variable_name: sorted(node_ids)
                    for variable_name, node_ids in self._presence_list.items()
                },
                "mainMemory": dict(self._main_memory),
            }


directory_manager = DirectoryManager()
