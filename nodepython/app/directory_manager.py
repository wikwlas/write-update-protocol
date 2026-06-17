import logging
import threading

logger = logging.getLogger(__name__)


class DirectoryManager:
    def __init__(self):
        self._presence_list = {}
        self._main_memory = {}
        self._lock = threading.RLock()

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
