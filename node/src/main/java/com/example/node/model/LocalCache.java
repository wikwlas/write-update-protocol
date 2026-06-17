package com.example.node.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents the node's local cache.
 * Stores [variableName -> value] pairs safely in memory.
 * Uses strict concurrency controls to eliminate race conditions.
 */
@Slf4j
@Component
public class LocalCache {

    // Thread-safe map storing local copies of cluster variables.
    private final Map<String, String> cacheStorage = new ConcurrentHashMap<>();

    // Map storing dedicated locks per variable (granular locking).
    private final Map<String, ReentrantLock> variableLocks = new ConcurrentHashMap<>();

    /**
     * Reads a variable value from the local cache.
     * Under cache coherence protocols, reading a valid local state is immediate.
     *
     * @param variableName variable name (key)
     * @return variable value, or null if it does not exist in the local cache
     */
    public String get(String variableName) {
        return cacheStorage.get(variableName);
    }

    /**
     * Writes or updates a value in the local cache.
     * Protected by a per-variable lock to prevent conflicts between local writes
     * and network writes (Write-Update from another node).
     *
     * @param variableName variable name
     * @param value new value to store
     */
    public void put(String variableName, String value) {
        // Fetch the existing lock for this variable or create one for the first write.
        ReentrantLock lock = variableLocks.computeIfAbsent(variableName, k -> new ReentrantLock());

        lock.lock(); // Lock the critical write section for this variable.
        try {
            cacheStorage.put(variableName, value);
            log.info("Local Cache: updated variable state '{}' = '{}'", variableName, value);
        } finally {
            lock.unlock(); // Always release the lock in finally.
        }
    }

    /**
     * Removes a variable from the local cache, for example after invalidation.
     */
    public void remove(String variableName) {
        ReentrantLock lock = variableLocks.get(variableName);
        if (lock != null) {
            lock.lock();
            try {
                cacheStorage.remove(variableName);
                variableLocks.remove(variableName);
                log.info("Local Cache: removed variable '{}' from cache.", variableName);
            } finally {
                lock.unlock();
            }
        } else {
            cacheStorage.remove(variableName);
        }
    }

    /**
     * Returns an unmodifiable view of the full local cache.
     * Used by BullyElectionService during directory reconstruction when the new leader
     * requests a local memory dump (Directory Manager Recovery).
     */
    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(cacheStorage);
    }

    /**
     * Clears the full cache, for example during a full node reset.
     */
    public void clear() {
        cacheStorage.clear();
        variableLocks.clear();
        log.info("Local Cache was fully cleared.");
    }
}
