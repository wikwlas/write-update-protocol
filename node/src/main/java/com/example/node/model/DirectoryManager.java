package com.example.node.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Component managing the global coherence directory (Directory Manager).
 * This class is fully operational only on the active leader (Home Node).
 * It is responsible for:
 * 1. presenceList - tracking which nodes hold a copy of a given variable.
 * 2. mainMemory - storing authoritative, most recent variable values.
 */
@Slf4j
@Component
public class DirectoryManager {

    // Global presence map: [variableName -> set of node IDs that hold it].
    private final Map<String, Set<Integer>> presenceList = new ConcurrentHashMap<>();

    // Cluster virtual main memory: [variableName -> currentValue].
    private final Map<String, String> mainMemory = new ConcurrentHashMap<>();

    // Read/write lock protecting structure consistency against race conditions.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Per-variable write queues serialize complete leader-side write propagation.
    private final Map<String, ReentrantLock> variableWriteLocks = new ConcurrentHashMap<>();

    // Monotonic write timestamps per variable avoid ties for rapid consecutive writes.
    private final Map<String, Long> variableWriteTimestamps = new ConcurrentHashMap<>();

    public <T> T withVariableWriteLock(String variableName, Supplier<T> work) {
        ReentrantLock lock = variableWriteLocks.computeIfAbsent(variableName, key -> new ReentrantLock());
        lock.lock();
        try {
            return work.get();
        } finally {
            lock.unlock();
        }
    }

    public long nextWriteTimestamp(String variableName) {
        return variableWriteTimestamps.compute(variableName, (key, lastTimestamp) -> {
            long now = System.currentTimeMillis();
            if (lastTimestamp == null || now > lastTimestamp) {
                return now;
            }
            return lastTimestamp + 1;
        });
    }

    /**
     * Registers that a given node holds a copy of the selected variable in its cache.
     * This method is key for restoring directory state after the previous leader fails.
     */
    public void registerVariablePresence(String variableName, int nodeId) {
        rwLock.writeLock().lock();
        try {
            presenceList.computeIfAbsent(variableName, k -> new HashSet<>()).add(nodeId);
            log.debug("Directory: registered presence of variable '{}' on Node {}.", variableName, nodeId);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Updates the value in cluster main memory.
     */
    public void updateMainMemoryValue(String variableName, String value) {
        rwLock.writeLock().lock();
        try {
            mainMemory.put(variableName, value);
            log.debug("Directory: updated main memory for '{}' = {}.", variableName, value);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Returns the node IDs that hold a copy of the selected variable.
     * Used by the leader to select recipients for Write-Update broadcast messages.
     *
     * @return set containing node IDs, or an empty set if nobody has the variable.
     */
    public Set<Integer> getOwnersOf(String variableName) {
        rwLock.readLock().lock();
        try {
            Set<Integer> owners = presenceList.get(variableName);
            return owners != null ? new HashSet<>(owners) : new HashSet<>();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns the current variable value stored in cluster main memory.
     */
    public String getValueFromMainMemory(String variableName) {
        rwLock.readLock().lock();
        try {
            return mainMemory.get(variableName);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Fully clears the global directory state.
     * Called by BullyElectionService before querying peers to eliminate the empty directory problem.
     */
    public void clearDirectory() {
        rwLock.writeLock().lock();
        try {
            presenceList.clear();
            mainMemory.clear();
            variableWriteLocks.clear();
            variableWriteTimestamps.clear();
            log.info("Global directory was reset and cleared for state reconstruction.");
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Removes a node from the presence list for all variables.
     * Useful after detecting a permanent timeout or follower failure.
     */
    public void removeNodeFromPresence(int failedNodeId) {
        rwLock.writeLock().lock();
        try {
            presenceList.forEach((varName, nodesSet) -> {
                if (nodesSet.remove(failedNodeId)) {
                    log.info("Directory: removed failed Node {} from the sharing list for variable '{}'.", failedNodeId, varName);
                }
            });
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Map<String, Object> snapshot() {
        rwLock.readLock().lock();
        try {
            Map<String, Set<Integer>> presenceSnapshot = presenceList.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new HashSet<>(entry.getValue())
                    ));

            return Map.of(
                    "presenceList", presenceSnapshot,
                    "mainMemory", new ConcurrentHashMap<>(mainMemory)
            );
        } finally {
            rwLock.readLock().unlock();
        }
    }
}
