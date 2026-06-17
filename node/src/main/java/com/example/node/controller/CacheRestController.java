package com.example.node.controller;

import com.example.node.dto.CacheUpdateRequest;
import com.example.node.dto.ElectionMessage;
import com.example.node.dto.UserUpdateRequest;
import com.example.node.model.DirectoryManager;
import com.example.node.model.LocalCache;
import com.example.node.model.SystemNode;
import com.example.node.service.BullyElectionService;
import com.example.node.service.ReplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Main REST controller for the application, equivalent to the RestAPI component in the project design.
 * Handles client requests and P2P communication with the remaining nodes (C# and Python).
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class CacheRestController {

    private final SystemNode systemNode;
    private final LocalCache localCache;
    private final DirectoryManager directoryManager;
    private final ReplicationService replicationService;
    private final BullyElectionService electionService;
    private final org.springframework.web.reactive.function.client.WebClient.Builder webClientBuilder;

    /**
     * Endpoint called by a local user or application to write or modify a variable.
     * Implements the strategy described in the project sequence diagram.
     */
    @PostMapping("/update-request")
    public ResponseEntity<Map<String, Object>> handleUserUpdateRequest(
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String value,
            @RequestBody(required = false) UserUpdateRequest requestBody) {
        if (requestBody != null) {
            if (key == null) {
                key = requestBody.getKey();
            }
            if (value == null) {
                value = requestBody.getValue();
            }
        }

        if (key == null || key.isBlank() || value == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Request requires 'key' and 'value' as JSON fields or query parameters."
            ));
        }

        final String resolvedKey = key;
        final String resolvedValue = value;

        log.info("Received local write request: {} = {}", resolvedKey, resolvedValue);

        if (systemNode.isLeader()) {
            return directoryManager.withVariableWriteLock(resolvedKey, () -> {
                long timestamp = directoryManager.nextWriteTimestamp(resolvedKey);

                // Case A: this node is the leader (Home Node).
                // 1. Update main memory and register local presence.
                directoryManager.updateMainMemoryValue(resolvedKey, resolvedValue);
                directoryManager.registerVariablePresence(resolvedKey, systemNode.getNodeId());
                localCache.put(resolvedKey, resolvedValue, timestamp);

                // 2. Fetch the nodes that hold this variable and send a Write-Update broadcast.
                replicationService.broadcastUpdate(resolvedKey, resolvedValue, timestamp);
                return ResponseEntity.ok(Map.of(
                        "status", "SUCCESS",
                        "message", "Variable updated and replicated by the leader.",
                        "key", resolvedKey,
                        "value", resolvedValue,
                        "timestamp", timestamp
                ));
            });
        } else {
            int leaderId = systemNode.getLeaderId();
            String leaderUrl = systemNode.getPeers().get(leaderId);

            log.info("Node 3 is acting as a proxy. Forwarding request to leader Node {}", leaderId);

            // Synchronously wait for the leader response on behalf of the client.
            Object responseFromLeader = webClientBuilder.build()
                    .post()
                    .uri(leaderUrl + "/update-request")
                    .bodyValue(new UserUpdateRequest(resolvedKey, resolvedValue))
                    .retrieve()
                    .bodyToMono(Object.class)
                    .block(); // Wait for the result before responding to the user.

            return ResponseEntity.ok(Map.of(
                    "status", "SUCCESS_VIA_PROXY",
                    "leaderId", leaderId,
                    "leaderResponse", responseFromLeader != null ? responseFromLeader : Map.of()
            ));
        }
    }

    /**
     * Forced update endpoint (Write-Update) called by the leader in the P2P network.
     * Forces the local cache to overwrite the value immediately in a thread-safe way.
     */
    @PostMapping("/force-update")
    public ResponseEntity<Map<String, Object>> handleForceUpdate(@RequestBody CacheUpdateRequest updateRequest) {
        log.info("P2P network: received forced Write-Update from Node {} for: {} = {}",
                updateRequest.getSenderNodeId(), updateRequest.getVariableName(), updateRequest.getNewValue());

        // Cache write protected by the LocalCache lock and timestamp versioning.
        boolean updated = localCache.put(updateRequest.getVariableName(), updateRequest.getNewValue(), updateRequest.getTimestamp());

        return ResponseEntity.ok(Map.of(
                "status", updated ? "SUCCESS" : "IGNORED_STALE_UPDATE",
                "key", updateRequest.getVariableName(),
                "value", updateRequest.getNewValue(),
                "timestamp", updateRequest.getTimestamp()
        ));
    }

    /**
     * Endpoint called by a newly elected leader after winning an election.
     * The new leader requests a local cache dump to rebuild its empty directory.
     */
    @GetMapping("/reconstruct-directory")
    public ResponseEntity<Map<String, String>> handleDirectoryReconstructionRequest() {
        log.info("P2P network: new leader requested a local cache dump for global directory reconstruction.");

        // Return an unmodifiable local cache map [variable -> value].
        return ResponseEntity.ok(localCache.getAll());
    }

    /**
     * Handles network messages for the leader election protocol (Bully Algorithm).
     */
    @PostMapping("/election")
    public ResponseEntity<Boolean> handleElectionMessage(@RequestBody ElectionMessage message) {
        log.info("Bully Algorithm: received '{}' message from Node {}", message.getType(), message.getSenderNodeId());

        switch (message.getType()) {
            case "ELECTION":
                // If the request comes from a lower-ID node, answer TRUE and take over the election.
                if (systemNode.hasHigherPriorityThan(message.getSenderNodeId())) {
                    log.info("This node has higher priority than Node {}. Answering and starting its own election.", message.getSenderNodeId());

                    // Start our own election asynchronously against even higher-priority nodes.
                    // Use a background thread so the HTTP response can return immediately.
                    new Thread(electionService::startElection).start();
                    return ResponseEntity.ok(true);
                }
                return ResponseEntity.ok(false);

            case "COORDINATOR":
                // A stronger node announced itself as the new coordinator/leader.
                log.info("Accepted new network leader: Node {}", message.getSenderNodeId());
                systemNode.updateLeader(message.getSenderNodeId());
                return ResponseEntity.ok().build();

            default:
                return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Simple GET endpoint for users/testers to inspect the local cache on this node.
     */
    @GetMapping("/cache/{key}")
    public ResponseEntity<Map<String, Object>> getFromCache(@PathVariable String key) {
        String value = localCache.get(key);
        if (value != null) {
            return ResponseEntity.ok(Map.of(
                    "key", key,
                    "value", value
            ));
        }
        return ResponseEntity.status(404).body(Map.of(
                "error", "Key not found.",
                "key", key
        ));
    }
}
