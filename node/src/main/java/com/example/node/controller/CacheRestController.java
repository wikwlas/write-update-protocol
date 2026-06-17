package com.example.node.controller;

import com.example.node.dto.CacheUpdateRequest;
import com.example.node.dto.ElectionMessage;
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
    public ResponseEntity<String> handleUserUpdateRequest(@RequestParam String key, @RequestParam String value) {
        log.info("Received local write request: {} = {}", key, value);

        if (systemNode.isLeader()) {
            // Case A: this node is the leader (Home Node).
            // 1. Update main memory and register local presence.
            directoryManager.updateMainMemoryValue(key, value);
            directoryManager.registerVariablePresence(key, systemNode.getNodeId());
            localCache.put(key, value);

            // 2. Fetch the nodes that hold this variable and send a Write-Update broadcast.
            replicationService.broadcastUpdate(key, value);
            return ResponseEntity.ok("Variable updated and replicated by the leader.");
        } else {
            int leaderId = systemNode.getLeaderId();
            String leaderUrl = systemNode.getPeers().get(leaderId);

            log.info("Node 3 is acting as a proxy. Forwarding request to leader Node {}", leaderId);

            // Synchronously wait for the leader response on behalf of the client.
            String responseFromLeader = webClientBuilder.build()
                    .post()
                    .uri(leaderUrl + "/update-request?key=" + key + "&value=" + value)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block(); // Wait for the result before responding to the user.

            return ResponseEntity.ok("Response from leader via proxy: " + responseFromLeader);
        }
    }

    /**
     * Forced update endpoint (Write-Update) called by the leader in the P2P network.
     * Forces the local cache to overwrite the value immediately in a thread-safe way.
     */
    @PostMapping("/force-update")
    public ResponseEntity<Void> handleForceUpdate(@RequestBody CacheUpdateRequest updateRequest) {
        log.info("P2P network: received forced Write-Update from Node {} for: {} = {}",
                updateRequest.getSenderNodeId(), updateRequest.getVariableName(), updateRequest.getNewValue());

        // Cache write protected by the LocalCache lock.
        localCache.put(updateRequest.getVariableName(), updateRequest.getNewValue());

        return ResponseEntity.ok().build();
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
    public ResponseEntity<String> getFromCache(@PathVariable String key) {
        String value = localCache.get(key);
        if (value != null) {
            return ResponseEntity.ok(value);
        }
        return ResponseEntity.notFound().build();
    }
}
