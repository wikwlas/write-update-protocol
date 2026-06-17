package com.example.node.service;

import com.example.node.model.SystemNode;
import com.example.node.model.LocalCache;
import com.example.node.model.DirectoryManager;
import com.example.node.dto.ElectionMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service implementing the Bully leader election algorithm
 * and state recovery after failures.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BullyElectionService {

    private final SystemNode systemNode;
    private final LocalCache localCache;
    private final DirectoryManager directoryManager; // Component activated after winning an election.
    private final WebClient.Builder webClientBuilder;

    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    /**
     * Starts the election procedure after a leader timeout or during node startup/recovery.
     */
    public void startElection() {
        // Prevent multiple election procedures from running at the same time.
        if (!electionInProgress.compareAndSet(false, true)) {
            log.info("Election is already in progress. Ignoring duplicate trigger.");
            return;
        }

        systemNode.setState("ELECTION");
        log.info("Node {} is starting the leader election procedure (Bully Algorithm)...", systemNode.getNodeId());

        // Filter nodes with IDs higher than this node's ID.
        Map<Integer, String> higherNodes = systemNode.getPeers().entrySet().stream()
                .filter(entry -> entry.getKey() > systemNode.getNodeId())
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (higherNodes.isEmpty()) {
            // This node has the highest ID in the network, so it automatically wins.
            log.info("No nodes with higher IDs found. Node {} announces itself as the new leader.", systemNode.getNodeId());
            announceVictory();
        } else {
            // Send an ELECTION message to nodes with higher IDs.
            AtomicBoolean receivedAnswer = new AtomicBoolean(false);

            Flux.fromIterable(higherNodes.entrySet())
                    .flatMap(entry -> sendElectionMessage(entry.getValue(), entry.getKey()))
                    .timeout(Duration.ofMillis(1500)) // Aggressive network timeout to avoid blocking the system.
                    .doOnNext(answer -> {
                        if (Boolean.TRUE.equals(answer)) {
                            receivedAnswer.set(true);
                        }
                    })
                    .onErrorResume(e -> Mono.empty()) // Ignore connection errors (node is offline).
                    .then()
                    .doOnTerminate(() -> {
                        if (!receivedAnswer.get()) {
                            // No higher node answered in time, so this node wins.
                            log.info("No node with a higher ID answered. Node {} wins the election.", systemNode.getNodeId());
                            announceVictory();
                        } else {
                            // A higher node answered and takes over the election process.
                            log.info("A higher-priority node answered. Waiting for the new leader announcement.");
                            electionInProgress.set(false);
                        }
                    })
                    .subscribe();
        }
    }

    /**
     * Sends an ELECTION message to a specific node.
     */
    private Mono<Boolean> sendElectionMessage(String nodeUrl, int targetNodeId) {
        WebClient client = webClientBuilder.baseUrl(nodeUrl).build();
        return client.post()
                .uri("/election")
                .bodyValue(new ElectionMessage(systemNode.getNodeId(), "ELECTION"))
                .retrieve()
                .bodyToMono(Boolean.class)
                .doOnError(err -> log.debug("Node {} did not answer the election message (probably offline).", targetNodeId))
                .onErrorReturn(false);
    }

    /**
     * Announces victory and sends the COORDINATOR message to all remaining nodes.
     */
    private void announceVictory() {
        systemNode.updateLeader(systemNode.getNodeId());

        // Recovery step: rebuild the global directory before managing the network.
        reconstructGlobalDirectoryFromPeers();

        // Send COORDINATOR messages to peers asynchronously and non-blockingly.
        Flux.fromIterable(systemNode.getPeers().entrySet())
                .flatMap(entry -> {
                    WebClient client = webClientBuilder.baseUrl(entry.getValue()).build();
                    return client.post()
                            .uri("/election")
                            .bodyValue(new ElectionMessage(systemNode.getNodeId(), "COORDINATOR"))
                            .retrieve()
                            .toBodilessEntity()
                            .onErrorResume(e -> Mono.empty());
                })
                .subscribe();

        electionInProgress.set(false);
        log.info("Node {} successfully broadcast the new leader status (COORDINATOR) and finished the election.", systemNode.getNodeId());
    }

    /**
     * Reconstructs the global directory (Directory Manager) on the newly elected leader.
     * Pulls local cache copies from all active nodes (C# and Python).
     */
    private void reconstructGlobalDirectoryFromPeers() {
        log.info("Starting directory state reconstruction from active nodes...");

        // Clear the old, uncertain local directory state.
        directoryManager.clearDirectory();

        Flux.fromIterable(systemNode.getPeers().entrySet())
                .flatMap(entry -> {
                    WebClient client = webClientBuilder.baseUrl(entry.getValue()).build();
                    return client.get()
                            .uri("/reconstruct-directory") // Dedicated endpoint exposed by peers.
                            .retrieve()
                            .bodyToMono(Map.class) // Receive [variableName -> value] from the peer's local cache.
                            .map(cacheMap -> Map.entry(entry.getKey(), cacheMap))
                            .onErrorResume(e -> Mono.empty());
                })
                .doOnNext(entry -> {
                    int peerId = entry.getKey();
                    Map<String, String> peerCache = entry.getValue();

                    // Register data in the new leader's DirectoryManager.
                    peerCache.forEach((variableName, value) -> {
                        directoryManager.registerVariablePresence(variableName, peerId);
                        directoryManager.updateMainMemoryValue(variableName, value);
                    });
                    log.info("Successfully synchronized and restored resource state from Node {}.", peerId);
                })
                .then()
                .doOnTerminate(() -> {
                    // Also add the leader's own local cache data to the directory and main memory.
                    localCache.getAll().forEach((varName, value) -> {
                        directoryManager.registerVariablePresence(varName, systemNode.getNodeId());
                        directoryManager.updateMainMemoryValue(varName, value);
                    });
                    log.info("Directory reconstruction completed successfully. Presence and value state restored.");
                })
                .subscribe();
    }
}
