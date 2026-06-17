package com.example.node.service;

import com.example.node.dto.CacheUpdateRequest;
import com.example.node.model.SystemNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Service responsible for data replication in the Write-Update model.
 * When a variable changes, this service asynchronously sends the new value
 * to the remaining nodes using WebClient (HTTP REST).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReplicationService {

    private final SystemNode systemNode;
    private final WebClient.Builder webClientBuilder;

    /**
     * Broadcasts variable update information (Write-Update Broadcast)
     * to all remaining active nodes in the P2P cluster.
     *
     * @param variableName name of the modified variable (key)
     * @param newValue new variable value
     */
    public void broadcastUpdate(String variableName, String newValue) {
        log.info("Starting asynchronous Write-Update broadcast for variable: {} = {}", variableName, newValue);

        // Create a unified JSON data contract for heterogeneous environments (C#, Python).
        CacheUpdateRequest updateRequest = new CacheUpdateRequest(
                systemNode.getNodeId(),
                variableName,
                newValue,
                System.currentTimeMillis()
        );

        // Iterate over the peer map and send requests in parallel.
        Flux.fromIterable(systemNode.getPeers().entrySet())
                .flatMap(entry -> {
                    int peerId = entry.getKey();
                    String peerUrl = entry.getValue();

                    log.debug("Sending update to Node {} at {}", peerId, peerUrl);

                    // Configure and execute an asynchronous POST request using WebClient.
                    WebClient webClient = webClientBuilder.baseUrl(peerUrl).build();

                    return webClient.post()
                            .uri("/force-update") // Endpoint required on peers.
                            .bodyValue(updateRequest)
                            .retrieve()
                            .toBodilessEntity() // Only the HTTP status code matters here.
                            .timeout(Duration.ofMillis(1000)) // Wait up to 1 second for network communication.
                            .doOnSuccess(response -> log.info("Node {} successfully updated its cache.", peerId))
                            .doOnError(error -> log.warn("Could not update Node {} (node may be offline or overloaded): {}", peerId, error.getMessage()))
                            .onErrorResume(e -> Mono.empty()); // Swallow errors so one failed node does not stop the others.
                })
                .subscribe(); // Starts the asynchronous background process; the main method returns immediately.
    }
}
