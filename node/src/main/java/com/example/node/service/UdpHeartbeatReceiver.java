package com.example.node.service;

import com.example.node.model.SystemNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service responsible for receiving and analyzing UDP heartbeat packets.
 * Monitors P2P cluster health and detects failures of other nodes.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UdpHeartbeatReceiver {

    private final SystemNode systemNode;
    private final BullyElectionService electionService;
    private final ObjectMapper objectMapper; // Safely parses JSON from other environments.

    // Thread-safe map storing: [node ID -> last heartbeat timestamp].
    private final Map<Integer, Long> lastHeartbeats = new ConcurrentHashMap<>();

    // Maximum time without a response before considering a node dead.
    private static final long TIMEOUT_MS = 5000;

    /**
     * Method called by UdpConfig after receiving a UDP packet.
     * Processes text messages coming from any environment (Java, C#, Python).
     */
    public void processHeartbeat(String payload) {
        try {
            int senderNodeId;

            // Compatibility handling for heterogeneous payload formats.
            // Check whether the payload is JSON or plain text.
            if (payload.trim().startsWith("{")) {
                JsonNode jsonNode = objectMapper.readTree(payload);
                senderNodeId = jsonNode.get("nodeId").asInt();
            } else {
                // Assume plain text looks like "HEARTBEAT_FROM_NODE_1".
                senderNodeId = Integer.parseInt(payload.replaceAll("[^0-9]", ""));
            }

            // Update the last contact time in the thread-safe map.
            lastHeartbeats.put(senderNodeId, System.currentTimeMillis());
            log.debug("Received UDP heartbeat from Node {}", senderNodeId);

        } catch (Exception e) {
            log.error("Error while parsing UDP heartbeat packet: {}. Raw payload: {}", e.getMessage(), payload);
        }
    }

    /**
     * Background verifier running periodically (for example every 2 seconds).
     * Checks whether any active node, especially the leader, has failed.
     */
    @Scheduled(fixedRate = 2000)
    public void checkNodeTimeouts() {
        long currentTime = System.currentTimeMillis();

        // Fetch the current list of known peers from SystemNode configuration.
        for (Integer peerId : systemNode.getPeers().keySet()) {
            Long lastContact = lastHeartbeats.get(peerId);

            // If this node was seen before, check whether it timed out.
            if (lastContact != null && (currentTime - lastContact) > TIMEOUT_MS) {
                log.warn("Detected timeout for Node {}! No response for {} ms.", peerId, (currentTime - lastContact));

                // Remove from the map to avoid triggering the same failure forever.
                lastHeartbeats.remove(peerId);

                // Invoke node failure handling logic.
                handleNodeFailure(peerId);
            }
        }
    }

    /**
     * Reacts to a network failure or another node process crash.
     */
    private void handleNodeFailure(int failedNodeId) {
        // If the current leader disappeared, start a new election immediately.
        if (systemNode.getLeaderId() == failedNodeId) {
            log.warn("Leader failure reported (Node {}). Starting Bully algorithm...", failedNodeId);
            electionService.startElection(); // Transition to Election state.
        } else {
            // Regular follower failure; the leader updates its directory if needed.
            log.info("Node {} (Follower) is inactive. The system continues operating.", failedNodeId);
        }
    }
}
