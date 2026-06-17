package com.example.node.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the identity and state of the current node in the distributed system.
 * This class acts as a Spring-managed singleton component shared by network services,
 * the Bully algorithm, and the controller layer.
 */
@Slf4j
@Component
@Getter
@Setter
public class SystemNode {

    // Unique node ID injected from configuration. Java/Windows defaults to 3.
    @Value("${node.id:3}")
    private int nodeId;

    // IP address and HTTP port of this node, injected from application.properties.
    @Value("${node.ip:localhost}")
    private String nodeIp;

    @Value("${server.port:8083}")
    private int httpPort;

    // Identifier of the current network leader (Home Node).
    private volatile int leaderId;

    // Flag indicating whether this node is currently the leader.
    private volatile boolean isLeader = false;

    // Node state in the cluster, for example "NORMAL", "ELECTION", or "RECOVERY".
    private volatile String state = "NORMAL";

    /**
     * Thread-safe, externally immutable map of remaining nodes (peers).
     * Key: node ID (1 for C#, 2 for Python)
     * Value: HTTP base address, for example "http://192.168.1.50:8082"
     */
    private Map<Integer, String> peers;

    // Raw configuration properties injected by Spring to build the peer map.
    @Value("${peers.node1.url:http://localhost:8081}")
    private String node1Url;

    @Value("${peers.node2.url:http://localhost:8082}")
    private String node2Url;

    /**
     * Initialization method automatically run after Spring dependency injection.
     * Builds the network peer map from the configuration file.
     */
    @PostConstruct
    public void init() {
        Map<Integer, String> tempPeers = new HashMap<>();

        // Map addresses according to the architecture (Node 1 = C#/.NET, Node 2 = Python/FastAPI).
        if (nodeId != 1) tempPeers.put(1, node1Url);
        if (nodeId != 2) tempPeers.put(2, node2Url);

        // Wrap with unmodifiableMap for thread-safety; the map is read-only.
        this.peers = Collections.unmodifiableMap(tempPeers);

        // At startup, assume the default leader from the project design (Node 3).
        // If the network state changes, the Bully algorithm will correct it.
        this.leaderId = 3;
        if (this.nodeId == 3) {
            this.isLeader = true;
            log.info("Node {} initialized as the initial leader (Home Node).", nodeId);
        } else {
            log.info("Node {} initialized as a follower. Current leader: Node {}", nodeId, leaderId);
        }
    }

    /**
     * Checks whether this node has higher priority (ID) than another node.
     * Used directly in the Bully algorithm message handling logic.
     */
    public boolean hasHigherPriorityThan(int otherNodeId) {
        return this.nodeId > otherNodeId;
    }

    /**
     * Safely updates the leader role on this node.
     * Called when the Bully algorithm finishes an election or receives a new leader message.
     */
    public synchronized void updateLeader(int newLeaderId) {
        this.leaderId = newLeaderId;
        this.isLeader = (this.nodeId == newLeaderId);
        this.state = "NORMAL";
        log.info("Node state updated. New network leader: Node {}. Is this node leader: {}", newLeaderId, isLeader);
    }
}
