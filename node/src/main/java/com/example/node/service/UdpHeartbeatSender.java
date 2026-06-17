package com.example.node.service;

import com.example.node.model.SystemNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Service responsible for periodically sending heartbeat signals
 * to the remaining nodes over connectionless UDP.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UdpHeartbeatSender {

    private final SystemNode systemNode;
    private final ObjectMapper objectMapper; // Generates a unified JSON format.

    // UDP port used by the other nodes, injected from application.properties.
    @Value("${udp.port:4444}")
    private int udpPort;

    // IP addresses of the remaining machines, defined in the properties file.
    @Value("${peers.node1.ip:localhost}")
    private String node1Ip;

    @Value("${peers.node2.ip:localhost}")
    private String node2Ip;

    /**
     * Method automatically executed in a separate thread every 2000 ms (2 seconds).
     * Sends asynchronous UDP packets in fire-and-forget mode.
     */
    @Scheduled(fixedRate = 2000)
    public void sendHeartbeats() {
        // Skip sending while the node is taking part in an election.
        if ("ELECTION".equals(systemNode.getState())) {
            return;
        }

        try {
            // Build a compact JSON object for the shared data contract.
            ObjectNode heartbeatJson = objectMapper.createObjectNode();
            heartbeatJson.put("nodeId", systemNode.getNodeId());
            heartbeatJson.put("status", "ALIVE");
            heartbeatJson.put("timestamp", System.currentTimeMillis());

            String jsonPayload = heartbeatJson.toString();
            byte[] buffer = jsonPayload.getBytes();

            // Open a UDP socket. try-with-resources closes the DatagramSocket automatically.
            try (DatagramSocket socket = new DatagramSocket()) {

                // 1. Send to Node 1 (C# / Linux).
                if (systemNode.getNodeId() != 1) {
                    sendUdpPacket(socket, buffer, node1Ip);
                }

                // 2. Send to Node 2 (Python / macOS).
                if (systemNode.getNodeId() != 2) {
                    sendUdpPacket(socket, buffer, node2Ip);
                }
            }

        } catch (Exception e) {
            log.error("Critical error while preparing UDP heartbeat packet: {}", e.getMessage());
        }
    }

    /**
     * Helper method sending a raw packet to the selected IP address.
     */
    private void sendUdpPacket(DatagramSocket socket, byte[] buffer, String targetIp) {
        try {
            InetAddress address = InetAddress.getByName(targetIp);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, udpPort);
            socket.send(packet);
            log.debug("Sent UDP heartbeat to {}:{}", targetIp, udpPort);
        } catch (Exception e) {
            // Log as warning because an offline peer is expected in a distributed system.
            log.warn("Could not send UDP packet to {} (node may be offline): {}", targetIp, e.getMessage());
        }
    }
}
