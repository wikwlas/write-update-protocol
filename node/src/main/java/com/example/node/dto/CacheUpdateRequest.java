package com.example.node.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) representing a cache update message.
 * The class maps automatically to JSON to preserve the heterogeneous system contract.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheUpdateRequest {
    private int senderNodeId;       // Node that sent the update request.
    private String variableName;    // Variable identifier (key).
    private String newValue;        // New value sent over the network.
    private long timestamp;         // Timestamp preventing chronological conflicts.
}
