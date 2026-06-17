package com.example.node.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) used during global directory reconstruction.
 * Defines the request structure sent by a newly elected leader, or a node in recovery,
 * to the remaining nodes (C# and Python) to collect their local cache state.
 * Addresses the empty directory problem described in the design review.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DirectoryRecoveryRequest {

    /**
     * Identifier of the new leader requesting the data dump, for example 3 for Java.
     * Lets the receiving node verify that the request came from an authorized network coordinator.
     */
    private int requesterNodeId;

    /**
     * Unique token or state recovery session identifier.
     * Helps synchronize asynchronous messages and prevents stale reconstruction requests
     * from being processed again.
     */
    private String recoverySessionId;

    /**
     * Timestamp of request creation.
     * Used to verify request freshness in a distributed environment.
     */
    private long timestamp;
}
