package com.example.node.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) representing a Bully algorithm message.
 * Defines a unified JSON data exchange contract between Java (Windows),
 * C# (Linux), and Python (macOS) environments.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ElectionMessage {

    /**
     * Identifier of the node sending this message, for example 1, 2, or 3.
     * Sent as a standard 32-bit int in the heterogeneous environment to avoid
     * numeric type size interpretation issues.
     */
    private int senderNodeId;

    /**
     * Bully algorithm message type.
     * Allowed and expected protocol values:
     * - "ELECTION"    : election initialization sent to nodes with higher IDs.
     * - "ANSWER"      : "OK" response from a higher node, stopping the sender from becoming leader.
     * - "COORDINATOR" : victory announcement and leader role takeover by the sender.
     */
    private String type;
}
