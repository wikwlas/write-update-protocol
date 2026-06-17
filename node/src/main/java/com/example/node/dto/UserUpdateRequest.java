package com.example.node.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) representing a client write request.
 * Used as the preferred JSON contract for /update-request.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserUpdateRequest {
    private String key;
    private String value;
}
