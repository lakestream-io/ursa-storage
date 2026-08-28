/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Map;

/**
 * Stream configuration — retention, cleanup policy, and other settings.
 *
 * @param properties configuration key-value pairs (e.g., "retention.ms", "cleanup.policy")
 */
public record StreamConfig(Map<String, String> properties) {

    /**
     * Creates a default stream config with no properties.
     */
    public StreamConfig() {
        this(Map.of());
    }
}
