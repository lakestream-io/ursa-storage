/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.ToString;

@ToString
public class IcebergPartitionConfig {
    @JsonProperty("sourceColumn")  // Maps JSON key "sourceColumn" to this field
    private String sourceColumn;

    @JsonProperty("transform")
    private String transform;

    @JsonProperty("targetName")
    private String targetName;  // Optional

    // Default constructor (required for Jackson)
    public IcebergPartitionConfig() {}

    // Constructor for manual initialization
    public IcebergPartitionConfig(String sourceColumn, String transform, String targetName) {
        this.sourceColumn = sourceColumn;
        this.transform = transform;
        this.targetName = targetName;
    }

    // Getters and setters (required for Jackson)
    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public String getTransform() {
        return transform;
    }

    public void setTransform(String transform) {
        this.transform = transform;
    }

    public String getTargetName() {
        return targetName;
    }

    public void setTargetName(String targetName) {
        this.targetName = targetName;
    }
}
