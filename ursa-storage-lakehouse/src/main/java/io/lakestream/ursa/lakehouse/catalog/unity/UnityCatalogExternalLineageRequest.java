/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import com.databricks.sdk.service.catalog.SystemType;
import java.util.Objects;

public final class UnityCatalogExternalLineageRequest {

    private final String topicName;
    private final String tableFullName;
    private final String topicMetadataName;
    private final String clusterName;
    private final SystemType systemType;

    public UnityCatalogExternalLineageRequest(String topicName, String tableFullName,
                                              String topicMetadataName, String clusterName,
                                              SystemType systemType) {
        this.topicName = Objects.requireNonNull(topicName, "topicName must not be null");
        this.tableFullName = Objects.requireNonNull(tableFullName, "tableFullName must not be null");
        this.topicMetadataName = Objects.requireNonNull(topicMetadataName, "topicMetadataName must not be null");
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName must not be null");
        this.systemType = Objects.requireNonNull(systemType, "systemType must not be null");
    }

    public String getTopicName() {
        return topicName;
    }

    public String getTableFullName() {
        return tableFullName;
    }

    public String getTopicMetadataName() {
        return topicMetadataName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public SystemType getSystemType() {
        return systemType;
    }
}
