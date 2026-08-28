/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import com.databricks.sdk.service.catalog.SystemType;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration.LakehouseType;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.utils.TableNameFormatUtils;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public final class UnityCatalogLineageUtil {

    private UnityCatalogLineageUtil() {
    }

    public static List<UnityCatalogExternalLineageRequest> buildRequests(LakehouseConfiguration config,
                                                                         String topicName,
                                                                         Optional<String> catalogName) {
        List<UnityCatalogExternalLineageRequest> requests = new ArrayList<>();
        LakehouseType lakehouseType = config.getLakehouseType();
        if (lakehouseType == LakehouseType.DELTA || lakehouseType == LakehouseType.DELTA_AND_ICEBERG) {
            buildDeltaRequest(config, topicName).ifPresent(requests::add);
        }
        if (lakehouseType == LakehouseType.ICEBERG || lakehouseType == LakehouseType.DELTA_AND_ICEBERG) {
            buildIcebergRequest(config, topicName, catalogName).ifPresent(requests::add);
        }
        return requests;
    }

    public static Optional<UnityCatalogExternalLineageRequest> buildDeltaRequest(LakehouseConfiguration config,
                                                                                 String topicName) {
        UnityTableIdentifier identifier = UnityTableIdentifier.parse(topicName);
        String unityCatalogName = config.getUnityCatalogName();
        if (StringUtils.isBlank(unityCatalogName)) {
            return Optional.empty();
        }

        return Optional.of(new UnityCatalogExternalLineageRequest(
                topicName,
                identifier.getTableFullName(unityCatalogName),
                "lakestream_ursa_" + identifier.getSchema() + "_" + identifier.getTable(),
                clusterName(config),
                resolveSystemType(config.getUnityCatalogByolSystemType())));
    }

    public static Optional<UnityCatalogExternalLineageRequest> buildIcebergRequest(LakehouseConfiguration config,
                                                                                   String topicName,
                                                                                   Optional<String> catalogName) {
        Map<String, String> icebergProps = config.getIcebergProperties(catalogName);
        if (!IcebergTable.ICEBERG_CATALOG_TYPE_UNITYCATALOG
                .equalsIgnoreCase(String.valueOf(config.getIcebergCatalogBackendType(catalogName)))) {
            return Optional.empty();
        }

        String icebergUri = icebergProps.get("uri");
        if (StringUtils.isBlank(icebergUri)) {
            return Optional.empty();
        }

        TopicName parsedTopic = TopicName.getPartitionedTopicName(topicName);
        String[] namespaceLevels = new String[]{parsedTopic.getNamespace()};
        String cluster = clusterName(config);
        String ns = java.util.stream.Stream
                .concat(java.util.stream.Stream.of(cluster), java.util.Arrays.stream(namespaceLevels))
                .map(TableNameFormatUtils::formatUnityCatalogIcebergNamespaceName)
                .reduce((left, right) -> left + "_" + right)
                .orElseThrow();
        String tableName = TableNameFormatUtils.formatUnityCatalogIcebergTableName(parsedTopic.getLocalName());
        String icebergCatalogName = getIcebergUnityCatalogName(config, icebergProps);
        String tableFullName = icebergCatalogName + "." + ns + "." + tableName;
        String topicMetadataName = "lakestream_ursa_" + ns + "_" + tableName;

        return Optional.of(new UnityCatalogExternalLineageRequest(
                topicName, tableFullName, topicMetadataName, cluster,
                resolveSystemType(config.getUnityCatalogByolSystemType())));
    }

    private static String clusterName(LakehouseConfiguration config) {
        return StringUtils.defaultIfBlank(config.getProperties().getProperty("clusterName"), "ursa");
    }

    private static String getIcebergUnityCatalogName(LakehouseConfiguration config, Map<String, String> icebergProps) {
        String warehouse = icebergProps.get("warehouse");
        if (StringUtils.isNotBlank(warehouse)) {
            return warehouse;
        }
        return config.getUnityCatalogName();
    }

    private static SystemType resolveSystemType(String systemTypeStr) {
        try {
            return SystemType.valueOf(systemTypeStr);
        } catch (Exception e) {
            return SystemType.KAFKA;
        }
    }
}
