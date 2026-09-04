/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import com.databricks.sdk.service.catalog.SystemType;
import io.lakestream.api.SourceMetadataProperties;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration.LakehouseType;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.utils.StreamTableNaming;
import io.lakestream.ursa.lakehouse.utils.TableNameFormatUtils;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
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
        String sourceTopic = sourceTopic(config, topicName);
        UnityTableIdentifier sourceIdentifier = UnityTableIdentifier.parse(sourceTopic);
        TableIdentifier resolvedTable = StreamTableNaming.resolve(topicName, config.getProperties());
        UnityTableIdentifier tableIdentifier = UnityTableIdentifier.parse(
                StreamTableNaming.qualifiedName(resolvedTable));
        String unityCatalogName = config.getUnityCatalogName();
        if (StringUtils.isBlank(unityCatalogName)) {
            return Optional.empty();
        }

        return Optional.of(new UnityCatalogExternalLineageRequest(
                sourceTopic,
                tableIdentifier.getTableFullName(unityCatalogName),
                "lakestream_ursa_" + sourceIdentifier.getSchema() + "_" + sourceIdentifier.getTable(),
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

        String sourceTopic = sourceTopic(config, topicName);
        TopicName parsedSource = TopicName.getPartitionedTopicName(sourceTopic);
        TableIdentifier resolvedTable = StreamTableNaming.resolve(topicName, config.getProperties());
        String cluster = clusterName(config);
        String tableNamespace = unityIcebergNamespace(cluster, resolvedTable.namespace());
        String tableName = TableNameFormatUtils.formatUnityCatalogIcebergTableName(resolvedTable.name());
        String icebergCatalogName = getIcebergUnityCatalogName(config, icebergProps);
        String tableFullName = icebergCatalogName + "." + tableNamespace + "." + tableName;
        String sourceNamespace = unityIcebergNamespace(cluster, parsedSource.getNamespace());
        String sourceName = TableNameFormatUtils.formatUnityCatalogIcebergTableName(parsedSource.getLocalName());
        String topicMetadataName = "lakestream_ursa_" + sourceNamespace + "_" + sourceName;

        return Optional.of(new UnityCatalogExternalLineageRequest(
                sourceTopic, tableFullName, topicMetadataName, cluster,
                resolveSystemType(config.getUnityCatalogByolSystemType())));
    }

    private static String sourceTopic(LakehouseConfiguration config, String fallback) {
        String logicalName = config.getProperties().getProperty(SourceMetadataProperties.LOGICAL_NAME_PROPERTY);
        if (StringUtils.isNotBlank(logicalName)) {
            return logicalName;
        }
        String legacyKafkaTopic = config.getProperties().getProperty("lakestream.kafka.topic.name");
        return StringUtils.defaultIfBlank(legacyKafkaTopic, fallback);
    }

    private static String unityIcebergNamespace(String cluster, String namespace) {
        return Stream.of(cluster, namespace)
                .map(TableNameFormatUtils::formatUnityCatalogIcebergNamespaceName)
                .reduce((left, right) -> left + "_" + right)
                .orElseThrow();
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
