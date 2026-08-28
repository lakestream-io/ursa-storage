/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.FIXED_PARTITION_KEY;
import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.NONE_PARTITION_KEY;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

@Slf4j
@Data
@Builder
public class TableOptions {
    // the table schema
    private final Schema schema;
    private final String location;
    private final Map<String, String> properties;
    private final Set<String> identifierFields;
    private final String partitionKey;

    // the processed schema should ensure the identifier fields in the schema if configured.
    private final Schema processedSchema;
    private final IcebergPartitionSpec partitionSpec;

    @Builder
    private TableOptions(Schema schema, String location, Map<String, String> properties,
                         Set<String> identifierFields, String partitionKey, Schema processedSchema,
                         IcebergPartitionSpec partitionSpec) {
        this.schema = schema;
        this.location = location;
        this.properties = properties != null ? properties : new HashMap<>();
        this.identifierFields = identifierFields != null ? identifierFields : new HashSet<>();
        this.partitionKey = partitionKey != null ? partitionKey : "";

        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }

        this.processedSchema = buildProcessedSchema();
        this.partitionSpec = partitionSpec != null ? partitionSpec : buildPartitionSpec();
    }

    public Schema getSchema() {
        return processedSchema;
    }

    public IcebergPartitionSpec getPartitionSpec() {
        return partitionSpec;
    }

    // When create table with the schema, we need to ensure the identifier fields in the schema.
    private Schema buildProcessedSchema() {
        if (identifierFields.isEmpty()) {
            return schema;
        }

        var fields = schema.asStruct().fields();
        Set<Integer> identifierFieldIds = fields.stream()
            .filter(f -> identifierFields.contains(f.name()) && f.isRequired())
            .map(Types.NestedField::fieldId)
            .collect(Collectors.toSet());

        return new Schema(fields, identifierFieldIds);
    }

    private IcebergPartitionSpec buildPartitionSpec() {
        List<IcebergPartitionConfig> partitionConfigs = getPartitionConfig();
        return Utilities.buildPartitionSpec(processedSchema, partitionConfigs);
    }

    private List<IcebergPartitionConfig> getPartitionConfig() {
        if (NONE_PARTITION_KEY.equalsIgnoreCase(partitionKey)
            || FIXED_PARTITION_KEY.equalsIgnoreCase(partitionKey)) {
            return Collections.emptyList();
        } else {
            try {
                return IcebergPartitionConfigLoader.loadFromJson(partitionKey);
            } catch (IOException e) {
                log.warn("Failed to parse json to load partition config: {}, use non partition key instead.",
                        partitionKey);
                return Collections.emptyList();
            }
        }
    }
}
