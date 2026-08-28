/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl.materialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lakestream.api.materialization.CommitConfig;
import io.lakestream.api.materialization.Compression;
import io.lakestream.api.materialization.ErrorHandling;
import io.lakestream.api.materialization.ErrorMode;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.FrameworkConf;
import io.lakestream.api.materialization.PartitionSpec;
import io.lakestream.api.materialization.PartitionTransform;
import io.lakestream.api.materialization.RetentionConfig;
import io.lakestream.api.materialization.SortColumn;
import io.lakestream.api.materialization.SortDirection;
import io.lakestream.api.materialization.StartPosition;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableConf;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.api.materialization.TableNaming;
import io.lakestream.api.materialization.WriteMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON (de)serialization helpers for materialization records.
 *
 * <p>The Lakestream API materialization records use {@link Optional} extensively
 * and Jackson's {@code jackson-datatype-jdk8} module is not on the classpath of
 * this module. We therefore translate each record into a Jackson {@link JsonNode}
 * shape that uses a nullable-field convention: a missing or {@code null} field
 * in JSON corresponds to {@link Optional#empty()} on the record.
 *
 * <p>Maps and lists are encoded as the natural JSON shapes. Backward compat:
 * deserializers tolerate missing fields and treat them as empty.
 */
public final class MaterializationJson {

    private static final JsonNodeFactory NF = JsonNodeFactory.instance;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STRING_MAP_TYPE =
            new TypeReference<>() {};

    private MaterializationJson() {
    }

    // --- TableCatalog ---

    public static ObjectNode tableCatalogToJson(TableCatalog catalog) {
        ObjectNode node = NF.objectNode();
        node.put("name", catalog.name());
        node.put("type", catalog.type().name());
        node.set("connection", stringMapToJson(catalog.connection()));
        node.set("properties", stringMapToJson(catalog.properties()));
        return node;
    }

    public static TableCatalog tableCatalogFromJson(JsonNode node) {
        String name = node.path("name").asText();
        TableCatalogType type = TableCatalogType.valueOf(node.path("type").asText());
        Map<String, String> connection = jsonToStringMap(node.get("connection"));
        Map<String, String> properties = jsonToStringMap(node.get("properties"));
        return new TableCatalog(name, type, connection, properties);
    }

    // --- TableMaterializationPolicy ---

    public static ObjectNode policyToJson(TableMaterializationPolicy policy) {
        ObjectNode node = NF.objectNode();
        policy.catalogRef().ifPresent(v -> node.put("catalogRef", v));
        policy.tableNaming().ifPresent(v -> node.set("tableNaming", tableNamingToJson(v)));
        policy.tableIdentifier()
                .ifPresent(v -> node.set("tableIdentifier", tableIdentifierToJson(v)));
        policy.enabled().ifPresent(v -> node.put("enabled", v));
        policy.framework().ifPresent(v -> node.set("framework", frameworkToJson(v)));
        policy.evolution().ifPresent(v -> node.set("evolution", evolutionToJson(v)));
        policy.primaryKey().ifPresent(v -> node.set("primaryKey", stringListToJson(v)));
        policy.baseSchemaVersion().ifPresent(v -> node.put("baseSchemaVersion", v));
        policy.table().ifPresent(v -> node.set("table", tableConfToJson(v)));
        node.set("connectionOverrides", stringMapToJson(policy.connectionOverrides()));
        return node;
    }

    public static TableMaterializationPolicy policyFromJson(JsonNode node) {
        Optional<String> catalogRef = optionalText(node, "catalogRef");
        Optional<TableNaming> tableNaming = optionalNode(node, "tableNaming")
                .map(MaterializationJson::tableNamingFromJson);
        Optional<TableIdentifier> tableIdentifier = optionalNode(node, "tableIdentifier")
                .map(MaterializationJson::tableIdentifierFromJson);
        Optional<Boolean> enabled = optionalBoolean(node, "enabled");
        Optional<FrameworkConf> framework = optionalNode(node, "framework")
                .map(MaterializationJson::frameworkFromJson);
        Optional<EvolutionPolicy> evolution = optionalNode(node, "evolution")
                .map(MaterializationJson::evolutionFromJson);
        Optional<List<String>> primaryKey = optionalNode(node, "primaryKey")
                .map(MaterializationJson::jsonToStringList);
        Optional<Long> baseSchemaVersion = optionalLong(node, "baseSchemaVersion");
        Optional<TableConf> table = optionalNode(node, "table")
                .map(MaterializationJson::tableConfFromJson);
        Map<String, String> connectionOverrides = node.has("connectionOverrides")
                ? jsonToStringMap(node.get("connectionOverrides"))
                : Map.of();
        return new TableMaterializationPolicy(catalogRef, tableNaming, tableIdentifier,
                enabled, framework, evolution, primaryKey, baseSchemaVersion, table,
                connectionOverrides);
    }

    // --- TableNaming ---

    private static ObjectNode tableNamingToJson(TableNaming naming) {
        ObjectNode node = NF.objectNode();
        naming.tableNamespacePrefix().ifPresent(v -> node.put("tableNamespacePrefix", v));
        node.put("tableNameTemplate", naming.tableNameTemplate());
        return node;
    }

    private static TableNaming tableNamingFromJson(JsonNode node) {
        Optional<String> prefix = optionalText(node, "tableNamespacePrefix");
        String template = node.path("tableNameTemplate").asText();
        return new TableNaming(prefix, template);
    }

    // --- TableIdentifier ---

    private static ObjectNode tableIdentifierToJson(TableIdentifier id) {
        ObjectNode node = NF.objectNode();
        node.put("namespace", id.namespace());
        node.put("name", id.name());
        return node;
    }

    private static TableIdentifier tableIdentifierFromJson(JsonNode node) {
        return new TableIdentifier(node.path("namespace").asText(), node.path("name").asText());
    }

    // --- FrameworkConf ---

    private static ObjectNode frameworkToJson(FrameworkConf framework) {
        ObjectNode node = NF.objectNode();
        framework.writeMode().ifPresent(v -> node.put("writeMode", v.name()));
        framework.startPosition().ifPresent(v -> node.put("startPosition", v.name()));
        framework.paused().ifPresent(v -> node.put("paused", v));
        framework.errorHandling().ifPresent(v -> node.set("errorHandling", errorHandlingToJson(v)));
        framework.commit().ifPresent(v -> node.set("commit", commitToJson(v)));
        return node;
    }

    private static FrameworkConf frameworkFromJson(JsonNode node) {
        Optional<WriteMode> writeMode = optionalText(node, "writeMode").map(WriteMode::valueOf);
        Optional<StartPosition> startPosition = optionalText(node, "startPosition")
                .map(StartPosition::valueOf);
        Optional<Boolean> paused = optionalBoolean(node, "paused");
        Optional<ErrorHandling> errorHandling = optionalNode(node, "errorHandling")
                .map(MaterializationJson::errorHandlingFromJson);
        Optional<CommitConfig> commit = optionalNode(node, "commit")
                .map(MaterializationJson::commitFromJson);
        return new FrameworkConf(writeMode, startPosition, paused, errorHandling, commit);
    }

    // --- ErrorHandling ---

    private static ObjectNode errorHandlingToJson(ErrorHandling handling) {
        ObjectNode node = NF.objectNode();
        node.put("mode", handling.mode().name());
        handling.dlqTopic().ifPresent(v -> node.put("dlqTopic", v));
        return node;
    }

    private static ErrorHandling errorHandlingFromJson(JsonNode node) {
        ErrorMode mode = ErrorMode.valueOf(node.path("mode").asText());
        Optional<String> dlqTopic = optionalText(node, "dlqTopic");
        return new ErrorHandling(mode, dlqTopic);
    }

    // --- CommitConfig ---

    private static ObjectNode commitToJson(CommitConfig commit) {
        ObjectNode node = NF.objectNode();
        commit.maxRetries().ifPresent(v -> node.put("maxRetries", v));
        commit.retryDelayMs().ifPresent(v -> node.put("retryDelayMs", v));
        commit.batchSize().ifPresent(v -> node.put("batchSize", v));
        return node;
    }

    private static CommitConfig commitFromJson(JsonNode node) {
        return new CommitConfig(
                optionalInt(node, "maxRetries"),
                optionalLong(node, "retryDelayMs"),
                optionalInt(node, "batchSize"));
    }

    // --- EvolutionPolicy ---

    private static ObjectNode evolutionToJson(EvolutionPolicy evolution) {
        ObjectNode node = NF.objectNode();
        evolution.addColumn().ifPresent(v -> node.put("addColumn", v));
        evolution.addNullableColumn().ifPresent(v -> node.put("addNullableColumn", v));
        evolution.dropColumn().ifPresent(v -> node.put("dropColumn", v));
        evolution.widenType().ifPresent(v -> node.put("widenType", v));
        evolution.narrowType().ifPresent(v -> node.put("narrowType", v));
        evolution.renameColumn().ifPresent(v -> node.put("renameColumn", v));
        evolution.reorderColumns().ifPresent(v -> node.put("reorderColumns", v));
        evolution.nullabilityRelax().ifPresent(v -> node.put("nullabilityRelax", v));
        evolution.nullabilityTighten().ifPresent(v -> node.put("nullabilityTighten", v));
        return node;
    }

    private static EvolutionPolicy evolutionFromJson(JsonNode node) {
        return new EvolutionPolicy(
                optionalBoolean(node, "addColumn"),
                optionalBoolean(node, "addNullableColumn"),
                optionalBoolean(node, "dropColumn"),
                optionalBoolean(node, "widenType"),
                optionalBoolean(node, "narrowType"),
                optionalBoolean(node, "renameColumn"),
                optionalBoolean(node, "reorderColumns"),
                optionalBoolean(node, "nullabilityRelax"),
                optionalBoolean(node, "nullabilityTighten"));
    }

    // --- TableConf ---

    private static ObjectNode tableConfToJson(TableConf table) {
        ObjectNode node = NF.objectNode();
        table.mode().ifPresent(v -> node.put("mode", v.name()));
        table.partitionBy().ifPresent(v -> {
            ArrayNode arr = NF.arrayNode();
            for (PartitionSpec spec : v) {
                arr.add(partitionSpecToJson(spec));
            }
            node.set("partitionBy", arr);
        });
        table.sortBy().ifPresent(v -> {
            ArrayNode arr = NF.arrayNode();
            for (SortColumn sort : v) {
                arr.add(sortColumnToJson(sort));
            }
            node.set("sortBy", arr);
        });
        table.retention().ifPresent(v -> node.set("retention", retentionToJson(v)));
        table.targetFileSizeBytes().ifPresent(v -> node.put("targetFileSizeBytes", v));
        table.compression().ifPresent(v -> node.put("compression", v.name()));
        return node;
    }

    private static TableConf tableConfFromJson(JsonNode node) {
        Optional<TableMode> mode = optionalText(node, "mode").map(TableMode::valueOf);
        Optional<List<PartitionSpec>> partitionBy = optionalArray(node, "partitionBy").map(arr -> {
            List<PartitionSpec> specs = new ArrayList<>();
            arr.forEach(spec -> specs.add(partitionSpecFromJson(spec)));
            return List.copyOf(specs);
        });
        Optional<List<SortColumn>> sortBy = optionalArray(node, "sortBy").map(arr -> {
            List<SortColumn> sorts = new ArrayList<>();
            arr.forEach(sort -> sorts.add(sortColumnFromJson(sort)));
            return List.copyOf(sorts);
        });
        Optional<RetentionConfig> retention = optionalNode(node, "retention")
                .map(MaterializationJson::retentionFromJson);
        Optional<Long> targetFileSizeBytes = optionalLong(node, "targetFileSizeBytes");
        Optional<Compression> compression = optionalText(node, "compression")
                .map(Compression::valueOf);
        return new TableConf(mode, partitionBy, sortBy, retention, targetFileSizeBytes,
                compression);
    }

    // --- PartitionSpec ---

    private static ObjectNode partitionSpecToJson(PartitionSpec spec) {
        ObjectNode node = NF.objectNode();
        node.put("column", spec.column());
        node.put("transform", spec.transform().name());
        spec.parameter().ifPresent(v -> node.put("parameter", v));
        return node;
    }

    private static PartitionSpec partitionSpecFromJson(JsonNode node) {
        return new PartitionSpec(
                node.path("column").asText(),
                PartitionTransform.valueOf(node.path("transform").asText()),
                optionalText(node, "parameter"));
    }

    // --- SortColumn ---

    private static ObjectNode sortColumnToJson(SortColumn sort) {
        ObjectNode node = NF.objectNode();
        node.put("column", sort.column());
        node.put("direction", sort.direction().name());
        node.put("nullsFirst", sort.nullsFirst());
        return node;
    }

    private static SortColumn sortColumnFromJson(JsonNode node) {
        return new SortColumn(
                node.path("column").asText(),
                SortDirection.valueOf(node.path("direction").asText()),
                node.path("nullsFirst").asBoolean(false));
    }

    // --- RetentionConfig ---

    private static ObjectNode retentionToJson(RetentionConfig retention) {
        ObjectNode node = NF.objectNode();
        retention.snapshotRetentionMs().ifPresent(v -> node.put("snapshotRetentionMs", v));
        retention.maxSnapshots().ifPresent(v -> node.put("maxSnapshots", v));
        retention.rowRetentionMs().ifPresent(v -> node.put("rowRetentionMs", v));
        return node;
    }

    private static RetentionConfig retentionFromJson(JsonNode node) {
        return new RetentionConfig(
                optionalLong(node, "snapshotRetentionMs"),
                optionalInt(node, "maxSnapshots"),
                optionalLong(node, "rowRetentionMs"));
    }

    // --- Primitive helpers ---

    private static ObjectNode stringMapToJson(Map<String, String> map) {
        ObjectNode node = NF.objectNode();
        Map<String, String> ordered = new LinkedHashMap<>(map);
        ordered.forEach(node::put);
        return node;
    }

    private static ArrayNode stringListToJson(List<String> list) {
        ArrayNode arr = NF.arrayNode();
        list.forEach(arr::add);
        return arr;
    }

    private static Map<String, String> jsonToStringMap(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return Map.of();
        }
        try {
            Map<String, String> result = MAPPER.convertValue(node, STRING_MAP_TYPE);
            return Map.copyOf(result);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Invalid string-map node: " + node, e);
        }
    }

    private static List<String> jsonToStringList(JsonNode node) {
        List<String> result = new ArrayList<>();
        if (node != null && node.isArray()) {
            for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
                result.add(it.next().asText());
            }
        }
        return result;
    }

    private static Optional<String> optionalText(JsonNode parent, String field) {
        JsonNode value = parent.get(field);
        if (value == null || value.isNull()) {
            return Optional.empty();
        }
        return Optional.of(value.asText());
    }

    private static Optional<Boolean> optionalBoolean(JsonNode parent, String field) {
        JsonNode value = parent.get(field);
        if (value == null || value.isNull()) {
            return Optional.empty();
        }
        return Optional.of(value.asBoolean());
    }

    private static Optional<Integer> optionalInt(JsonNode parent, String field) {
        JsonNode value = parent.get(field);
        if (value == null || value.isNull()) {
            return Optional.empty();
        }
        return Optional.of(value.asInt());
    }

    private static Optional<Long> optionalLong(JsonNode parent, String field) {
        JsonNode value = parent.get(field);
        if (value == null || value.isNull()) {
            return Optional.empty();
        }
        return Optional.of(value.asLong());
    }

    private static Optional<JsonNode> optionalNode(JsonNode parent, String field) {
        JsonNode value = parent.get(field);
        if (value == null || value.isNull() || value.isMissingNode()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    private static Optional<JsonNode> optionalArray(JsonNode parent, String field) {
        JsonNode value = parent.get(field);
        if (value == null || value.isNull() || !value.isArray()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }
}
