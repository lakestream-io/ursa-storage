/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lakestream.api.CatalogPaths;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.lakestream.impl.materialization.MaterializationJson;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.UnaryOperator;

/** Owns the persisted indexed-stream config schema and its versioned update semantics. */
final class IndexedStreamConfigStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AsyncOxiaClient oxiaClient;
    private final CatalogPaths catalogPaths;

    IndexedStreamConfigStore(AsyncOxiaClient oxiaClient, CatalogPaths catalogPaths) {
        this.oxiaClient = Objects.requireNonNull(oxiaClient, "oxiaClient");
        this.catalogPaths = Objects.requireNonNull(catalogPaths, "catalogPaths");
    }

    CompletableFuture<Void> registerExternalStream(
            StreamIdentifier id, int partitionCount, Map<String, String> properties) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(properties, "properties");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        return registerExternalStreamAttempt(id, partitionCount, properties);
    }

    private CompletableFuture<Void> registerExternalStreamAttempt(
            StreamIdentifier id, int partitionCount, Map<String, String> creationProperties) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                StreamConfigData initial = new StreamConfigData(
                    partitionCount, creationProperties, Optional.empty());
                return put(path, initial, Set.of(PutOption.IfRecordDoesNotExist))
                    .handle((ignored, failure) -> failure)
                    .thenCompose(failure -> retryRegistrationAfterConflict(
                        id, partitionCount, creationProperties, failure));
            }

            StreamConfigData existing = parse(id, result.value());
            if (existing.partitions() >= partitionCount) {
                return CompletableFuture.completedFuture(null);
            }
            StreamConfigData grown = new StreamConfigData(
                partitionCount, existing.properties(), existing.materialization());
            return put(path, grown,
                Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((ignored, failure) -> failure)
                .thenCompose(failure -> retryRegistrationAfterConflict(
                    id, partitionCount, creationProperties, failure));
        });
    }

    private CompletableFuture<Void> retryRegistrationAfterConflict(
            StreamIdentifier id, int partitionCount, Map<String, String> creationProperties,
            Throwable failure) {
        if (failure == null) {
            return CompletableFuture.completedFuture(null);
        }
        Throwable cause = unwrap(failure);
        if (cause instanceof KeyAlreadyExistsException
                || cause instanceof UnexpectedVersionIdException) {
            return registerExternalStreamAttempt(id, partitionCount, creationProperties);
        }
        return CompletableFuture.failedFuture(cause);
    }

    CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        return oxiaClient.delete(catalogPaths.streamConfigPath(id)).thenApply(ignored -> null);
    }

    CompletableFuture<StreamConfigData> read(StreamIdentifier id) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenApply(result -> {
            if (result == null) {
                throw new NoSuchStreamException(id);
            }
            return parse(id, result.value());
        });
    }

    CompletableFuture<Boolean> exists(StreamIdentifier id) {
        return oxiaClient.get(catalogPaths.streamConfigPath(id)).thenApply(result -> result != null);
    }

    CompletableFuture<Void> write(StreamIdentifier id, int partitions,
                                  Map<String, String> properties,
                                  Optional<TableMaterializationPolicy> materialization) {
        return put(catalogPaths.streamConfigPath(id),
            new StreamConfigData(partitions, properties, materialization), null);
    }

    CompletableFuture<Void> setProperties(StreamIdentifier id, Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");
        return update(id, current -> {
            Map<String, String> merged = new HashMap<>(current.properties());
            merged.putAll(properties);
            return new StreamConfigData(
                current.partitions(), merged, current.materialization());
        });
    }

    CompletableFuture<Void> removeProperties(StreamIdentifier id, List<String> keys) {
        Objects.requireNonNull(keys, "keys");
        return update(id, current -> {
            Map<String, String> updated = new HashMap<>(current.properties());
            keys.forEach(updated::remove);
            return new StreamConfigData(
                current.partitions(), updated, current.materialization());
        });
    }

    CompletableFuture<Void> setMaterialization(
            StreamIdentifier id, Optional<TableMaterializationPolicy> materialization) {
        Objects.requireNonNull(materialization, "materialization");
        return update(id, current -> new StreamConfigData(
            current.partitions(), current.properties(), materialization));
    }

    private CompletableFuture<Void> update(
            StreamIdentifier id, UnaryOperator<StreamConfigData> mutation) {
        String path = catalogPaths.streamConfigPath(id);
        return oxiaClient.get(path).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(new NoSuchStreamException(id));
            }
            StreamConfigData updated = mutation.apply(parse(id, result.value()));
            return put(path, updated,
                Set.of(PutOption.IfVersionIdEquals(result.version().versionId())))
                .handle((ignored, failure) -> failure)
                .thenCompose(failure -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    Throwable cause = unwrap(failure);
                    if (cause instanceof UnexpectedVersionIdException) {
                        return update(id, mutation);
                    }
                    return CompletableFuture.failedFuture(cause);
                });
        });
    }

    private CompletableFuture<Void> put(
            String path, StreamConfigData config, Set<PutOption> options) {
        byte[] bytes;
        try {
            bytes = toBytes(config);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return options == null
            ? oxiaClient.put(path, bytes).thenApply(ignored -> null)
            : oxiaClient.put(path, bytes, options).thenApply(ignored -> null);
    }

    private static byte[] toBytes(StreamConfigData config) throws Exception {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("partitions", config.partitions());
        ObjectNode properties = node.putObject("properties");
        config.properties().forEach(properties::put);
        config.materialization().ifPresent(policy ->
            node.set("materialization", MaterializationJson.policyToJson(policy)));
        return MAPPER.writeValueAsBytes(node);
    }

    private static StreamConfigData parse(StreamIdentifier id, byte[] value) {
        try {
            JsonNode node = MAPPER.readTree(value);
            int partitions = node.get("partitions").asInt();
            Map<String, String> properties = node.has("properties")
                ? MAPPER.convertValue(node.get("properties"),
                    new TypeReference<Map<String, String>>() {})
                : Map.of();
            Optional<TableMaterializationPolicy> materialization =
                node.has("materialization") && !node.get("materialization").isNull()
                    ? Optional.of(MaterializationJson.policyFromJson(node.get("materialization")))
                    : Optional.empty();
            return new StreamConfigData(partitions, properties, materialization);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse stream config for: " + id.fullName(), e);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null
            ? failure.getCause() : failure;
    }

    record StreamConfigData(
            int partitions, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization) {
    }
}
