/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.lakestream.ursa.json.UrsaObjectMapperFactory;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.StreamProperties;
import io.lakestream.ursa.storage.Utils;
import io.lakestream.ursa.storage.impl.StorageFormat;
import io.lakestream.ursa.storage.impl.utils.RangeScanConsumerImpl;
import io.lakestream.ursa.utils.FutureUtils;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import picocli.CommandLine;

/**
 * Migrate stream-id entries from empty value to stream metadata with JSON format.
 *
 * <p>Before: entries like {@code /stream-id/1 -> new byte[0]} (empty value).
 * After: same key with value {@code {"key": "analytics/orders"}}.
 *
 * <p>Keys are opaque stream names interpreted by the integration that created them.
 */
@CommandLine.Command(
    name = "migrate-stream-id",
    description = "Migrate stream-id entries from empty value to stream metadata with JSON format"
)
public class MigrateStreamID implements Callable<Integer> {

    @CommandLine.ParentCommand
    private Admin parent;

    private Map<String, Long> getKeyToStreamIdMap(AsyncOxiaClient oxiaClient) {
        var key  = StorageFormat.STREAM_ID_GENERATOR_PATH + "/";
        var rangeScan = new RangeScanConsumerImpl();

        var keyRange = Utils.generateKeyRange(key, 4);

        oxiaClient.rangeScan(
                keyRange.getLeft(),
                keyRange.getRight(),
                rangeScan,
                Set.of(RangeScanOption.PartitionKey(StorageFormat.STREAM_ID_GENERATOR_PATH)));

        return rangeScan.getFuture().thenApply(results ->
                results.stream()
                        .collect(Collectors.toMap(
                                r -> r.key().substring(key.length()),
                                r -> Long.parseLong(new String(r.value(), StandardCharsets.UTF_8))
                        ))
        ).join();
    }

    private final ObjectReader streamPropertiesReader =
            UrsaObjectMapperFactory.getMapper().readerFor(StreamProperties.class);
    private final ObjectWriter streamPropertiesWriter =
            UrsaObjectMapperFactory.getMapper().writerFor(StreamProperties.class);

    private CompletableFuture<Void> checkAndUpdateStreamId(AsyncOxiaClient oxiaClient, long streamId, String key) {
        var k = StorageFormat.STREAM_REGISTER_PATH + "/" + streamId;
        return oxiaClient.get(k).thenCompose((getResult) -> {
            if (getResult == null) {
                System.err.println(
                        "Warning: Stream ID " + streamId + " does not exist. Skipping migration for this stream.");
                return CompletableFuture.completedFuture(null);
            }
            try {
                streamPropertiesReader.readValue(getResult.value());
                return CompletableFuture.completedFuture(null);
            } catch (IOException ignored) {
            }
            try {
                return oxiaClient.put(k, streamPropertiesWriter.writeValueAsBytes(new StreamProperties(key)),
                                Set.of(PutOption.IfVersionIdEquals(getResult.version().versionId())))
                        .thenRun(() -> System.out.println("Successfully migrated stream ID " + streamId + " -> " + key))
                        .exceptionally(throwable -> {
                            var e = FutureUtils.unwrapCompletionException(throwable);
                            if (e instanceof UnexpectedVersionIdException) {
                                System.err.println("Warning: Version conflict detected for stream ID " + streamId
                                        + ". The entry was modified by another process. Please retry the migration.");
                            }
                            throw new CompletionException(e);
                        });
            } catch (IOException e) {
                return CompletableFuture.failedFuture(e);
            }
        });
    }

    @Override
    public Integer call() throws Exception {
        StorageApi storageApi = Admin.initializeStorage(parent.getConfigFile());
        try (storageApi) {
            return execute(storageApi);
        } catch (Exception e) {
            System.err.println("Error: Failed to migrate stream IDs: " + e.getMessage());
            e.printStackTrace();
            return 1;
        } finally {
            Admin.cleanup();
        }
    }

    Integer execute(StorageApi storageApi) {
        var oxiaClient = storageApi.getStorageOxiaClient();
        var keyToStreamIdMap = getKeyToStreamIdMap(oxiaClient);
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (var entry : keyToStreamIdMap.entrySet()) {
            futures.add(checkAndUpdateStreamId(oxiaClient, entry.getValue(), entry.getKey()));
        }
        FutureUtils.waitForAll(futures).join();
        return 0;
    }
}
