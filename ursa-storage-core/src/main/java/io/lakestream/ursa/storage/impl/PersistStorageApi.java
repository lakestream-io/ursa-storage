/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageFormat.FIRST_UNCOMPACTED_OFFSET;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_ID_GENERATOR_PATH;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_ID_GENERATOR_VALUE;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_REGISTER_PATH;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogState;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.Position;
import io.lakestream.ursa.json.UrsaObjectMapperFactory;
import io.lakestream.ursa.metrics.Counter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.metrics.Unit;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.Key;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.StreamProperties;
import io.lakestream.ursa.storage.Value;
import io.lakestream.ursa.storage.WalStorage;
import io.lakestream.ursa.storage.impl.exception.NoSuchKeyException;
import io.lakestream.ursa.storage.impl.exception.NoSuchOffsetException;
import io.lakestream.ursa.storage.impl.exception.StreamPropertiesSerDeException;
import io.lakestream.ursa.storage.impl.exception.WalFileAlreadyCompactedException;
import io.lakestream.ursa.storage.impl.utils.RangeScanConsumerImpl;
import io.lakestream.ursa.storage.proto.IndexType;
import io.lakestream.ursa.utils.FutureUtils;
import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class PersistStorageApi implements StorageApi {

    private static final int MAX_CONDITIONAL_MAPPING_DELETE_RETRIES = 3;
    private static final long CONDITIONAL_MAPPING_DELETE_RETRY_DELAY_MS = 10L;
    private static final int MAX_KEYED_ALLOCATION_RETRIES = 3;
    private static final long KEYED_ALLOCATION_RETRY_DELAY_MS = 10L;

    private final StorageFormat storageFormat;
    private final AsyncOxiaClient oxiaClient;
    private final WalStorage storage;

    // stream storage async cleaner process
    private final AsyncCleaner asyncCleaner;

    private final int defaultReadBatchContextInitializeSize;

    // otel metrics
    private final LatencyHistogram readEntryMetadataLatencyHistogram;
    private final LatencyHistogram addEntryLatencyHistogram;
    private final LatencyHistogram readEntryLatencyHistogram;
    private final LatencyHistogram readEntriesIndexDuration;
    private final LatencyHistogram readEntriesDataDuration;
    private final Counter addMessagesCounter;
    private final Counter addMessagesSizeCounter;
    private final Counter readEntryCounter;
    private final Counter readEntrySizeCounter;
    private final Counter lessThanBatchSizeReadCounter;
    private final LogStateManager streamStateManager;

    public PersistStorageApi(StorageConfig config, AsyncOxiaClient oxiaClient, WalStorage storage,
                             InstrumentProvider instrumentProvider, StorageFormat storageFormat,
                             LogStateManager streamStateManager) {
        this.storageFormat = storageFormat;
        this.oxiaClient = oxiaClient;
        this.storage = storage;
        this.asyncCleaner = new AsyncCleaner(this, storage, config);
        this.defaultReadBatchContextInitializeSize = config.getDefaultReadBatchContextInitializeSize();

        // otel metrics
        readEntryMetadataLatencyHistogram =
            instrumentProvider.newLatencyHistogram("ursa.storage.read.entrymetadata.duration",
                "PersistentStorageAPi read entry metadata latency", Attributes.empty());
        addEntryLatencyHistogram = instrumentProvider.newLatencyHistogram("ursa.storage.add.entry.duration",
            "PersistentStorageApi add entry latency", Attributes.empty());
        readEntryLatencyHistogram = instrumentProvider.newLatencyHistogram("ursa.storage.read.entry.duration",
            "PersistentStorageApi read entry latency", Attributes.empty());
        addMessagesCounter = instrumentProvider.newCounter("ursa.storage.add.messages.count", Unit.Messages,
            "PersistentStorageApi add messages count", Attributes.empty());
        addMessagesSizeCounter = instrumentProvider.newCounter("ursa.storage.add.messages.size", Unit.Bytes,
            "PersistentStorageApi add messages size", Attributes.empty());
        readEntryCounter = instrumentProvider.newCounter("ursa.storage.read.entry.count", Unit.Messages,
            "PersistentStorageApi read entry count", Attributes.empty());
        readEntrySizeCounter = instrumentProvider.newCounter("ursa.storage.read.entry.size", Unit.Bytes,
            "PersistentStorageApi read entry size", Attributes.empty());
        readEntriesIndexDuration = instrumentProvider.newLatencyHistogram("ursa.storage.read.entries.index.duration",
            "PersistentStorageApi read entries index duration", Attributes.empty());
        readEntriesDataDuration = instrumentProvider.newLatencyHistogram("ursa.storage.read.entries.data.duration",
            "PersistentStorageApi read entries data duration", Attributes.empty());
        lessThanBatchSizeReadCounter = instrumentProvider.newCounter(
            "ursa.storage.read.entries.lessthanbatchsize.count", Unit.Messages,
            "PersistentStorageApi read entries less than batch size count", Attributes.empty());
        this.streamStateManager = streamStateManager;
    }

    private int indexSerializeFormatVersion() {
        return storageFormat.getIndexSerializeFormatVersion();
    }

    private String getStreamIdKey(long streamId) {
        return storageFormat.getStreamIdKey(streamId);
    }

    private String getLargestStreamIdKey(long streamId) {
        return storageFormat.getLargestStreamIdKey(streamId);
    }

    private String getSmallestStreamIdKey(long streamId) {
        return storageFormat.getSmallestStreamIdKey(streamId);
    }

    private static final ObjectMapper OBJECT_MAPPER = UrsaObjectMapperFactory.getMapper();
    private static final ObjectReader STREAM_PROPERTIES_READER =
            OBJECT_MAPPER.readerFor(StreamProperties.class);
    private static final ObjectWriter STREAM_PROPERTIES_WRITER =
            OBJECT_MAPPER.writerFor(StreamProperties.class);

    @VisibleForTesting
    static StreamProperties deserializeStreamProperties(byte[] value) throws IOException {
        return STREAM_PROPERTIES_READER.readValue(value);
    }

    private CompletableFuture<Long> registerStream(long streamId, String key) {
        StreamProperties value = new StreamProperties(key);
        try {
            return oxiaClient.put(STREAM_REGISTER_PATH + "/" + streamId,
                            STREAM_PROPERTIES_WRITER.writeValueAsBytes(value),
                            Set.of(PutOption.IfRecordDoesNotExist))
                    .thenApply(result -> streamId);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(
                    new StreamPropertiesSerDeException("Failed to register stream id: " + key, e));
        }
    }

    private CompletableFuture<Boolean> removeStream(long streamId) {
        return oxiaClient.delete(STREAM_REGISTER_PATH + "/" + streamId);
    }

    private CompletableFuture<Void> putUncompactedOffset(long streamId, long offset) {
        return oxiaClient.put(FIRST_UNCOMPACTED_OFFSET + "/" + streamId,
                Long.toString(offset).getBytes(StandardCharsets.UTF_8))
            .thenApply(x -> null);
    }

    private CompletableFuture<Long> getUncompactedOffset(long streamId) {
        return oxiaClient.get(FIRST_UNCOMPACTED_OFFSET + "/" + streamId)
            .thenApply(result -> {
                if (result == null) {
                    return -1L;
                }
                return Long.parseLong(new String(result.value(), StandardCharsets.UTF_8));
            });
    }

    @Override
    public CompletableFuture<Long> generateStreamId(Optional<String> key) {
        return allocateStreamId(key).thenApply(StreamIdAllocation::streamId);
    }

    @Override
    public CompletableFuture<StreamIdAllocation> allocateStreamId(Optional<String> key) {
        return internalAllocateStreamId(key, 0).thenApply(allocation -> {
            streamStateManager.setState(allocation.streamId(), LogState.NORMAL);
            return allocation;
        });
    }

    @Override
    public CompletableFuture<Long> getStreamIdByKey(String key) {
        String keyPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        return oxiaClient.get(keyPath, Set.of(GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH)))
            .thenCompose(result -> {
                if (result == null) {
                    return CompletableFuture.failedFuture(
                        new CompletionException(new NoSuchKeyException("No stream id found for key: " + key)));
                }
                return CompletableFuture.completedFuture(
                        Long.parseLong(new String(result.value(), StandardCharsets.UTF_8)));
            });
    }

    @Override
    public CompletableFuture<Void> deleteStreamIdMapping(String key) {
        return oxiaClient.delete(STREAM_ID_GENERATOR_PATH + "/" + key,
                Set.of(DeleteOption.PartitionKey(STREAM_ID_GENERATOR_PATH)))
            .thenApply(__ -> null);
    }

    @Override
    public CompletableFuture<Void> deleteStreamIdMapping(String key, long expectedStreamId) {
        return deleteStreamIdMapping(key, expectedStreamId, 0);
    }

    @Override
    public boolean supportsConditionalStreamIdMappingDeletion() {
        return true;
    }

    private CompletableFuture<Void> deleteStreamIdMapping(
            String key, long expectedStreamId, int retryAttempt) {
        String keyPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        Set<GetOption> getOptions = Set.of(GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        return oxiaClient.get(keyPath, getOptions).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.completedFuture(null);
            }
            long currentStreamId = Long.parseLong(
                new String(result.value(), StandardCharsets.UTF_8));
            if (currentStreamId != expectedStreamId) {
                return CompletableFuture.completedFuture(null);
            }
            return oxiaClient.delete(keyPath, Set.of(
                    DeleteOption.PartitionKey(STREAM_ID_GENERATOR_PATH),
                    DeleteOption.IfVersionIdEquals(result.version().versionId())))
                .handle((deleted, failure) -> new ConditionalDeleteResult(
                    Boolean.TRUE.equals(deleted), failure == null ? null
                        : FutureUtils.unwrapCompletionException(failure)))
                .thenCompose(delete -> {
                    if (delete.deleted()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (delete.failure() instanceof UnexpectedVersionIdException) {
                        if (retryAttempt >= MAX_CONDITIONAL_MAPPING_DELETE_RETRIES) {
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                "Conditional keyed stream-ID delete exhausted retries for "
                                    + keyPath, delete.failure()));
                        }
                        long delayMillis = CONDITIONAL_MAPPING_DELETE_RETRY_DELAY_MS
                            << retryAttempt;
                        log.warn("Retrying conditional keyed stream-ID delete for {} after "
                                + "a version conflict (attempt {}/{})", keyPath,
                            retryAttempt + 1, MAX_CONDITIONAL_MAPPING_DELETE_RETRIES);
                        return CompletableFuture.runAsync(
                                () -> { }, CompletableFuture.delayedExecutor(
                                    delayMillis, TimeUnit.MILLISECONDS))
                            .thenCompose(ignored -> deleteStreamIdMapping(
                                key, expectedStreamId, retryAttempt + 1));
                    }
                    if (delete.failure() != null) {
                        return CompletableFuture.failedFuture(delete.failure());
                    }
                    return verifyConditionalMappingDelete(keyPath, getOptions, expectedStreamId);
                });
        });
    }

    private CompletableFuture<Void> verifyConditionalMappingDelete(
            String keyPath, Set<GetOption> getOptions, long expectedStreamId) {
        return oxiaClient.get(keyPath, getOptions).thenCompose(current -> {
            if (current == null) {
                return CompletableFuture.completedFuture(null);
            }
            long currentStreamId = Long.parseLong(
                new String(current.value(), StandardCharsets.UTF_8));
            if (currentStreamId != expectedStreamId) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.failedFuture(new IllegalStateException(
                "Conditional keyed stream-ID delete returned false for " + keyPath));
        });
    }

    private record ConditionalDeleteResult(boolean deleted, Throwable failure) {
    }

    @Override
    public CompletableFuture<Map<Long, StreamProperties>> listStreamsWithProperties() {
        String prefix = STREAM_REGISTER_PATH + "/";
        var rangeScan = new RangeScanConsumerImpl();

        oxiaClient.rangeScan(prefix, prefix + "/", rangeScan);

        return rangeScan.getFuture().thenApply(results ->
                results.stream()
                        .collect(Collectors.toMap(
                                r -> Long.parseLong(r.key().substring(prefix.length())),
                                r -> {
                                    try {
                                        return deserializeStreamProperties(r.value());
                                    } catch (IOException e) {
                                        throw new StreamPropertiesSerDeException(
                                                "Failed to deserialize stream properties for key: " + r.key(), e);
                                    }
                                }
                        ))
        );
    }


    private CompletableFuture<StreamIdAllocation> internalAllocateStreamId(
            Optional<String> key, int retryAttempt) {
        if (key.isPresent()) {
            String streamKey = key.orElseThrow();
            String keyPath = STREAM_ID_GENERATOR_PATH + "/" + streamKey;
            return oxiaClient.get(keyPath, Set.of(GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH)))
                .thenCompose(result -> {
                    if (result == null) {
                        return generateId()
                            .thenCompose(streamId -> createKeyedStreamIdMapping(
                                streamKey, keyPath, streamId, retryAttempt));
                    }
                    long streamId = Long.parseLong(
                        new String(result.value(), StandardCharsets.UTF_8));
                    return completeKeyedAllocation(
                        streamId, streamKey, keyPath, false);
                });
        }
        return generateId().thenCompose(streamId -> registerStream(streamId, null)
            .thenApply(registeredStreamId ->
                new StreamIdAllocation(registeredStreamId, false)));
    }

    private CompletableFuture<StreamIdAllocation> createKeyedStreamIdMapping(
            String streamKey, String keyPath, long streamId, int retryAttempt) {
        Set<GetOption> getOptions = Set.of(GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        return oxiaClient.put(keyPath, Long.toString(streamId).getBytes(StandardCharsets.UTF_8),
                Set.of(PutOption.IfRecordDoesNotExist,
                    PutOption.PartitionKey(STREAM_ID_GENERATOR_PATH)))
            .handle((result, failure) -> new MappingWriteResult(
                failure == null ? null : FutureUtils.unwrapCompletionException(failure)))
            .thenCompose(write -> {
                if (write.failure() == null) {
                    return completeKeyedAllocation(
                        streamId, streamKey, keyPath, true);
                }
                return oxiaClient.get(keyPath, getOptions)
                    .handle((current, readFailure) -> new MappingReadResult(
                        current, readFailure == null ? null
                            : FutureUtils.unwrapCompletionException(readFailure)))
                    .thenCompose(read -> {
                        if (read.failure() != null) {
                            write.failure().addSuppressed(read.failure());
                            return CompletableFuture.failedFuture(write.failure());
                        }
                        if (read.result() != null) {
                            long mappedStreamId = Long.parseLong(new String(
                                read.result().value(), StandardCharsets.UTF_8));
                            if (mappedStreamId == streamId) {
                                return completeKeyedAllocation(
                                    streamId, streamKey, keyPath, true);
                            }
                        }
                        if (read.result() == null
                                && !(write.failure() instanceof KeyAlreadyExistsException)) {
                            return CompletableFuture.failedFuture(write.failure());
                        }
                        if (retryAttempt >= MAX_KEYED_ALLOCATION_RETRIES) {
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                "Keyed stream-ID allocation exhausted retries for " + keyPath,
                                write.failure()));
                        }
                        long delayMillis = KEYED_ALLOCATION_RETRY_DELAY_MS << retryAttempt;
                        log.warn("Retrying keyed stream-ID allocation for {} after a mapping "
                                + "conflict (attempt {}/{})", keyPath, retryAttempt + 1,
                            MAX_KEYED_ALLOCATION_RETRIES);
                        return CompletableFuture.runAsync(
                                () -> { }, CompletableFuture.delayedExecutor(
                                    delayMillis, TimeUnit.MILLISECONDS))
                            .thenCompose(ignored -> internalAllocateStreamId(
                                Optional.of(streamKey), retryAttempt + 1));
                    });
            });
    }

    private CompletableFuture<Long> ensureStreamRegistered(
            long streamId, String key) {
        return registerStream(streamId, key)
            .handle((registered, failure) -> new RegistrationWriteResult(
                registered, failure == null ? null
                    : FutureUtils.unwrapCompletionException(failure)))
            .thenCompose(write -> {
                if (write.failure() == null) {
                    return CompletableFuture.completedFuture(write.registration());
                }
                String path = STREAM_REGISTER_PATH + "/" + streamId;
                return oxiaClient.get(path)
                    .handle((current, readFailure) -> new MappingReadResult(
                        current, readFailure == null ? null
                            : FutureUtils.unwrapCompletionException(readFailure)))
                    .thenCompose(read -> {
                        if (read.failure() != null) {
                            write.failure().addSuppressed(read.failure());
                            return CompletableFuture.failedFuture(write.failure());
                        }
                        if (read.result() != null) {
                            try {
                                StreamProperties current = deserializeStreamProperties(
                                    read.result().value());
                                if (Objects.equals(current.key(), key)) {
                                    return CompletableFuture.completedFuture(streamId);
                                }
                            } catch (IOException e) {
                                write.failure().addSuppressed(e);
                            }
                        }
                        return CompletableFuture.failedFuture(write.failure());
                    });
            });
    }

    private CompletableFuture<StreamIdAllocation> completeKeyedAllocation(
            long streamId, String streamKey, String keyPath,
            boolean createdKeyedMapping) {
        return ensureStreamRegistered(streamId, streamKey)
            .thenCompose(registeredStreamId -> validateKeyedAllocation(
                registeredStreamId, keyPath, createdKeyedMapping));
    }

    private CompletableFuture<StreamIdAllocation> validateKeyedAllocation(
            long streamId, String keyPath,
            boolean createdKeyedMapping) {
        Set<GetOption> getOptions = Set.of(
            GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        return oxiaClient.get(keyPath, getOptions)
            .handle((current, failure) -> new MappingReadResult(
                current, failure == null ? null
                    : FutureUtils.unwrapCompletionException(failure)))
            .thenCompose(read -> {
                Throwable invalidation = read.failure();
                if (invalidation == null && read.result() != null) {
                    try {
                        long mappedStreamId = Long.parseLong(new String(
                            read.result().value(), StandardCharsets.UTF_8));
                        if (mappedStreamId == streamId) {
                            return CompletableFuture.completedFuture(
                                new StreamIdAllocation(
                                    streamId, createdKeyedMapping));
                        }
                    } catch (RuntimeException e) {
                        invalidation = e;
                    }
                }
                if (invalidation == null) {
                    invalidation = new IllegalStateException(
                        "Keyed stream-ID mapping changed while registering " + keyPath);
                }
                // Registration is shared by every allocator that observes the same keyed mapping.
                // Even when this call created it, a concurrent allocator may already have reused
                // and published the registration. Only the lifecycle-aware catalog can prove that
                // the stream ID is no longer referenced, so this layer must preserve it.
                return CompletableFuture.failedFuture(
                    new KeyedAllocationInvalidatedException(
                        new StreamIdAllocation(streamId, createdKeyedMapping), invalidation));
            });
    }

    private record MappingWriteResult(Throwable failure) {
    }

    private record MappingReadResult(GetResult result, Throwable failure) {
    }

    private record RegistrationWriteResult(
            Long registration, Throwable failure) {
    }

    private CompletableFuture<Long> generateId() {
        return oxiaClient.put(STREAM_ID_GENERATOR_PATH, STREAM_ID_GENERATOR_VALUE)
            .thenApply(result -> result.version().versionId());
    }

    @Override
    public CompletableFuture<EntryIndex> getFirstEntry(long streamId) {
        return getFirstEntry(streamId, false);
    }

    @Override
    public CompletableFuture<EntryIndex> getFirstEntry(long streamId, boolean includeTrimmed) {
        long start = System.nanoTime();
        final CompletableFuture<GetResult> future;
        if (includeTrimmed) {
            future = oxiaClient.get(getSmallestStreamIdKey(streamId),
                    Set.of(GetOption.PartitionKey(String.valueOf(streamId)), GetOption.ComparisonCeiling));
        } else {
            future = getMarkDeletedOffsetWithVersion(streamId)
                    .thenCompose(markDeletedOffsetWithVersion -> {
                        final String key;
                        if (markDeletedOffsetWithVersion.getLeft() == -1L) {
                            key = getSmallestStreamIdKey(streamId);
                        } else {
                            key = Key.largestKey(streamId, markDeletedOffsetWithVersion.getLeft()).toString();
                        }
                        return oxiaClient.get(key,
                                Set.of(GetOption.PartitionKey(String.valueOf(streamId)), GetOption.ComparisonHigher));
                    });
        }
        return future.thenApply(result -> {
                    long now = System.nanoTime();
                    if (result == null || !result.key().startsWith(getStreamIdKey(streamId))) {
                        readEntryMetadataLatencyHistogram.recordFailure(now - start);
                        return EntryIndex.NOT_FOUND;
                    }
                    readEntryMetadataLatencyHistogram.recordSuccess(now - start);
                    return storageFormat.getEntryIndex(result);
                });
    }


    @Override
    public CompletableFuture<EntryIndex> getLastEntry(long streamId) {
        long start = System.nanoTime();
        return oxiaClient.get(getLargestStreamIdKey(streamId),
                Set.of(GetOption.PartitionKey(String.valueOf(streamId)), GetOption.ComparisonFloor))
            .thenApply(result -> {
                long now = System.nanoTime();
                if (result == null || !result.key().startsWith(getStreamIdKey(streamId))) {
                    readEntryMetadataLatencyHistogram.recordFailure(now - start);
                    return EntryIndex.NOT_FOUND;
                }

                readEntryMetadataLatencyHistogram.recordSuccess(now - start);
                return storageFormat.getEntryIndex(result);
            });
    }


    record PutKey(long streamId, long offset) {
    }

    private final ConcurrentHashMap<PutKey, CompletableFuture<Void>> oxiaRequests =
            new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<EntryHeader> readEntryHeader(long streamId, long offset) {
        return readEntryIndex(streamId, offset)
                .thenApply(EntryIndex::header);
    }

    @Override
    public CompletableFuture<EntryIndex> readEntryIndex(long streamId, long offset) {
        long start = System.nanoTime();
        return findFirstValidOffset(streamId, offset)
                .thenCompose(validOffset -> oxiaClient.get(Key.largestKey(streamId, validOffset).toString(),
                        Set.of(GetOption.PartitionKey(String.valueOf(streamId)), GetOption.ComparisonHigher)))
                .thenApply(result -> {
                    long now = System.nanoTime();
                    if (result == null || !result.key().startsWith(getStreamIdKey(streamId))) {
                        readEntryMetadataLatencyHistogram.recordFailure(now - start);
                        return EntryIndex.NOT_FOUND;
                    }

                    readEntryMetadataLatencyHistogram.recordSuccess(now - start);
                    return storageFormat.getEntryIndex(result);
                });
    }


    @Override
    public CompletableFuture<List<EntryIndex>> readIndexes(long streamId, long startOffset, long endOffset) {
        return readIndexes(streamId, startOffset, endOffset, false);
    }

    public CompletableFuture<List<EntryIndex>> readIndexes(long streamId, long startOffset,
                                                           long endOffset, boolean includeTrimmed) {

        List<EntryIndex> list = new ArrayList<>();
        CompletableFuture<List<EntryIndex>> promise = new CompletableFuture<>();
        //todo: use oxiaClient.list and return cached entry index, if already cached
        final CompletableFuture<Long> firstValidOffsetFuture =
                includeTrimmed ? CompletableFuture.completedFuture(startOffset) :
                        findFirstValidOffset(streamId, startOffset);
        firstValidOffsetFuture
                .thenAccept(firstValidOffset -> {
                    String startKey = Key.largestKey(streamId, firstValidOffset).toString();
                    String endKey = Key.largestKey(streamId, endOffset).toString();
                    oxiaClient.rangeScan(
                            startKey,
                            endKey,
                            new RangeScanConsumer() {
                                @Override
                                public boolean onNext(GetResult result) {
                                    var index = storageFormat.getEntryIndex(result);
                                    list.add(index);
                                    return true;
                                }

                                @Override
                                public void onError(Throwable e) {
                                    promise.completeExceptionally(e);
                                }

                                @Override
                                public void onCompleted() {
                                    promise.complete(list);
                                }
                            },
                            Set.of(RangeScanOption.PartitionKey(String.valueOf(streamId)))
                    );
                });
        return promise;
    }

    private void recordAddSuccess(long start, int numberOfMessages, int entrySize) {
        addEntryLatencyHistogram.recordSuccess(System.nanoTime() - start);
        addMessagesCounter.add(numberOfMessages);
        addMessagesSizeCounter.add(entrySize);
    }

    @VisibleForTesting
    @Deprecated
    public CompletableFuture<PutResult> writeNonCompactedIndex(long streamId, int numberOfMessages, int entrySize,
                                                 Position position) {
        return LogStateUtil.toException(streamStateManager.getState(streamId), streamId)
                .<CompletableFuture<PutResult>>map(CompletableFuture::failedFuture)
                .orElseGet(() -> oxiaClient.put(getStreamIdKey(streamId),
                        new Value(numberOfMessages, entrySize, 1, IndexType.NORMAL, position)
                                .toBytes(storageFormat.getNonCompactedIndexFormatVersion()),
                        Set.of(PutOption.PartitionKey(String.valueOf(streamId)),
                                PutOption.SequenceKeysDeltas(List.of((long) numberOfMessages, (long) entrySize)))));
    }

    @Deprecated
    public CompletableFuture<PutResult> writeNonCompactedIndex(long streamId, int numberOfMessages, int entrySize,
                                                               long initialOffset, long cumulativeSize,
                                                               Position position) {
        return LogStateUtil.toException(streamStateManager.getState(streamId), streamId)
                .<CompletableFuture<PutResult>>map(CompletableFuture::failedFuture)
                .orElseGet(() -> oxiaClient.put(
                        new Key(streamId, initialOffset + numberOfMessages, cumulativeSize).toString(),
                        new Value(numberOfMessages, entrySize, 1, IndexType.NORMAL, position)
                                .toBytes(storageFormat.getNonCompactedIndexFormatVersion()),
                        Set.of(PutOption.PartitionKey(String.valueOf(streamId)),
                                PutOption.IfRecordDoesNotExist)));
    }
    @Override
    public CompletableFuture<AddResult> append(long streamId, int numberOfMessages, ByteBuf data) {
        return write(streamId, numberOfMessages, -1, -1, data);
    }

    @Override
    public CompletableFuture<AddResult> write(long streamId, int numberOfMessages, long initialOffset,
                                              long cumulativeSize, ByteBuf data) {
        final var optException = LogStateUtil.toException(streamStateManager.getState(streamId), streamId);
        if (optException.isPresent()) {
            return CompletableFuture.failedFuture(optException.get());
        }
        long start = System.nanoTime();
        return storage.put(streamId, numberOfMessages, initialOffset, cumulativeSize, data)
                .thenCompose(putResult -> {
                            if (putResult.header() != null) {
                                recordAddSuccess(start, numberOfMessages, putResult.header().entrySize());
                                return CompletableFuture.completedFuture(putResult);
                            } else {
                                int entrySize = data.readableBytes();
                                return (initialOffset != -1 && cumulativeSize != -1
                                        ? writeNonCompactedIndex(streamId, numberOfMessages, entrySize,
                                        initialOffset, cumulativeSize, putResult.position())
                                        : writeNonCompactedIndex(streamId, numberOfMessages, entrySize,
                                        putResult.position()))
                                        .thenApply(indexResult -> {
                                            long now = System.nanoTime();
                                            if (indexResult == null) {
                                                addEntryLatencyHistogram.recordFailure(now - start);
                                                putResult.header(EntryHeader.NOT_FOUND);
                                            } else {
                                                Key k = Key.parse(indexResult.key());
                                                long newOffset = k.offset() - numberOfMessages;
                                                recordAddSuccess(start, numberOfMessages, entrySize);
                                                putResult.header(new EntryHeader(newOffset, numberOfMessages,
                                                        indexResult.version().createdTimestamp(),
                                                        entrySize, k.cumulativeSize()));
                                            }
                                            return putResult;
                                        });
                            }
                        }
                );
    }

    private CompletableFuture<Long> findFirstValidOffset(long streamId, long offset) {
        return getMarkDeletedOffsetWithVersion(streamId)
                .thenApply(markDeletedOffsetWithVersion -> {
                    if (markDeletedOffsetWithVersion.getLeft() == -1L) {
                        return offset;
                    }
                    return Math.max(offset, markDeletedOffsetWithVersion.getLeft());
                });
    }

    @Override
    public CompletableFuture<Entry> read(long streamId, long offset) {
        long start = System.nanoTime();
        return findFirstValidOffset(streamId, offset)
                .thenCompose(validOffset -> oxiaClient.get(
                                Key.largestKey(streamId, validOffset).toString(),
                                Set.of(GetOption.PartitionKey(String.valueOf(streamId)), GetOption.ComparisonHigher))
                        .thenCompose(result -> {
                            if (result == null) {
                                throw new CompletionException(new IOException("No such entry"));
                            }
                            var index = storageFormat.getEntryIndex(result);
                            return storageFormat.isProtobufFormat()
                                    ? storage.get(streamId, validOffset, index) : storage.get(streamId, index)
                                    .thenApply(entry -> {
                                        readEntryLatencyHistogram.recordSuccess(System.nanoTime() - start);
                                        readEntryCounter.increment();
                                        readEntrySizeCounter.add(entry.payload().readableBytes());
                                        return entry;
                                    });
                        })
                )
                .exceptionally(e -> {
                    readEntryLatencyHistogram.recordFailure(System.nanoTime() - start);
                    return null;
                });
    }

    @Override
    public CompletableFuture<Entry> read(long streamId, long offset, EntryIndex index) {
        return read(streamId, offset, index, false);
    }

    @Override
    public CompletableFuture<Entry> read(long streamId, long offset, EntryIndex index, boolean includeTrimmed) {
        long start = System.nanoTime();
        final CompletableFuture<Long> firstValidOffsetFuture =
                includeTrimmed ? CompletableFuture.completedFuture(offset) :
                        findFirstValidOffset(streamId, offset);
        return firstValidOffsetFuture
                .thenCompose(validOffset -> storageFormat.isProtobufFormat()
                        ? storage.get(streamId, validOffset, index) : storage.get(streamId, index))
                .thenApply(entry -> {
                    readEntryLatencyHistogram.recordSuccess(System.nanoTime() - start);
                    readEntryCounter.increment();
                    readEntrySizeCounter.add(entry.payload().readableBytes());
                    return entry;
                });
    }

    private CompletableFuture<GetResult> getKey(long streamId, long offset, GetOption option) {
        return getKey(streamId, Key.largestKey(streamId, offset).toString(), option);
    }

    private CompletableFuture<GetResult> getKey(long streamId, String key, GetOption option) {
        return oxiaClient.get(key,
                        Set.of(GetOption.PartitionKey(String.valueOf(streamId)), option))
                .thenApply(result -> {
                    if (result == null || !result.key().startsWith(getStreamIdKey(streamId))) {
                        return null;
                    }
                    return result;
                });
    }


    @Override
    public CompletableFuture<List<Entry>> readEntries(long streamId, long startOffset, int maxMessageCount,
                                                      int maxSize) {
        CompletableFuture<List<Entry>> finalResult = new CompletableFuture<>();

        if (maxSize <= 0) {
            maxSize = Integer.MAX_VALUE;
        }

        int finalMaxSize = maxSize;
        long start = System.nanoTime();
        var firstValidOffsetFuture = findFirstValidOffset(streamId, startOffset);
        var startKeyFuture = firstValidOffsetFuture.thenCompose(
                firstValidOffset -> getKey(streamId, firstValidOffset, GetOption.ComparisonHigher));
        var endKeyFuture = firstValidOffsetFuture.thenCompose(
                firstValidOffset -> getKey(streamId, firstValidOffset + maxMessageCount, GetOption.ComparisonHigher)
                        .thenCompose(result -> {
                            if (result == null) {
                                return getKey(streamId, firstValidOffset + maxMessageCount, GetOption.ComparisonFloor);
                            }

                            return CompletableFuture.completedFuture(result);
                        }));

        CompletableFuture<List<EntryIndex>> readEntriesContexts = new CompletableFuture<>();
        startKeyFuture.thenCombine(endKeyFuture, (startKey, endKeyInclusive) -> {
            var fistValidOffset = firstValidOffsetFuture.join();
            if (startKey == null) {
                readEntriesContexts.completeExceptionally(
                        new NoSuchOffsetException(streamId, fistValidOffset));
            } else if (endKeyInclusive == null || startKey.key().equals(endKeyInclusive.key())) {
                EntryIndex entryIndex = storageFormat.getEntryIndex(startKey);
                if (!entryIndex.position().isBinary()) {
                    readEntriesContexts.completeExceptionally(new WalFileAlreadyCompactedException());
                } else {
                    readEntriesContexts.complete(List.of(storageFormat.getEntryIndex(startKey)));
                }
            } else {
                String endKeyExclusive =
                        Key.largestKey(streamId, Key.parse(endKeyInclusive.key()).offset() + 1).toString();

                oxiaClient.rangeScan(
                        startKey.key(),
                        // TODO: maxEntries is entry count, not sizeInBytes,
                        //  so we need to scan more entries to get the maxEntries entries.
                        endKeyExclusive,
                        new RangeScanConsumer() {

                            final List<EntryIndex> indices =
                                    new ArrayList<>(defaultReadBatchContextInitializeSize);
                            long readMessageCount = 0;
                            long readEntrySize = 0;
                            boolean skipRead = false;

                            @Override
                            public boolean onNext(GetResult getResult) {
                                var index = storageFormat.getEntryIndex(getResult);
                                EntryHeader eh = index.header();
                                Position pos = index.position();
                                if (skipRead || !pos.isBinary()) {
                                    // only read WAL type files
                                    skipRead = true;
                                    return false;
                                }

                                // check if the maxEntries or maxSize is reached.
                                if ((readMessageCount + index.header().numberOfMessages()) > maxMessageCount
                                        || (readEntrySize + eh.entrySize()) > finalMaxSize) {
                                    if (indices.isEmpty()
                                            // Add this index if the index is compacted
                                            // because we don't know where the last key's sizeInBytes is located in the
                                            // compacted index.
                                            // It is fine to scan slightly more indices
                                            // as EntryList.canAdd will double-check its maxEntries and maxSize.
                                            || (readMessageCount < maxMessageCount && readEntrySize < finalMaxSize)) {
                                        indices.add(index);
                                        readMessageCount += index.header().numberOfMessages();
                                        readEntrySize += eh.entrySize();
                                    }
                                    if (!readEntriesContexts.isDone()) {
                                        readEntriesContexts.complete(indices);
                                    }
                                    return false;
                                }
                                indices.add(index);
                                // Ignore readEntryCount and readEntrySize update if the first index is compacted
                                // because we don't know where the first key's sizeInBytes is located
                                // in the compacted index. It is fine to scan slightly more indices
                                // as EntryList.canAdd will double-check its maxEntries and maxSize.
                                if (!startKey.key().equals(getResult.key())) {
                                    readMessageCount += index.header().numberOfMessages();
                                    readEntrySize += eh.entrySize();
                                }
                                return true;
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                readEntriesContexts.completeExceptionally(throwable);
                            }

                            @Override
                            public void onCompleted() {
                                if (!readEntriesContexts.isDone()) {
                                    readEntriesContexts.complete(indices);
                                }
                            }
                        },
                        Set.of(RangeScanOption.PartitionKey(String.valueOf(streamId))));
            }

            return readEntriesContexts.whenComplete((indices, throwable) -> {
                if (throwable != null) {
                    readEntriesIndexDuration.recordFailure(System.nanoTime() - start);
                    finalResult.completeExceptionally(throwable);
                    return;
                }

                readEntriesIndexDuration.recordSuccess(System.nanoTime() - start);

                long readDataStart = System.nanoTime();
                EntryList entryList = new EntryList(streamId, fistValidOffset, maxMessageCount, finalMaxSize);
                storage.get(indices, entryList)
                        .whenComplete((__, e) -> {
                            long now = System.nanoTime();
                            if (e != null) {
                                readEntryLatencyHistogram.recordFailure(now - start);
                                readEntriesDataDuration.recordFailure(now - readDataStart);
                                clearEntryListAfterFailure(entryList, e);
                                finalResult.completeExceptionally(e);
                                return;
                            }

                            readEntryLatencyHistogram.recordSuccess(now - start);
                            readEntryCounter.add(entryList.size());
                            readEntrySizeCounter.add((int) entryList.getSizeInBytes());
                            readEntriesDataDuration.recordSuccess(now - readDataStart);
                            finalResult.complete(entryList.getEntries());
                        });
            });
        }).exceptionally(e -> {
            readEntryLatencyHistogram.recordFailure(System.nanoTime() - start);
            finalResult.completeExceptionally(e);
            return null;
        });
        return finalResult;
    }


    @Override
    public CompletableFuture<List<Entry>> readEntries(List<EntryIndex> entryIndices, EntryList entryList) {
        CompletableFuture<List<Entry>> finalResult = new CompletableFuture<>();
        long start = System.nanoTime();

        storage.get(entryIndices, entryList)
            .whenComplete((__, e) -> {
                //log.info("entryIndices:{}", entryIndices);
                long now = System.nanoTime();
                if (e != null) {
                    readEntryLatencyHistogram.recordFailure(now - start);
                    readEntriesDataDuration.recordFailure(now - start);
                    clearEntryListAfterFailure(entryList, e);
                    finalResult.completeExceptionally(e);
                    return;
                }
                readEntryLatencyHistogram.recordSuccess(now - start);
                readEntryCounter.add(entryList.size());
                readEntrySizeCounter.add((int) entryList.getSizeInBytes());
                readEntriesDataDuration.recordSuccess(now - start);
                finalResult.complete(entryList.getEntries());
            });
        return finalResult;
    }

    private static void clearEntryListAfterFailure(EntryList entryList, Throwable failure) {
        try {
            entryList.clear();
        } catch (RuntimeException | Error cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    @Override
    public void preFetchEntries(long streamId, List<Position> positions) {
        if (positions == null || positions.isEmpty()) {
            return;
        }
        // Filter out any non-RAW positions since we only want to prefetch from wal
        List<Position> rawPositions = positions.stream()
            .filter(pos -> pos.fileType() == Position.FileType.RAW)
            .collect(Collectors.toList());
        if (!rawPositions.isEmpty()) {
            storage.preFetch(streamId, rawPositions);
        }
    }

    private CompletableFuture<Long> tryUpdateMarkDeletedOffset(long streamId, long markDeletedOffset) {
        if (log.isDebugEnabled()) {
            log.debug("Updating mark deleted offset for stream {} to {}", streamId, markDeletedOffset);
        }
        return getMarkDeletedOffsetWithVersion(streamId)
                .thenCompose(getResult -> {
                    final PutOption putOption;
                    if (getResult.getLeft() == -1L) {
                        putOption = PutOption.IfRecordDoesNotExist;
                    } else {
                        if (getResult.getLeft() >= markDeletedOffset) {
                            return CompletableFuture.completedFuture(getResult.getLeft());
                        }
                        putOption = PutOption.IfVersionIdEquals(getResult.getRight());
                    }
                    return oxiaClient.put(StorageFormat.MARK_DELETED_OFFSET_PATH + "/" + streamId,
                                    Long.toString(markDeletedOffset).getBytes(StandardCharsets.UTF_8),
                                    Set.of(PutOption.PartitionKey(String.valueOf(streamId)), putOption))
                            .thenApply(__ -> markDeletedOffset);
                });
    }

    private CompletableFuture<Long> updateMarkDeletedOffset(long streamId, long markDeletedOffset) {
        if (markDeletedOffset <= -1) {
            return CompletableFuture.completedFuture(-1L);
        }
        // We can safely allow this operation to fail after several conflicts attempts. The stream trimming executor
        // will retry it later.
        return updateMarkDeletedOffsetWithRetry(streamId, markDeletedOffset, 3);
    }

    private CompletableFuture<Long> updateMarkDeletedOffsetWithRetry(long streamId, long markDeletedOffset,
                                                                     int retriesLeft) {
        return tryUpdateMarkDeletedOffset(streamId, markDeletedOffset)
                .exceptionallyCompose(e -> {
                    Throwable realCause = FutureUtils.unwrapCompletionException(e);
                    if ((realCause instanceof KeyAlreadyExistsException
                            || realCause instanceof UnexpectedVersionIdException) && retriesLeft > 0) {
                        return updateMarkDeletedOffsetWithRetry(streamId, markDeletedOffset, retriesLeft - 1);
                    }
                    return CompletableFuture.failedFuture(e);
                });
    }

    @Override
    public CompletableFuture<Long> softTrimStream(long streamId, long offsetIncluded) {
        return getLastEntry(streamId).thenApply(lastEntry -> {
            // Avoid trimming all offsets. Otherwise, the next offset will start from 0 again.
            // offsetIncluded =2
            // lastEntry= 1->3(3 entries)
            return Math.min(offsetIncluded + 1, lastEntry.header().offset());
        }).thenCompose(exclusiveTrimOffset -> updateMarkDeletedOffset(streamId, exclusiveTrimOffset)
                .thenApply(markDeleteOffset -> {
                    log.info("Updated mark deleted offset for stream {} to {}", streamId, markDeleteOffset);
                    return markDeleteOffset;
                })
                .exceptionally(e -> {
                    log.warn("failed to set mark deleted offset", e);
                    return exclusiveTrimOffset;
                }));
    }


    @VisibleForTesting
    CompletableFuture<Pair<Long, Long>> getMarkDeletedOffsetWithVersion(long streamId) {
        return oxiaClient.get(StorageFormat.MARK_DELETED_OFFSET_PATH + "/" + streamId,
                        Set.of(GetOption.PartitionKey(String.valueOf(streamId))))
                .thenApply(result -> {
                    if (result == null) {
                        return Pair.of(-1L, -1L);
                    }
                    var deleteOffset = Long.parseLong(new String(result.value(), StandardCharsets.UTF_8));
                    return Pair.of(deleteOffset, result.version().versionId());
                });
    }

    @Override
    public CompletableFuture<Void> deleteStream(long streamId, Optional<String> key) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (key.isPresent()) {
            future = oxiaClient.delete(STREAM_ID_GENERATOR_PATH + "/" + key.get(),
                    Set.of(DeleteOption.PartitionKey(STREAM_ID_GENERATOR_PATH))).thenApply(x -> null);
        } else {
            future.complete(null);
        }
        return future.thenCompose(__ -> oxiaClient.deleteRange(
                getSmallestStreamIdKey(streamId),
                getLargestStreamIdKey(streamId),
                Set.of(DeleteRangeOption.PartitionKey(String.valueOf(streamId)))))
            .thenCompose(__ -> removeStream(streamId))
            .thenRun(() -> {
                storageFormat.removeCachedKey(streamId);
            });
    }

    @Override
    public CompletableFuture<Void> compactEntryIndex(long streamId, long startOffset, long endOffset,
                                                     long endCumulativeSize, Value value) {
        Key toUpdateKey = new Key(streamId, endOffset, endCumulativeSize);
        log.info("Compact stream {} entry index {}-{}, endCumulativeSize: {} new value: {}", streamId,
                startOffset, endOffset, endCumulativeSize, value);
        return oxiaClient.get(toUpdateKey.toString(), Set.of(GetOption.PartitionKey(String.valueOf(streamId))))
                .thenCompose(getResult -> {
                    if (getResult == null) {
                        log.info("The stream {} endOffset {} already be removed, delete [{}-{}) directly.",
                            streamId, endOffset, startOffset, endOffset);
                        return oxiaClient.deleteRange(Key.smallestKey(streamId, startOffset).toString(),
                                        Key.largestKey(streamId, endOffset - 1).toString(),
                                        Set.of(DeleteRangeOption.PartitionKey(String.valueOf(streamId))))
                                .whenComplete((res, ex) -> {
                                    if (ex != null) {
                                        log.warn("The stream {} delete [{}-{}) failed", streamId, startOffset,
                                                endOffset, ex);
                                    } else {
                                        log.info("The stream {} delete [{}-{}) succeed", streamId, startOffset,
                                                endOffset);
                                    }
                                });
                    } else {
                        return oxiaClient.put(toUpdateKey.toString(), value.toBytes(indexSerializeFormatVersion()),
                                Set.of(PutOption.PartitionKey(String.valueOf(streamId))))
                            .thenCompose((v) -> {
                                log.info("The stream {} redirect {} to {} succeed.", streamId, toUpdateKey, value);
                                return oxiaClient.deleteRange(Key.smallestKey(streamId, startOffset).toString(),
                                        Key.largestKey(streamId, endOffset - 1).toString(),
                                        Set.of(DeleteRangeOption.PartitionKey(String.valueOf(streamId))))
                                        .whenComplete((res, ex) -> {
                                            if (ex != null) {
                                                log.warn("The stream {} delete [{}-{}) failed", streamId, startOffset,
                                                        endOffset, ex);
                                            } else {
                                                log.info("The stream {} delete [{}-{}) succeed", streamId, startOffset,
                                                        endOffset);
                                            }
                                        });
                            });
                    }
        }).thenCompose(x -> putUncompactedOffset(streamId, endOffset + 1));
    }

    @Override
    public CompletableFuture<Position> getFirstUnCompactedPosition(long streamId) {
        return getUncompactedOffset(streamId)
            .thenCompose(offset -> readEntryIndex(streamId, offset))
            .thenApply(EntryIndex::position);
    }

    @Override
    public CompletableFuture<Map<String, Long>> getMarkDeletedOffsetMap() {
        CompletableFuture<Map<String, Long>> future = new CompletableFuture<>();
        final var markDeletedOffsetMap = new HashedMap<String, Long>();
        final var startKey = StorageFormat.MARK_DELETED_OFFSET_PATH + "/";
        oxiaClient.rangeScan(
                startKey,
                startKey + "/",
                new RangeScanConsumer() {
                    @Override
                    public boolean onNext(GetResult result) {
                        var streamId =
                                result.key().substring((StorageFormat.MARK_DELETED_OFFSET_PATH + "/").length());
                        var markDeletedOffset =
                                Long.parseLong(new String(result.value(), StandardCharsets.UTF_8));
                        markDeletedOffsetMap.put(streamId, markDeletedOffset);
                        return true;
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        future.complete(markDeletedOffsetMap);
                    }
                });
        return future;
    }

    @Override
    public AsyncOxiaClient getStorageOxiaClient() {
        return oxiaClient;
    }

    @VisibleForTesting
    public StorageFormat getStorageFormat() {
        return storageFormat;
    }

    @Override
    public void startWALCleanupService() throws Exception {
        asyncCleaner.startCleanupTask();
    }

    @Override
    public CompletableFuture<Set<Long>> listStreams() {
        var pathWithSlash = STREAM_REGISTER_PATH + "/";
        return oxiaClient.list(pathWithSlash, pathWithSlash + "/")
            .thenApply(streamIds -> streamIds.stream()
                .map(s -> Long.parseLong(s.substring(pathWithSlash.length()))).collect(Collectors.toSet()));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public LogStateManager getStreamStateManager() {
        return streamStateManager;
    }

    @Override
    public CompletableFuture<Void> hardTrimStream(long streamId, long offsetExcluded) {
        return oxiaClient.deleteRange(Key.smallestKey(streamId).toString(),
                        Key.largestKey(streamId, offsetExcluded).toString(),
                        Set.of(DeleteRangeOption.PartitionKey(String.valueOf(streamId))));
    }
}
