/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageFormat.FIRST_UNCOMPACTED_OFFSET;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_ID_GENERATOR_PATH;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_ID_GENERATOR_VALUE;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_REGISTER_PATH;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_WRITE_FENCE_PATH;
import static io.lakestream.ursa.storage.impl.StorageFormat.STREAM_WRITE_LEASE_PATH;

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
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.Key;
import io.lakestream.ursa.storage.OwnedResultFutures;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLease;
import io.lakestream.ursa.storage.StorageApi.StreamWriteLeaseDrainTimeoutException;
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
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
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
    private static final Duration DEFAULT_STREAM_WRITE_LEASE_DRAIN_TIMEOUT = Duration.ofSeconds(30);
    private static final long STREAM_WRITE_LEASE_DRAIN_POLL_INTERVAL_MS = 25L;
    private static final long STREAM_WRITE_LEASE_RELEASE_INITIAL_RETRY_DELAY_MS = 10L;
    private static final long STREAM_WRITE_LEASE_RELEASE_MAX_RETRY_DELAY_MS = 1_000L;
    private static final byte[] STREAM_WRITE_LIFECYCLE_MARKER = new byte[] {1};

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
    private final ConcurrentHashMap<Long, LocalWriteGate> localWriteGates =
            new ConcurrentHashMap<>();

    /**
     * Process-local side of the durable lease protocol.
     *
     * <p>The Oxia lease protects a process from a remote purge. This gate additionally makes lease
     * close atomic with accepting and draining mutations in this process, so the durable lease is
     * never removed while a mutation it protects can still publish storage state.
     */
    private static final class LocalWriteGate {

        private int openLeases;
        private int inFlightMutations;
        private CompletableFuture<Void> lastLeaseDrain;

        synchronized void openLease() {
            openLeases++;
            if (lastLeaseDrain != null) {
                CompletableFuture<Void> drain = lastLeaseDrain;
                lastLeaseDrain = null;
                drain.complete(null);
            }
        }

        synchronized CompletableFuture<Void> closeLease() {
            if (openLeases <= 0) {
                throw new IllegalStateException("Local write lease is already closed");
            }
            openLeases--;
            if (openLeases > 0 || inFlightMutations == 0) {
                return CompletableFuture.completedFuture(null);
            }
            if (lastLeaseDrain == null) {
                lastLeaseDrain = new CompletableFuture<>();
            }
            return lastLeaseDrain;
        }

        synchronized boolean enterMutation() {
            if (openLeases <= 0) {
                return false;
            }
            inFlightMutations++;
            return true;
        }

        synchronized void exitMutation() {
            if (inFlightMutations <= 0) {
                throw new IllegalStateException("No local write mutation is active");
            }
            inFlightMutations--;
            if (inFlightMutations == 0 && openLeases == 0 && lastLeaseDrain != null) {
                CompletableFuture<Void> drain = lastLeaseDrain;
                lastLeaseDrain = null;
                drain.complete(null);
            }
        }

        synchronized boolean isIdle() {
            return openLeases == 0 && inFlightMutations == 0 && lastLeaseDrain == null;
        }
    }

    private static final class LocalWritePermit {

        private final LocalWriteGate gate;

        private LocalWritePermit(LocalWriteGate gate) {
            this.gate = gate;
        }

        void close() {
            gate.exitMutation();
        }
    }

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
    private static final ObjectReader STREAM_ID_MAPPING_READER =
            OBJECT_MAPPER.readerFor(PersistedStreamIdMapping.class);
    private static final ObjectWriter STREAM_ID_MAPPING_WRITER =
            OBJECT_MAPPER.writerFor(PersistedStreamIdMapping.class);

    private enum StreamIdMappingState {
        ACTIVE,
        TOMBSTONE
    }

    private record PersistedStreamIdMapping(
            int version, StreamIdMappingState state, long streamId,
            String incarnationId, String ownerToken, long ownerGeneration) {

        private static final int CURRENT_VERSION = 1;

        private PersistedStreamIdMapping {
            if (version != CURRENT_VERSION) {
                throw new IllegalArgumentException(
                    "Unsupported keyed stream-ID mapping version: " + version);
            }
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(incarnationId, "incarnationId");
            Objects.requireNonNull(ownerToken, "ownerToken");
        }

        static PersistedStreamIdMapping active(
                long streamId, StreamIdMappingOwner owner) {
            return new PersistedStreamIdMapping(
                CURRENT_VERSION, StreamIdMappingState.ACTIVE, streamId,
                owner.incarnationId(), owner.ownerToken(), owner.ownerGeneration());
        }

        static PersistedStreamIdMapping tombstone(StreamIdMappingFence fence) {
            StreamIdMappingOwner owner = fence.owner();
            return new PersistedStreamIdMapping(
                CURRENT_VERSION, StreamIdMappingState.TOMBSTONE, fence.streamId(),
                owner.incarnationId(), owner.ownerToken(), owner.ownerGeneration());
        }

        static PersistedStreamIdMapping legacyActive(long streamId) {
            return active(streamId, StreamIdMappingOwner.legacy());
        }

        StreamIdMappingOwner owner() {
            return new StreamIdMappingOwner(incarnationId, ownerToken, ownerGeneration);
        }

        StreamIdMappingFence fence() {
            if (state != StreamIdMappingState.TOMBSTONE) {
                throw new IllegalStateException("Active mapping is not a fence");
            }
            return new StreamIdMappingFence(streamId, owner());
        }
    }

    @VisibleForTesting
    static StreamProperties deserializeStreamProperties(byte[] value) throws IOException {
        return STREAM_PROPERTIES_READER.readValue(value);
    }

    private static PersistedStreamIdMapping deserializeStreamIdMapping(byte[] value) {
        String legacyValue = new String(value, StandardCharsets.UTF_8);
        try {
            return PersistedStreamIdMapping.legacyActive(Long.parseLong(legacyValue));
        } catch (NumberFormatException ignored) {
            try {
                return STREAM_ID_MAPPING_READER.readValue(value);
            } catch (IOException | RuntimeException e) {
                throw new IllegalStateException("Invalid keyed stream-ID mapping value", e);
            }
        }
    }

    private static byte[] serializeStreamIdMapping(PersistedStreamIdMapping mapping) {
        try {
            return STREAM_ID_MAPPING_WRITER.writeValueAsBytes(mapping);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize keyed stream-ID mapping", e);
        }
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
        return internalAllocateStreamId(key, 0);
    }

    @Override
    public CompletableFuture<StreamIdAllocation> allocateStreamId(
            String key, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(acknowledgedFence, "acknowledgedFence");
        if (key.isBlank()) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("key must not be blank"));
        }
        if (owner.isLegacy()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "Legacy mapping owner cannot allocate lifecycle-aware stream IDs"));
        }
        return internalAllocateOwnedStreamId(
                key, owner, acknowledgedFence, 0);
    }

    @Override
    public CompletableFuture<Void> bindStreamIdMapping(
            String key, long streamId, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(acknowledgedFence, "acknowledgedFence");
        if (key.isBlank()) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("key must not be blank"));
        }
        if (streamId < 0) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("streamId must be non-negative"));
        }
        if (owner.isLegacy()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "Legacy mapping owner cannot bind lifecycle-aware stream IDs"));
        }
        return internalBindStreamIdMapping(
                key, streamId, owner, acknowledgedFence, 0)
            .thenApply(__ -> null);
    }

    @Override
    public CompletableFuture<Long> getStreamIdByKey(String key) {
        String keyPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        return oxiaClient.get(keyPath, Set.of(GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH)))
            .thenCompose(result -> {
                if (result == null) {
                    return noSuchStreamIdMapping(key);
                }
                PersistedStreamIdMapping mapping = deserializeStreamIdMapping(result.value());
                if (mapping.state() == StreamIdMappingState.TOMBSTONE) {
                    return noSuchStreamIdMapping(key);
                }
                return CompletableFuture.completedFuture(mapping.streamId());
            });
    }

    private static CompletableFuture<Long> noSuchStreamIdMapping(String key) {
        return CompletableFuture.failedFuture(
            new CompletionException(new NoSuchKeyException("No stream id found for key: " + key)));
    }

    @Override
    public CompletableFuture<Void> deleteStreamIdMapping(String key) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Unconditional keyed stream-ID deletion is disabled; use a durable mapping fence"));
    }

    @Override
    public CompletableFuture<Void> deleteStreamIdMapping(String key, long expectedStreamId) {
        return fenceStreamIdMappingState(key, expectedStreamId, Optional.empty(), 0)
            .thenApply(__ -> null);
    }

    @Override
    public boolean supportsConditionalStreamIdMappingDeletion() {
        return true;
    }

    @Override
    public boolean supportsFencedStreamIdMappings() {
        return true;
    }

    @Override
    public CompletableFuture<StreamIdMappingFenceResult> fenceStreamIdMappingState(
            String key, long expectedStreamId, StreamIdMappingOwner expectedOwner) {
        Objects.requireNonNull(expectedOwner, "expectedOwner");
        if (expectedOwner.isLegacy() && expectedStreamId == -1) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "Legacy mapping owner cannot capture an absent mapping fence"));
        }
        return fenceStreamIdMappingState(
            key, expectedStreamId, Optional.of(expectedOwner), 0);
    }

    private CompletableFuture<StreamIdMappingFenceResult> fenceStreamIdMappingState(
            String key, long expectedStreamId,
            Optional<StreamIdMappingOwner> expectedOwner, int retryAttempt) {
        String keyPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        Set<GetOption> getOptions = Set.of(GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        return oxiaClient.get(keyPath, getOptions).thenCompose(result -> {
            if (result == null) {
                StreamIdMappingFence absentFence = new StreamIdMappingFence(
                    -1, expectedOwner.orElse(StreamIdMappingOwner.legacy()));
                return writeStreamIdMappingFence(
                    key, keyPath, null, absentFence, expectedStreamId,
                    expectedOwner, retryAttempt);
            }
            PersistedStreamIdMapping current = deserializeStreamIdMapping(result.value());
            if (current.state() == StreamIdMappingState.TOMBSTONE) {
                return CompletableFuture.completedFuture(
                    new StreamIdMappingFenceResult.Fenced(current.fence()));
            }
            boolean sameExpectedOwner = expectedOwner.isEmpty()
                || current.owner().equals(expectedOwner.orElseThrow());
            boolean adoptExactLegacy = current.streamId() == expectedStreamId
                && expectedOwner.isPresent() && current.owner().isLegacy();
            boolean captureLateSameOwnerAllocation = expectedStreamId == -1
                && expectedOwner.isPresent()
                && current.owner().equals(expectedOwner.orElseThrow());
            if ((!sameExpectedOwner || current.streamId() != expectedStreamId)
                    && !adoptExactLegacy && !captureLateSameOwnerAllocation) {
                return CompletableFuture.completedFuture(
                    new StreamIdMappingFenceResult.PreservedActive(
                        new ActiveStreamIdMapping(current.streamId(), current.owner())));
            }
            StreamIdMappingFence fence = new StreamIdMappingFence(
                current.streamId(), expectedOwner.orElse(current.owner()));
            return writeStreamIdMappingFence(
                key, keyPath, result, fence, expectedStreamId,
                expectedOwner, retryAttempt);
        });
    }

    private CompletableFuture<StreamIdMappingFenceResult> writeStreamIdMappingFence(
            String key, String keyPath, GetResult current,
            StreamIdMappingFence fence, long expectedStreamId,
            Optional<StreamIdMappingOwner> expectedOwner, int retryAttempt) {
        Set<PutOption> putOptions = current == null
            ? Set.of(PutOption.IfRecordDoesNotExist,
                PutOption.PartitionKey(STREAM_ID_GENERATOR_PATH))
            : Set.of(PutOption.IfVersionIdEquals(current.version().versionId()),
                PutOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        return oxiaClient.put(keyPath,
                serializeStreamIdMapping(PersistedStreamIdMapping.tombstone(fence)), putOptions)
            .handle((write, failure) -> failure == null ? null
                : FutureUtils.unwrapCompletionException(failure))
            .thenCompose(failure -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(
                        new StreamIdMappingFenceResult.Fenced(fence));
                }
                Set<GetOption> getOptions = Set.of(
                    GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
                return oxiaClient.get(keyPath, getOptions)
                    .thenCompose(readback -> {
                        if (readback != null) {
                            PersistedStreamIdMapping observed =
                                deserializeStreamIdMapping(readback.value());
                            if (observed.state() == StreamIdMappingState.TOMBSTONE
                                    && observed.fence().equals(fence)) {
                                return CompletableFuture.completedFuture(
                                    new StreamIdMappingFenceResult.Fenced(fence));
                            }
                        }
                        if (failure instanceof UnexpectedVersionIdException
                                || failure instanceof KeyAlreadyExistsException) {
                            if (retryAttempt >= MAX_CONDITIONAL_MAPPING_DELETE_RETRIES) {
                                return CompletableFuture.failedFuture(new IllegalStateException(
                                    "Conditional keyed stream-ID fence exhausted retries for "
                                        + keyPath, failure));
                            }
                            long delayMillis = CONDITIONAL_MAPPING_DELETE_RETRY_DELAY_MS
                                << retryAttempt;
                            log.warn("Retrying conditional keyed stream-ID fence for {} after "
                                    + "a version conflict (attempt {}/{})", keyPath,
                                retryAttempt + 1, MAX_CONDITIONAL_MAPPING_DELETE_RETRIES);
                            return CompletableFuture.runAsync(
                                    () -> { }, CompletableFuture.delayedExecutor(
                                        delayMillis, TimeUnit.MILLISECONDS))
                                .thenCompose(ignored -> fenceStreamIdMappingState(
                                    key, expectedStreamId, expectedOwner,
                                    retryAttempt + 1));
                        }
                        return CompletableFuture.failedFuture(failure);
                    });
            });
    }

    @Override
    public CompletableFuture<Void> canonicalizeStreamIdMappingFence(
            String key, StreamIdMappingFence expectedFence,
            StreamIdMappingFence canonicalFence) {
        Objects.requireNonNull(expectedFence, "expectedFence");
        Objects.requireNonNull(canonicalFence, "canonicalFence");
        if (!expectedFence.owner().equals(canonicalFence.owner())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "Stream-ID fence canonicalization requires the same lifecycle owner"));
        }
        return canonicalizeStreamIdMappingFence(
            key, expectedFence, canonicalFence, 0);
    }

    private CompletableFuture<Void> canonicalizeStreamIdMappingFence(
            String key, StreamIdMappingFence expectedFence,
            StreamIdMappingFence canonicalFence, int retryAttempt) {
        String keyPath = STREAM_ID_GENERATOR_PATH + "/" + key;
        Set<GetOption> getOptions = Set.of(
            GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        return oxiaClient.get(keyPath, getOptions).thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "Cannot canonicalize absent keyed stream-ID fence " + keyPath));
            }
            PersistedStreamIdMapping current = deserializeStreamIdMapping(result.value());
            if (current.state() != StreamIdMappingState.TOMBSTONE) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "Cannot canonicalize active keyed stream-ID mapping " + keyPath));
            }
            if (current.fence().equals(canonicalFence)) {
                return CompletableFuture.completedFuture(null);
            }
            if (!current.fence().equals(expectedFence)) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "Keyed stream-ID tombstone changed while canonicalizing " + keyPath));
            }
            Set<PutOption> putOptions = Set.of(
                PutOption.IfVersionIdEquals(result.version().versionId()),
                PutOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
            return oxiaClient.put(keyPath,
                    serializeStreamIdMapping(PersistedStreamIdMapping.tombstone(canonicalFence)),
                    putOptions)
                .handle((write, failure) -> failure == null ? null
                    : FutureUtils.unwrapCompletionException(failure))
                .thenCompose(failure -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (failure instanceof UnexpectedVersionIdException
                            && retryAttempt < MAX_CONDITIONAL_MAPPING_DELETE_RETRIES) {
                        return canonicalizeStreamIdMappingFence(
                            key, expectedFence, canonicalFence, retryAttempt + 1);
                    }
                    return CompletableFuture.failedFuture(failure);
                });
        });
    }

    private CompletableFuture<StreamIdAllocation> internalAllocateOwnedStreamId(
            String streamKey, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence, int retryAttempt) {
        String keyPath = STREAM_ID_GENERATOR_PATH + "/" + streamKey;
        Set<GetOption> getOptions = Set.of(
            GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        return oxiaClient.get(keyPath, getOptions).thenCompose(result -> {
            if (result == null) {
                if (acknowledgedFence.isPresent()) {
                    return CompletableFuture.failedFuture(new IllegalStateException(
                        "Acknowledged keyed stream-ID fence is absent for " + keyPath));
                }
                return generateId().thenCompose(streamId -> installOwnedStreamIdMapping(
                    streamKey, keyPath, streamId, owner, acknowledgedFence,
                    null, retryAttempt, false));
            }
            PersistedStreamIdMapping current = deserializeStreamIdMapping(result.value());
            if (current.state() == StreamIdMappingState.ACTIVE) {
                if (current.owner().equals(owner)) {
                    return completeOwnedKeyedAllocation(
                        current.streamId(), streamKey, keyPath, owner, false);
                }
                return differentMappingOwner(streamKey, current);
            }
            if (acknowledgedFence.isEmpty()
                    || !current.fence().equals(acknowledgedFence.orElseThrow())) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "Keyed stream-ID tombstone was not acknowledged for " + keyPath));
            }
            return generateId().thenCompose(streamId -> installOwnedStreamIdMapping(
                streamKey, keyPath, streamId, owner, acknowledgedFence,
                result, retryAttempt, false));
        });
    }

    private CompletableFuture<StreamIdAllocation> internalBindStreamIdMapping(
            String streamKey, long streamId, StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence, int retryAttempt) {
        String keyPath = STREAM_ID_GENERATOR_PATH + "/" + streamKey;
        Set<GetOption> getOptions = Set.of(
            GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        return oxiaClient.get(keyPath, getOptions).thenCompose(result -> {
            if (result == null) {
                if (acknowledgedFence.isPresent()) {
                    return CompletableFuture.failedFuture(new IllegalStateException(
                        "Acknowledged keyed stream-ID fence is absent for " + keyPath));
                }
                return installOwnedStreamIdMapping(
                    streamKey, keyPath, streamId, owner, acknowledgedFence,
                    null, retryAttempt, true);
            }
            PersistedStreamIdMapping current = deserializeStreamIdMapping(result.value());
            if (current.state() == StreamIdMappingState.ACTIVE) {
                if (current.streamId() != streamId) {
                    return differentMappingOwner(streamKey, current);
                }
                if (current.owner().equals(owner)) {
                    return completeOwnedKeyedAllocation(
                        streamId, streamKey, keyPath, owner, false);
                }
                if (!current.owner().isLegacy()) {
                    return differentMappingOwner(streamKey, current);
                }
                // A broker may have created the legacy numeric mapping before the catalog had an
                // owner token. Adopt that exact ID with a versioned CAS; a different ID is never
                // overwritten.
                return installOwnedStreamIdMapping(
                    streamKey, keyPath, streamId, owner, acknowledgedFence,
                    result, retryAttempt, true);
            }
            if (acknowledgedFence.isEmpty()
                    || !current.fence().equals(acknowledgedFence.orElseThrow())) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "Keyed stream-ID tombstone was not acknowledged for " + keyPath));
            }
            return installOwnedStreamIdMapping(
                streamKey, keyPath, streamId, owner, acknowledgedFence,
                result, retryAttempt, true);
        });
    }

    private CompletableFuture<StreamIdAllocation> installOwnedStreamIdMapping(
            String streamKey, String keyPath, long streamId,
            StreamIdMappingOwner owner,
            Optional<StreamIdMappingFence> acknowledgedFence,
            GetResult expectedCurrent, int retryAttempt, boolean bindExistingId) {
        Set<PutOption> putOptions = expectedCurrent == null
            ? Set.of(PutOption.IfRecordDoesNotExist,
                PutOption.PartitionKey(STREAM_ID_GENERATOR_PATH))
            : Set.of(PutOption.IfVersionIdEquals(expectedCurrent.version().versionId()),
                PutOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
        PersistedStreamIdMapping desired =
            PersistedStreamIdMapping.active(streamId, owner);
        return oxiaClient.put(keyPath, serializeStreamIdMapping(desired), putOptions)
            .handle((write, failure) -> new MappingWriteResult(
                failure == null ? null : FutureUtils.unwrapCompletionException(failure)))
            .thenCompose(write -> {
                if (write.failure() == null) {
                    return completeOwnedKeyedAllocation(
                        streamId, streamKey, keyPath, owner, true);
                }
                Set<GetOption> getOptions = Set.of(
                    GetOption.PartitionKey(STREAM_ID_GENERATOR_PATH));
                return oxiaClient.get(keyPath, getOptions)
                    .thenCompose(readback -> {
                        if (readback != null) {
                            PersistedStreamIdMapping current =
                                deserializeStreamIdMapping(readback.value());
                            if (current.state() == StreamIdMappingState.ACTIVE
                                    && current.owner().equals(owner)) {
                                if (bindExistingId && current.streamId() != streamId) {
                                    return differentMappingOwner(streamKey, current);
                                }
                                return completeOwnedKeyedAllocation(
                                    current.streamId(), streamKey, keyPath, owner,
                                    current.streamId() == streamId);
                            }
                            if (current.state() == StreamIdMappingState.ACTIVE) {
                                return differentMappingOwner(streamKey, current);
                            }
                        }
                        if (!(write.failure() instanceof UnexpectedVersionIdException)
                                && !(write.failure() instanceof KeyAlreadyExistsException)) {
                            return CompletableFuture.failedFuture(write.failure());
                        }
                        if (retryAttempt >= MAX_KEYED_ALLOCATION_RETRIES) {
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                "Lifecycle-aware keyed stream-ID allocation exhausted retries for "
                                    + keyPath, write.failure()));
                        }
                        if (bindExistingId) {
                            return internalBindStreamIdMapping(
                                streamKey, streamId, owner,
                                acknowledgedFence, retryAttempt + 1);
                        }
                        return internalAllocateOwnedStreamId(
                            streamKey, owner, acknowledgedFence, retryAttempt + 1);
                    });
            });
    }

    private CompletableFuture<StreamIdAllocation> completeOwnedKeyedAllocation(
            long streamId, String streamKey, String keyPath,
            StreamIdMappingOwner owner, boolean createdKeyedMapping) {
        return ensureStreamRegistered(streamId, streamKey)
            .thenCompose(registeredStreamId -> validateOwnedKeyedAllocation(
                registeredStreamId, keyPath, owner, createdKeyedMapping));
    }

    private CompletableFuture<StreamIdAllocation> validateOwnedKeyedAllocation(
            long streamId, String keyPath, StreamIdMappingOwner owner,
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
                        PersistedStreamIdMapping mapping =
                            deserializeStreamIdMapping(read.result().value());
                        if (mapping.state() == StreamIdMappingState.ACTIVE
                                && mapping.streamId() == streamId
                                && mapping.owner().equals(owner)) {
                            return CompletableFuture.completedFuture(
                                new StreamIdAllocation(streamId, createdKeyedMapping));
                        }
                    } catch (RuntimeException e) {
                        invalidation = e;
                    }
                }
                if (invalidation == null) {
                    invalidation = new IllegalStateException(
                        "Keyed stream-ID mapping changed while registering " + keyPath);
                }
                return CompletableFuture.failedFuture(
                    new KeyedAllocationInvalidatedException(
                        new StreamIdAllocation(streamId, createdKeyedMapping), invalidation));
            });
    }

    private static <T> CompletableFuture<T> differentMappingOwner(
            String streamKey, PersistedStreamIdMapping current) {
        return CompletableFuture.failedFuture(new StreamIdMappingConflictException(
            streamKey, new ActiveStreamIdMapping(current.streamId(), current.owner())));
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
                    PersistedStreamIdMapping mapping =
                        deserializeStreamIdMapping(result.value());
                    if (mapping.state() == StreamIdMappingState.TOMBSTONE) {
                        return CompletableFuture.failedFuture(new IllegalStateException(
                            "Keyed stream-ID mapping is fenced at " + keyPath));
                    }
                    return completeKeyedAllocation(
                        mapping.streamId(), streamKey, keyPath, false);
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
                            PersistedStreamIdMapping mapping =
                                deserializeStreamIdMapping(read.result().value());
                            if (mapping.state() == StreamIdMappingState.ACTIVE
                                    && mapping.streamId() == streamId) {
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
                        PersistedStreamIdMapping mapping =
                            deserializeStreamIdMapping(read.result().value());
                        if (mapping.state() == StreamIdMappingState.ACTIVE
                                && mapping.streamId() == streamId) {
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
        return withLocalWritePermit(streamId,
            () -> writeNonCompactedIndexInternal(
                streamId, numberOfMessages, entrySize, position));
    }

    private CompletableFuture<PutResult> writeNonCompactedIndexInternal(
            long streamId, int numberOfMessages, int entrySize, Position position) {
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
        return withLocalWritePermit(streamId,
            () -> writeNonCompactedIndexInternal(
                streamId, numberOfMessages, entrySize,
                initialOffset, cumulativeSize, position));
    }

    private CompletableFuture<PutResult> writeNonCompactedIndexInternal(
            long streamId, int numberOfMessages, int entrySize,
            long initialOffset, long cumulativeSize, Position position) {
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
        return withLocalWritePermit(streamId,
            () -> writeInternal(
                streamId, numberOfMessages, initialOffset, cumulativeSize, data));
    }

    private CompletableFuture<AddResult> writeInternal(
            long streamId, int numberOfMessages, long initialOffset,
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
                                        ? writeNonCompactedIndexInternal(streamId, numberOfMessages, entrySize,
                                        initialOffset, cumulativeSize, putResult.position())
                                        : writeNonCompactedIndexInternal(streamId, numberOfMessages, entrySize,
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
        return withLocalWritePermit(streamId,
            () -> softTrimStreamInternal(streamId, offsetIncluded));
    }

    private CompletableFuture<Long> softTrimStreamInternal(
            long streamId, long offsetIncluded) {
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
    public boolean supportsDurableStreamWriteFencing() {
        return true;
    }

    private LocalWriteGate registerLocalWriteLease(long streamId) {
        AtomicReference<LocalWriteGate> registered = new AtomicReference<>();
        localWriteGates.compute(streamId, (ignored, current) -> {
            LocalWriteGate gate = current == null ? new LocalWriteGate() : current;
            gate.openLease();
            registered.set(gate);
            return gate;
        });
        return registered.get();
    }

    private CompletableFuture<Void> beginLocalWriteLeaseClose(
            long streamId, LocalWriteGate expectedGate) {
        AtomicReference<CompletableFuture<Void>> drain = new AtomicReference<>();
        localWriteGates.compute(streamId, (ignored, current) -> {
            if (current != expectedGate) {
                throw new IllegalStateException(
                    "Local write lease state is missing for stream " + streamId);
            }
            drain.set(current.closeLease());
            return current;
        });
        return drain.get();
    }

    private void removeLocalWriteGateIfIdle(
            long streamId, LocalWriteGate expectedGate) {
        localWriteGates.compute(streamId, (ignored, current) ->
            current == expectedGate && current.isIdle() ? null : current);
    }

    private LocalWritePermit acquireLocalWritePermit(long streamId) {
        AtomicReference<LocalWritePermit> permit = new AtomicReference<>();
        localWriteGates.computeIfPresent(streamId, (ignored, gate) -> {
            if (gate.enterMutation()) {
                permit.set(new LocalWritePermit(gate));
            }
            return gate;
        });
        return permit.get();
    }

    private <T> CompletableFuture<T> withLocalWritePermit(
            long streamId, Supplier<CompletableFuture<T>> mutation) {
        LocalWritePermit permit = acquireLocalWritePermit(streamId);
        if (permit == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "Writing stream " + streamId + " requires an active durable write lease"));
        }
        final CompletableFuture<T> operation;
        try {
            operation = Objects.requireNonNull(mutation.get(), "storage mutation future");
        } catch (RuntimeException | Error failure) {
            permit.close();
            throw failure;
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        operation.whenComplete((value, failure) -> {
            permit.close();
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(failure);
            }
        });
        return OwnedResultFutures.nonCancellableCompletion(result);
    }

    @Override
    public CompletableFuture<StreamWriteLease> acquireStreamWriteLease(long streamId) {
        Optional<IllegalArgumentException> invalid = validateStreamId(streamId);
        if (invalid.isPresent()) {
            return CompletableFuture.failedFuture(invalid.orElseThrow());
        }
        Optional<? extends RuntimeException> localFence = LogStateUtil.toException(
            streamStateManager.getState(streamId), streamId);
        if (localFence.isPresent()) {
            return CompletableFuture.failedFuture(localFence.orElseThrow());
        }

        String leaseToken = UUID.randomUUID().toString();
        String leaseKey = streamWriteLeasePrefix(streamId) + leaseToken;
        byte[] leaseValue = leaseToken.getBytes(StandardCharsets.UTF_8);
        Set<PutOption> createOptions = Set.of(
            PutOption.AsEphemeralRecord,
            PutOption.IfRecordDoesNotExist,
            PutOption.PartitionKey(streamWriteLifecyclePartitionKey(streamId)));
        return oxiaClient.put(leaseKey, leaseValue, createOptions)
            .handle((created, createFailure) -> createFailure == null ? null
                : FutureUtils.unwrapCompletionException(createFailure))
            .thenCompose(createFailure -> {
                if (createFailure != null) {
                    return CompletableFuture.failedFuture(createFailure);
                }
                Set<GetOption> getOptions = Set.of(
                    GetOption.PartitionKey(streamWriteLifecyclePartitionKey(streamId)));
                return oxiaClient.get(streamWriteFenceKey(streamId), getOptions)
                    .handle((fence, readFailure) -> new StreamWriteFenceRead(
                        fence, readFailure == null ? null
                            : FutureUtils.unwrapCompletionException(readFailure)))
                    .thenCompose(read -> {
                        if (read.failure() != null) {
                            return rejectStreamWriteLease(
                                streamId, leaseKey, read.failure());
                        }
                        if (read.fence() != null
                                || streamStateManager.getState(streamId) == LogState.FENCED) {
                            streamStateManager.setState(streamId, LogState.FENCED);
                            RuntimeException fenced = LogStateUtil.toException(
                                LogState.FENCED, streamId).orElseThrow();
                            return rejectStreamWriteLease(streamId, leaseKey, fenced);
                        }
                        LocalWriteGate localGate = registerLocalWriteLease(streamId);
                        return CompletableFuture.completedFuture(
                            new OxiaStreamWriteLease(streamId, leaseKey, localGate));
                    });
            });
    }

    private CompletableFuture<StreamWriteLease> rejectStreamWriteLease(
            long streamId, String leaseKey, Throwable failure) {
        CompletableFuture<StreamWriteLease> result = new CompletableFuture<>();
        deleteStreamWriteLeaseEventually(streamId, leaseKey).whenComplete((ignored, cleanupFailure) -> {
            if (cleanupFailure != null && cleanupFailure != failure) {
                failure.addSuppressed(FutureUtils.unwrapCompletionException(cleanupFailure));
            }
            result.completeExceptionally(failure);
        });
        return result;
    }

    @Override
    public CompletableFuture<Void> fenceStreamWrites(long streamId) {
        Optional<? extends RuntimeException> invalid = validateStreamId(streamId);
        if (invalid.isPresent()) {
            return CompletableFuture.failedFuture(invalid.orElseThrow());
        }
        Set<PutOption> options = Set.of(
            PutOption.IfRecordDoesNotExist,
            PutOption.PartitionKey(streamWriteLifecyclePartitionKey(streamId)));
        return oxiaClient.put(
                streamWriteFenceKey(streamId), STREAM_WRITE_LIFECYCLE_MARKER, options)
            .handle((write, failure) -> failure == null ? null
                : FutureUtils.unwrapCompletionException(failure))
            .thenCompose(failure -> {
                if (failure != null && !(failure instanceof KeyAlreadyExistsException)) {
                    return CompletableFuture.failedFuture(failure);
                }
                streamStateManager.setState(streamId, LogState.FENCED);
                return CompletableFuture.completedFuture(null);
            });
    }

    @Override
    public CompletableFuture<Void> deleteStream(long streamId) {
        return deleteStream(streamId, DEFAULT_STREAM_WRITE_LEASE_DRAIN_TIMEOUT);
    }

    @Override
    public CompletableFuture<Void> deleteStream(long streamId, Duration leaseDrainTimeout) {
        return fenceAndDrainStreamWrites(streamId, leaseDrainTimeout)
            .thenCompose(__ -> purgeStream(streamId));
    }

    @Override
    public CompletableFuture<Void> fenceAndDrainStreamWrites(
            long streamId, Duration leaseDrainTimeout) {
        Objects.requireNonNull(leaseDrainTimeout, "leaseDrainTimeout");
        Optional<? extends RuntimeException> invalid = validateStreamId(streamId);
        if (invalid.isPresent()) {
            return CompletableFuture.failedFuture(invalid.orElseThrow());
        }
        if (leaseDrainTimeout.isNegative()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "leaseDrainTimeout must not be negative"));
        }
        final long timeoutNanos;
        try {
            timeoutNanos = leaseDrainTimeout.toNanos();
        } catch (ArithmeticException e) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "leaseDrainTimeout is too large", e));
        }
        return fenceStreamWrites(streamId)
            .thenCompose(__ -> waitForStreamWriteLeasesToDrain(
                streamId, leaseDrainTimeout, timeoutNanos, System.nanoTime(),
                StreamWriteLeaseDrainTimeoutException.UNKNOWN_ACTIVE_LEASE_COUNT, true));
    }

    private CompletableFuture<Void> waitForStreamWriteLeasesToDrain(
            long streamId,
            Duration timeout,
            long timeoutNanos,
            long startNanos,
            int lastObservedActiveLeaseCount,
            boolean firstPoll) {
        long remainingNanos = remainingNanos(timeoutNanos, startNanos);
        if (!firstPoll && remainingNanos <= 0) {
            return CompletableFuture.failedFuture(
                new StreamWriteLeaseDrainTimeoutException(
                    streamId, lastObservedActiveLeaseCount, timeout));
        }
        String prefix = streamWriteLeasePrefix(streamId);
        Set<ListOption> options = Set.of(
            ListOption.PartitionKey(streamWriteLifecyclePartitionKey(streamId)));
        final CompletableFuture<List<String>> inventoryRead;
        try {
            inventoryRead = Objects.requireNonNull(
                oxiaClient.list(prefix, prefix + "\uffff", options),
                "write lease inventory future");
        } catch (RuntimeException | Error failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return boundLeaseInventoryRead(
                inventoryRead, streamId, lastObservedActiveLeaseCount, timeout, remainingNanos)
            .thenCompose(activeLeases -> {
                if (activeLeases.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                long remainingAfterRead = remainingNanos(timeoutNanos, startNanos);
                if (remainingAfterRead <= 0) {
                    return CompletableFuture.failedFuture(
                        new StreamWriteLeaseDrainTimeoutException(
                            streamId, activeLeases.size(), timeout));
                }
                long retryDelayNanos = Math.min(
                    TimeUnit.MILLISECONDS.toNanos(
                        STREAM_WRITE_LEASE_DRAIN_POLL_INTERVAL_MS),
                    remainingAfterRead);
                return CompletableFuture.runAsync(
                        () -> { }, CompletableFuture.delayedExecutor(
                            retryDelayNanos, TimeUnit.NANOSECONDS))
                    .thenCompose(__ -> waitForStreamWriteLeasesToDrain(
                        streamId, timeout, timeoutNanos, startNanos,
                        activeLeases.size(), false));
            });
    }

    private static CompletableFuture<List<String>> boundLeaseInventoryRead(
            CompletableFuture<List<String>> source,
            long streamId,
            int lastObservedActiveLeaseCount,
            Duration timeout,
            long remainingNanos) {
        CompletableFuture<List<String>> bounded = new CompletableFuture<>();
        CompletableFuture<Void> deadline = new CompletableFuture<>();
        source.whenComplete((leases, failure) -> {
            if (failure == null) {
                bounded.complete(leases);
            } else if (source.isCancelled()) {
                bounded.cancel(false);
            } else {
                bounded.completeExceptionally(failure);
            }
            deadline.complete(null);
        });
        try {
            deadline.orTimeout(Math.max(remainingNanos, 0L), TimeUnit.NANOSECONDS)
                .whenComplete((ignored, failure) -> {
                    if (failure != null) {
                        bounded.completeExceptionally(
                            new StreamWriteLeaseDrainTimeoutException(
                                streamId, lastObservedActiveLeaseCount, timeout));
                    }
                });
        } catch (RuntimeException | Error schedulingFailure) {
            bounded.completeExceptionally(schedulingFailure);
        }
        return bounded;
    }

    private static long remainingNanos(long timeoutNanos, long startNanos) {
        long elapsedNanos = System.nanoTime() - startNanos;
        if (elapsedNanos <= 0) {
            return timeoutNanos;
        }
        return timeoutNanos - Math.min(elapsedNanos, timeoutNanos);
    }

    private CompletableFuture<Void> purgeStream(long streamId) {
        return oxiaClient.deleteRange(
                getSmallestStreamIdKey(streamId),
                getLargestStreamIdKey(streamId),
                Set.of(DeleteRangeOption.PartitionKey(String.valueOf(streamId))))
            .thenCompose(__ -> removeStream(streamId))
            .thenRun(() -> storageFormat.removeCachedKey(streamId));
    }

    private CompletableFuture<Void> deleteStreamWriteLease(long streamId, String leaseKey) {
        return oxiaClient.delete(leaseKey, Set.of(
                DeleteOption.PartitionKey(streamWriteLifecyclePartitionKey(streamId))))
            .thenApply(__ -> null);
    }

    private CompletableFuture<Void> deleteStreamWriteLeaseEventually(
            long streamId, String leaseKey) {
        CompletableFuture<Void> release = new CompletableFuture<>();
        attemptStreamWriteLeaseDelete(streamId, leaseKey, 0, release);
        return release;
    }

    private void attemptStreamWriteLeaseDelete(
            long streamId, String leaseKey, int retryAttempt,
            CompletableFuture<Void> release) {
        final CompletableFuture<Void> deletion;
        try {
            deletion = Objects.requireNonNull(
                deleteStreamWriteLease(streamId, leaseKey), "stream write lease delete future");
        } catch (RuntimeException failure) {
            scheduleStreamWriteLeaseDeleteRetry(
                streamId, leaseKey, retryAttempt, release, failure);
            return;
        }
        deletion.whenComplete((ignored, failure) -> {
            if (failure == null) {
                release.complete(null);
                return;
            }
            scheduleStreamWriteLeaseDeleteRetry(
                streamId, leaseKey, retryAttempt, release,
                FutureUtils.unwrapCompletionException(failure));
        });
    }

    private void scheduleStreamWriteLeaseDeleteRetry(
            long streamId, String leaseKey, int retryAttempt,
            CompletableFuture<Void> release, Throwable failure) {
        long delayMillis = streamWriteLeaseReleaseRetryDelayMillis(retryAttempt);
        int nextAttempt = retryAttempt == Integer.MAX_VALUE
            ? Integer.MAX_VALUE : retryAttempt + 1;
        log.warn("Failed to release durable write lease {} for stream {}; retrying in {} ms "
                + "(attempt {})", leaseKey, streamId, delayMillis, nextAttempt, failure);
        CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS).execute(
            () -> attemptStreamWriteLeaseDelete(
                streamId, leaseKey, nextAttempt, release));
    }

    private static long streamWriteLeaseReleaseRetryDelayMillis(int retryAttempt) {
        int shift = Math.min(Math.max(retryAttempt, 0), 30);
        long exponentialDelay = STREAM_WRITE_LEASE_RELEASE_INITIAL_RETRY_DELAY_MS << shift;
        return Math.min(exponentialDelay, STREAM_WRITE_LEASE_RELEASE_MAX_RETRY_DELAY_MS);
    }

    private static Optional<IllegalArgumentException> validateStreamId(long streamId) {
        return streamId < 0
            ? Optional.of(new IllegalArgumentException("streamId must be non-negative"))
            : Optional.empty();
    }

    @VisibleForTesting
    static String streamWriteFenceKey(long streamId) {
        return STREAM_WRITE_FENCE_PATH + "/" + streamId;
    }

    @VisibleForTesting
    static String streamWriteLeasePrefix(long streamId) {
        return STREAM_WRITE_LEASE_PATH + "/" + streamId + "/";
    }

    private static String streamWriteLifecyclePartitionKey(long streamId) {
        return String.valueOf(streamId);
    }

    private record StreamWriteFenceRead(GetResult fence, Throwable failure) {
    }

    private final class OxiaStreamWriteLease implements StreamWriteLease {

        private final long streamId;
        private final String leaseKey;
        private final LocalWriteGate localGate;
        private final AtomicReference<CompletableFuture<Void>> closeFuture =
            new AtomicReference<>();
        private final AtomicReference<CompletableFuture<Void>> localDrainFuture =
            new AtomicReference<>();

        private OxiaStreamWriteLease(
                long streamId, String leaseKey, LocalWriteGate localGate) {
            this.streamId = streamId;
            this.leaseKey = leaseKey;
            this.localGate = localGate;
        }

        @Override
        public long streamId() {
            return streamId;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> existing = closeFuture.get();
            if (existing != null) {
                return existing;
            }
            CompletableFuture<Void> source = new CompletableFuture<>();
            CompletableFuture<Void> exposed =
                OwnedResultFutures.nonCancellableCompletion(source);
            if (!closeFuture.compareAndSet(null, exposed)) {
                return closeFuture.get();
            }
            CompletableFuture<Void> drain = localDrainFuture.get();
            if (drain == null) {
                try {
                    drain = beginLocalWriteLeaseClose(streamId, localGate);
                    localDrainFuture.compareAndSet(null, drain);
                } catch (RuntimeException | Error failure) {
                    source.completeExceptionally(failure);
                    return exposed;
                }
            }
            drain.thenCompose(__ -> deleteStreamWriteLeaseEventually(streamId, leaseKey))
                .whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        removeLocalWriteGateIfIdle(streamId, localGate);
                        source.complete(null);
                    } else {
                        source.completeExceptionally(
                            FutureUtils.unwrapCompletionException(failure));
                    }
                });
            return exposed;
        }
    }

    @Override
    @Deprecated
    public CompletableFuture<Void> deleteStream(long streamId, Optional<String> key) {
        Objects.requireNonNull(key, "key");
        if (key.isPresent()) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Keyed stream deletion is disabled; fence and acknowledge the mapping "
                    + "through the durable lifecycle API"));
        }
        return deleteStream(streamId);
    }

    @Override
    public CompletableFuture<Void> compactEntryIndex(long streamId, long startOffset, long endOffset,
                                                     long endCumulativeSize, Value value) {
        return withLocalWritePermit(streamId,
            () -> compactEntryIndexInternal(
                streamId, startOffset, endOffset, endCumulativeSize, value));
    }

    private CompletableFuture<Void> compactEntryIndexInternal(
            long streamId, long startOffset, long endOffset,
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

    /**
     * Makes the initialized WAL data plane available to a previously requested cleanup service.
     * Marker initialization and retries run asynchronously on the cleaner's supervised executor.
     */
    public void onWALDataPlaneAvailable(FileStorage initializedFileStorage) {
        asyncCleaner.onDataPlaneAvailable(initializedFileStorage);
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
        asyncCleaner.stop();
    }

    @Override
    public LogStateManager getStreamStateManager() {
        return streamStateManager;
    }

    @Override
    public CompletableFuture<Void> hardTrimStream(long streamId, long offsetExcluded) {
        return withLocalWritePermit(streamId, () -> oxiaClient.deleteRange(
                        Key.smallestKey(streamId).toString(),
                        Key.largestKey(streamId, offsetExcluded).toString(),
                        Set.of(DeleteRangeOption.PartitionKey(String.valueOf(streamId)))));
    }
}
