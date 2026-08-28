/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.BatchResult;
import com.google.cloud.NoCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.FileStorageMetrics;
import io.lakestream.ursa.storage.IDGeneratorWithDate;
import io.lakestream.ursa.storage.impl.exception.FileStorageException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.threeten.bp.Duration;

@Slf4j
public class GCSFileStorage implements FileStorage {

    private final Storage storage;
    private final String bucket;
    private final String prefix;
    private final ExecutorService executors;
    private final RateLimiter rateLimiter;
    private final FileStorageMetrics metrics;

    // metrics for itself
    private final LatencyHistogram pendingRequestDuration;

    private ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    private boolean useDataSizeAsChunkSize = true;

    public GCSFileStorage(StorageConfig config, InstrumentProvider provider) {
        this.bucket = config.getBucket();
        this.prefix = config.getPrefix();
        var rateLimit = config.getCloudStorageOpsRateLimitPerSecond() != -1
            ? config.getCloudStorageOpsRateLimitPerSecond() : config.getWriteBufferSegment();
        this.rateLimiter = RateLimiter.create(rateLimit);
        this.executors = Executors.newFixedThreadPool(config.getCloudStorageMaxConcurrencyRequest(),
            new DefaultThreadFactory("gcs-file-storage-worker"));

        try {
            storage = buildStorageClient(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.metrics = new FileStorageMetrics(provider, Type.GCS.name());

        this.pendingRequestDuration = provider.newLatencyHistogram("ursa.storage.backend.pending.duration",
            "GCS pending request duration", Attributes.of(AttributeKey.stringKey("type"), Type.GCS.name()));
        this.useDataSizeAsChunkSize = config.getProperties()
            .getOrDefault("useDataSizeAsChunkSize", "true").equals("true");
    }

    Storage buildStorageClient(StorageConfig config) throws IOException {
        var builder = StorageOptions.newBuilder();
        if (StringUtils.isNotEmpty(config.getCloudStorageEndpoint())) {
            builder.setHost(config.getCloudStorageEndpoint());
        }
        var projectId = config.getProperties().getProperty("projectId");
        if (StringUtils.isNotEmpty(projectId)) {
            builder.setProjectId(projectId);
        }

        boolean disableCredential = Boolean.parseBoolean(config.getProperties().getProperty("disableCredential"));
        if (disableCredential) {
            builder.setCredentials(NoCredentials.getInstance());
        } else {
            builder.setCredentials(GoogleCredentials.getApplicationDefault());
        }
        builder.setRetrySettings(RetrySettings.newBuilder()
            .setTotalTimeout(Duration.ofSeconds(30))
            .build());
        return builder.build().getService();
    }

    private BlobId getBlobId(String location) {
        return BlobId.of(bucket, prefix + "/" + location);
    }

    @Override
    public void put(ByteBuf data, String location) throws IOException {
        try {
            putAsync(data, location).get();
        } catch (Exception e) {
            final String errorMsg = "Failed to put object to the GCS bucket " + bucket + " with the key " + location;
            if (e instanceof ExecutionException) {
                throw new FileStorageException(errorMsg, e.getCause());
            } else {
                throw new FileStorageException(errorMsg, e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> putAsync(ByteBuf data, String location) {
        long start = System.nanoTime();
        return doOpAsync(() -> {
            BlobInfo blobInfo = BlobInfo.newBuilder(getBlobId(location)).build();
            try (WriteChannel wc = storage.writer(blobInfo)) {
                if (useDataSizeAsChunkSize) {
                    wc.setChunkSize(data.readableBytes());
                }
                var buffer = data.nioBuffer();
                while (buffer.hasRemaining()) {
                    int bytesWritten = wc.write(buffer);
                    if (bytesWritten <= 0) {
                        // If no bytes were written, give a small pause before retrying
                        Thread.sleep(10);
                    }
                }
            }
            return null;
        }).whenComplete((__, t) -> {
            if (t != null) {
                metrics.getWriteStorageLatency().recordFailure(System.nanoTime() - start);
            } else {
                metrics.getWriteBytesCount().add(data.readableBytes());
                metrics.getWriteStorageLatency().recordSuccess(System.nanoTime() - start);
            }
        }).thenApply(__ -> null);
    }

    @Override
    public ByteBuf get(String location) throws FileStorageException {
        try {
            return getAsync(location).get();
        } catch (Exception e) {
            final String errorMsg = "Failed to get object from the GCS bucket " + bucket + " with key " + location;
            if (e instanceof ExecutionException) {
                throw new FileStorageException(errorMsg, e.getCause());
            } else {
                throw new FileStorageException(errorMsg, e);
            }
        }
    }

    @Override
    public CompletableFuture<ByteBuf> getAsync(String location) {
        long start = System.nanoTime();
        return doOpAsync(() -> {
            var blobId = getBlobId(location);
            Blob blob = storage.get(blobId);
            if (blob == null) {
                throw new FileStorageException("The blob " + blobId + " does not exist");
            }
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(Math.toIntExact(blob.getSize()));
            try (var reader = blob.reader()) {
                if (useDataSizeAsChunkSize) {
                    reader.setChunkSize(Math.toIntExact(blob.getSize()));
                }
                // Read until either EOF (-1) or buffer is full
                int bytesRead;
                while (buf.readableBytes() < blob.getSize()) {
                    bytesRead = reader.read(buf.nioBuffer(buf.writerIndex(), buf.writableBytes()));
                    if (bytesRead == -1) {
                        // EOF reached before reading expected size
                        if (buf.readableBytes() != blob.getSize()) {
                            throw new FileStorageException("Unexpected EOF: read " + buf.readableBytes()
                                + " bytes, expected " + blob.getSize() + " bytes");
                        }
                        break;
                    } else if (bytesRead > 0) {
                        buf.writerIndex(buf.writerIndex() + bytesRead);
                    } else {
                        // bytesRead = 0 is normal for non-blocking channels, just continue reading
                        Thread.sleep(10);
                    }
                }
                return buf.retain();
            } finally {
                buf.release();
            }
        }).whenComplete((data, t) -> {
            if (t != null) {
                metrics.getReadStorageLatency().recordFailure(System.nanoTime() - start);
            } else {
                metrics.getReadBytesCount().add(data.readableBytes());
                metrics.getReadStorageLatency().recordSuccess(System.nanoTime() - start);
            }
        });
    }

    @Override
    public void delete(String location) throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CompletableFuture<Void> deleteAsync(List<String> locations) {
        // The GCS API will handle the deletion of multiple blobs in a single batch operation. This process splits
        // the tasks based on MAX_BATCH_SIZE, so we don't need to split them here.
        long start = System.nanoTime();
        return doOpAsync(() -> {
            var blobIds = locations.stream().map(l -> BlobId.of(bucket, l)).toList();

            StorageBatch batch = storage.batch();
            var deletedBlobs = new ArrayList<String>();
            var failedBlobs = new ArrayList<String>();
            AtomicReference<StorageException> exceptionRef = new AtomicReference<>();

            for (BlobId blob : blobIds) {
                batch.delete(blob).notify(new BatchResult.Callback<>() {
                    @Override
                    public void success(Boolean result) {
                        // If the blob was not found, it will return false, ignore it here.
                        if (result) {
                            deletedBlobs.add(blob.getName());
                        }
                    }

                    @Override
                    public void error(StorageException storageException) {
                        exceptionRef.set(storageException);
                        failedBlobs.add(blob.getName());
                    }
                });
            }
            batch.submit();

            if (!deletedBlobs.isEmpty()) {
                log.info("Deleted blobs in GCS bucket {}: {}", bucket, deletedBlobs);
            }

            StorageException exception = exceptionRef.get();
            if (exception != null) {
                metrics.getDeleteStorageLatency().recordFailure(System.nanoTime() - start);
                log.error("Failed to delete some blobs in the GCS bucket {}: {}", bucket, failedBlobs, exception);
                throw new FileStorageException("Failed to delete some blobs in the GCS bucket " + bucket, exception);
            }

            metrics.getDeleteStorageLatency().recordSuccess(System.nanoTime() - start);
            metrics.getRequests().increment(Attributes.of(AttributeKey.stringKey("operation"), "delete"));
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> deleteWithDatePrefixes(Set<String> prefixes) throws IOException {
        return doOpAsync(() -> {
            Bucket b = storage.get(bucket);
            var rules = b.getLifecycleRules();
            var updatedRules = updateRules(rules, prefixes);
            BucketInfo updatedBucketInfo = b.toBuilder().setLifecycleRules(updatedRules).build();
            storage.update(updatedBucketInfo);
            return null;
        });
    }

    List<? extends BucketInfo.LifecycleRule> updateRules(List<? extends BucketInfo.LifecycleRule> rules,
                                                         Set<String> prefixes) {
        var bucketPrefix = getBucketPrefixForLifecycleRules();
        var nonUrsaRules = new ArrayList<BucketInfo.LifecycleRule>();
        var finalPrefixes = new HashSet<String>();
        LocalDateTime now = LocalDateTime.now();
        for (BucketInfo.LifecycleRule rule : rules) {
            if (BucketInfo.LifecycleRule.DeleteLifecycleAction.TYPE.equals(rule.getAction().getActionType())) {
                var condition = rule.getCondition();
                var matchesPrefix = condition.getMatchesPrefix();
                for (String s : matchesPrefix) {
                    if (!s.startsWith(bucketPrefix)) {
                        nonUrsaRules.add(rule);
                        break;
                    }
                    var p = s.replaceFirst(bucketPrefix + "/", "");
                    if (!IDGeneratorWithDate.isDatePrefixOverThan(p, java.time.Duration.ofDays(7), now)) {
                        finalPrefixes.add(p);
                    }
                }
            } else {
                nonUrsaRules.add(rule);
            }
        }

        finalPrefixes.addAll(prefixes);

        var ursaRules = buildLifecycleRules(finalPrefixes);
        nonUrsaRules.addAll(ursaRules);
        return nonUrsaRules;
    }

    // limitations of the prefixes in the rule: https://cloud.google.com/storage/docs/lifecycle#matchesprefix-suffix
    List<? extends BucketInfo.LifecycleRule> buildLifecycleRules(Set<String> prefixes) {
        var bucketPrefix = getBucketPrefixForLifecycleRules();
        var conditionPrefixes = prefixes.stream().map(prefix -> bucketPrefix + "/" + prefix).toList();
        var action = BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction();

        var condition = BucketInfo.LifecycleRule.LifecycleCondition.newBuilder()
            .setAge(0)
            .setMatchesPrefix(conditionPrefixes)
            .build();
        BucketInfo.LifecycleRule lifecycleRule = new BucketInfo.LifecycleRule(action, condition);
        return List.of(lifecycleRule);
    }

    private String getBucketPrefixForLifecycleRules() {
        return prefix.startsWith("/") ? prefix.replaceFirst("/", "") : prefix;
    }

    <T> CompletableFuture<T> doOpAsync(Op<T> op) {
        CompletableFuture<T> future = new CompletableFuture<>();
        long start = System.currentTimeMillis();
        executors.execute(() -> {
            rateLimiter.acquire();
            pendingRequestDuration.recordSuccess(System.currentTimeMillis() - start);
            try {
                T result = op.run();
                future.complete(result);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    interface Op<T> {
        T run() throws Exception;
    }

    @Override
    public void close() throws Exception {
        executors.shutdown();
    }
}
