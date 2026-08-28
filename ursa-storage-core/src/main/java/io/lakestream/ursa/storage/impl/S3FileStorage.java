/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.FileStorageMetrics;
import io.lakestream.ursa.storage.IDGeneratorWithDate;
import io.lakestream.ursa.storage.impl.exception.FileStorageException;
import io.lakestream.ursa.storage.impl.exception.RetryableException;
import io.lakestream.ursa.utils.FutureUtils;
import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.CRC32C;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Slf4j
public class S3FileStorage implements FileStorage {

    private final String bucket;
    private final String bucketPrefix;
    private final S3AsyncClient s3AsyncClient;

    private final FileStorageMetrics metrics;

    private final Map<String, Map<String, String>> putMetadata = new HashMap<>();
    private final SdkAsyncHttpClient nettyHttpClient;
    private RateLimiter rateLimiter = null;

    // Maximum number of objects that can be deleted in a single S3 DeleteObjects request
    private static final int MAX_OBJECTS_PER_DELETE_REQUEST = 1000;

    public S3FileStorage(StorageConfig config, InstrumentProvider instrumentProvider) {
        this.metrics = new FileStorageMetrics(instrumentProvider, Type.S3.name());
        this.bucket = StringUtils.isEmpty(config.getS3Bucket()) ? config.getBucket() : config.getS3Bucket();
        this.bucketPrefix = StringUtils.isEmpty(config.getS3Prefix()) ? config.getPrefix() : config.getS3Prefix();

        if (config.getS3OpsRateLimitPerSecond() > 0) {
            this.rateLimiter = RateLimiter.create(config.getS3OpsRateLimitPerSecond());
        }

        NettyNioAsyncHttpClient.Builder nettyHttpClientBuilder = NettyNioAsyncHttpClient.builder()
            .tcpKeepAlive(true)
            .maxConcurrency(config.getCloudStorageMaxConcurrencyRequest());

        var pendingConnectionAcquires = config.getS3MaxPendingConnectionAcquires() > 0
            ? config.getS3MaxPendingConnectionAcquires() : config.getCloudStorageMaxPendingConnectionAcquires();
        if (pendingConnectionAcquires > 0) {
            nettyHttpClientBuilder.maxPendingConnectionAcquires(pendingConnectionAcquires);
        }

        var connectionAcquisitionTimeoutMs = config.getS3ConnectionAcquisitionTimeoutMs() > 0
            ? config.getS3ConnectionAcquisitionTimeoutMs() : config.getCloudStorageMaxPendingAcquireTimeoutInMs();
        if (connectionAcquisitionTimeoutMs > 0) {
            nettyHttpClientBuilder.connectionAcquisitionTimeout(Duration.ofMillis(connectionAcquisitionTimeoutMs));
        }

        this.nettyHttpClient = nettyHttpClientBuilder.build();

        S3AsyncClientBuilder builder = S3AsyncClient.builder()
            .forcePathStyle(true)
            .httpClient(this.nettyHttpClient);

        if (config.getS3OpsMaxRetries() > 0) {
            builder.overrideConfiguration(ClientOverrideConfiguration.builder()
                .retryPolicy(RetryPolicy.defaultRetryPolicy().toBuilder()
                    .numRetries(config.getS3OpsMaxRetries())
                    .build())
                .build());
        }

        if (StringUtils.isNotEmpty(config.getS3Region())) {
            builder.region(Region.of(config.getS3Region()));
        }

        if (StringUtils.isNotEmpty(config.getRegion())) {
            builder.region(Region.of(config.getRegion()));
        }

        if (config.getCloudStorageEndpoint() != null) {
            builder.endpointOverride(URI.create(config.getCloudStorageEndpoint()));
        }
        if (config.getS3AccessKeyId() != null && config.getS3SecretAccessKey() != null) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(config.getS3AccessKeyId(), config.getS3SecretAccessKey())));
        } else {
            var credentialChain = AwsCredentialsProviderChain.of(
                ProfileCredentialsProvider.create(),
                WebIdentityTokenFileCredentialsProvider.create());
            builder.credentialsProvider(credentialChain);
        }
        builder.disableS3ExpressSessionAuth(config.isDisableS3ExpressSessionAuth());

        s3AsyncClient = builder.build();
    }

    @Override
    public void put(ByteBuf data, String location) throws IOException {
        try {
            putAsync(data, location).get();
        } catch (Exception e) {
            final String errorMsg = "Failed to put object to the S3 bucket " + bucket + " with the key " + location;
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
        String key = StringUtils.isEmpty(bucketPrefix) ? location : bucketPrefix + "/" + location;
        String crc32cBase64 = calculateCRC32CBase64Encoded(data);
        metrics.getCalculateCrcLatency().recordSuccess(System.nanoTime() - start);

        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
            .checksumCRC32C(crc32cBase64);
        Map<String, String> metadata = putMetadata.remove(location);
        if (metadata != null) {
            putObjectRequestBuilder.metadata(metadata);
        }
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }

        CompletableFuture<PutObjectResponse> writePromise =
            s3AsyncClient.putObject(putObjectRequest, AsyncRequestBody.fromByteBufferUnsafe(data.nioBuffer()));
        return writePromise.thenAccept(res -> {
            long now = System.nanoTime();
            metrics.getWriteStorageLatency().recordSuccess(now - start);
            metrics.getWriteBytesCount().add(data.readableBytes());
            metrics.getRequests().increment(Attributes.of(AttributeKey.stringKey("operation"), "put"));
        });
    }

    private static String calculateCRC32CBase64Encoded(ByteBuf data) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(data.nioBuffer());
        int crc32cValue = (int) crc32c.getValue();
        return Base64.getEncoder().encodeToString(new byte[]{
            (byte) (crc32cValue >>> 24),
            (byte) (crc32cValue >>> 16),
            (byte) (crc32cValue >>> 8),
            (byte) crc32cValue
        });
    }

    @Override
    public ByteBuf get(String location) throws IOException {
        return getWithMetadata(location).getLeft();
    }

    @Override
    public CompletableFuture<ByteBuf> getAsync(String location) {
        return getWithMetadataAsync(location).thenApply(Pair::getLeft);
    }

    private CompletableFuture<Pair<ByteBuf, Map<String, String>>> getWithMetadataAsync(String location) {
        return getWithMetadataAsync(location, new ByteBufAsyncResponseTransformer());
    }

    CompletableFuture<Pair<ByteBuf, Map<String, String>>> getWithMetadataAsync(
        String location, ByteBufAsyncResponseTransformer transformer) {

        long start = System.nanoTime();
        String key = StringUtils.isEmpty(bucketPrefix) ? location : bucketPrefix + "/" + location;
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .checksumMode(ChecksumMode.ENABLED)
            .build();
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        CompletableFuture<Pair<ByteBuf, Map<String, String>>> promise = s3AsyncClient
                .getObject(getObjectRequest, transformer)
                .handle((result, ex) -> {
                    if (ex != null) {
                        Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;

                        // Wrap specific retryable SdkClientException
                        if (cause instanceof software.amazon.awssdk.core.exception.RetryableException) {
                            throw new CompletionException(new RetryableException("Retryable S3 error", cause));
                        }

                        // Let others pass through unwrapped
                        throw new CompletionException(cause);
                    }

                    return result;
                });

        promise.whenComplete((pair, e) -> {
            metrics.getRequests().increment(Attributes.of(AttributeKey.stringKey("operation"), "get"));
            if (e != null) {
                log.error("Failed to get object with metadata from the S3 bucket {} with the key {}", bucket, key, e);
                metrics.getReadStorageLatency().recordFailure(System.nanoTime() - start);
            } else {
                metrics.getReadStorageLatency().recordSuccess(System.nanoTime() - start);
                metrics.getReadBytesCount().add(pair.getLeft().readableBytes());
            }
        });

        return promise;
    }

    public Pair<ByteBuf, Map<String, String>> getWithMetadata(String location) throws IOException {
        String key = StringUtils.isEmpty(bucketPrefix) ? location : bucketPrefix + "/" + location;
        try {
            return getWithMetadataAsync(location).get();
        } catch (Exception e) {
            String errorMsg = "Failed to get object from the bucket " + bucket + " with the key " + key;
            if (e instanceof ExecutionException) {
                throw new FileStorageException(errorMsg, e.getCause());
            } else {
                throw new FileStorageException(errorMsg, e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> deleteWithDatePrefixes(Set<String> prefixes) {
        CompletableFuture<List<LifecycleRule>> existingRulesFuture = new CompletableFuture<>();
        s3AsyncClient.getBucketLifecycleConfiguration(r -> r.bucket(bucket)).whenComplete((existingConfig, e) -> {
            e = FutureUtils.unwrapCompletionException(e);
            if (e == null) {
                // find the existing lifecycle rules
                if (existingConfig == null || !existingConfig.hasRules()) {
                    existingRulesFuture.complete(Collections.emptyList());
                } else {
                    existingRulesFuture.complete(existingConfig.rules());
                }
            } else if (e instanceof S3Exception && ((S3Exception) e).statusCode() == 404) {
                // no existing lifecycle rules
                existingRulesFuture.complete(Collections.emptyList());
            } else {
                existingRulesFuture.completeExceptionally(e);
            }
        });

        return existingRulesFuture.thenCompose(existingRules -> {
            List<LifecycleRule> newRules = buildRules(prefixes, existingRules);
            PutBucketLifecycleConfigurationRequest putRequest =
                PutBucketLifecycleConfigurationRequest.builder()
                    .bucket(bucket)
                    .lifecycleConfiguration(c -> c.rules(newRules))
                    .build();
            return s3AsyncClient.putBucketLifecycleConfiguration(putRequest);
        }).thenApply(x -> null);
    }

    @VisibleForTesting
    List<LifecycleRule> buildRules(Set<String> prefixes, List<LifecycleRule> existingRules) {
        List<String> prefixesCopy = new ArrayList<>(prefixes);
        // Because each put request will overwrite all the lifecycle configuration, we need to merge the existing rules
        // with the new rules.
        // There are two type of rules should keep:
        //   1. the rules id does not start with the LIFECYCLE_RULE_ID_PREFIX (that means the rule is not added by us)
        //   2. the rules added by us before with the same prefix, that means we already mark the prefix as expired,
        //      we don't need to update it anymore.
        // And for the rules added by us before, we need to check if the prefix is expired, if it is expired,
        // we don't need to update it anymore. By default, the expiration time is 7 day.
        List<LifecycleRule> rules = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        for (LifecycleRule existingRule : existingRules) {
            if (existingRule.id().startsWith(LIFECYCLE_RULE_ID_PREFIX)) {
                String prefixInFilters = existingRule.filter().prefix();
                String prefix = prefixInFilters.substring(
                    StringUtils.isEmpty(bucketPrefix) ? 0 : bucketPrefix.length() + 1);
                prefixesCopy.remove(prefix);

                // the prefix is expired, we don't need to update it anymore
                try {
                    if (!IDGeneratorWithDate.isDatePrefixOverThan(prefix, Duration.ofDays(7), now)) {
                        rules.add(existingRule);
                    }
                } catch (Exception e) {
                    log.warn("Failed to check the prefix '{}' in the existing lifecycle rules is "
                        + "over than 7 days, that may because the prefix is in the wrong format", prefix);
                    rules.add(existingRule);
                }
            } else {
                rules.add(existingRule);
            }
        }

        // add the new rules
        for (String prefix : prefixesCopy) {
            LifecycleRule rule = LifecycleRule.builder()
                .id(getLifecycleRuleID(prefix))
                .expiration(LifecycleExpiration.builder()
                    .days(1)
                    .build())
                .filter(LifecycleRuleFilter.builder()
                    .prefix(StringUtils.isEmpty(bucketPrefix) ? prefix : bucketPrefix + "/" + prefix)
                    .build())
                .status(ExpirationStatus.ENABLED)
                .build();
            rules.add(rule);
        }
        // sort is used to make sure we have a constant order of each update
        if (!rules.isEmpty()) {
            rules.sort(Comparator.comparing(LifecycleRule::id));
        }
        return rules;
    }

    String getLifecycleRuleID(String prefix) {
        String p = StringUtils.isEmpty(bucketPrefix) ? prefix : bucketPrefix + "/" + prefix;
        String prefixBase64 = Base64.getEncoder().encodeToString(p.getBytes(StandardCharsets.UTF_8));
        return LIFECYCLE_RULE_ID_PREFIX + prefixBase64;
    }

    @Override
    public void delete(String location) throws IOException {
        long start = System.nanoTime();
        String key = StringUtils.isEmpty(bucketPrefix) ? location : bucketPrefix + "/" + location;
        try {
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            s3AsyncClient.deleteObject(r -> r.bucket(bucket).key(key)).get();
            metrics.getDeleteStorageLatency().recordSuccess(System.nanoTime() - start);
            metrics.getRequests().increment(Attributes.of(AttributeKey.stringKey("operation"), "delete"));
        } catch (Exception e) {
            log.error("Failed to delete object from the S3 bucket {} with the key {}", bucket, key, e);
            metrics.getDeleteStorageLatency().recordFailure(System.nanoTime() - start);
            throw new IOException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(List<String> locations) {
        long start = System.nanoTime();
        // Split locations into batches if they exceed S3's limit of 1000 objects per request
        List<CompletableFuture<Void>> batchFutures = new ArrayList<>();

        for (int i = 0; i < locations.size(); i += MAX_OBJECTS_PER_DELETE_REQUEST) {
            int endIndex = Math.min(i + MAX_OBJECTS_PER_DELETE_REQUEST, locations.size());
            List<String> batch = locations.subList(i, endIndex);
            batchFutures.add(deleteBatchAsync(batch));
        }
        return CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]))
                .whenComplete((__, e) -> {
                    if (e != null) {
                        Throwable cause = FutureUtils.unwrapCompletionException(e);
                        log.error("Failed to delete objects from the S3 bucket {} with the key {}", bucket, locations,
                                cause);
                        metrics.getDeleteStorageLatency().recordFailure(System.nanoTime() - start);
                    } else {
                        metrics.getDeleteStorageLatency().recordSuccess(System.nanoTime() - start);
                        metrics.getRequests().increment(Attributes.of(AttributeKey.stringKey("operation"), "delete"));
                    }
                });
    }

    private CompletableFuture<Void> deleteBatchAsync(List<String> locations) {
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        // Build the list of ObjectIdentifiers
        List<ObjectIdentifier> objectsToDelete = locations.stream()
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());

        // Create DeleteObjectsRequest
        DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder().objects(objectsToDelete).build())
                .build();

        return s3AsyncClient.deleteObjects(deleteRequest).thenApply(__ -> null);
    }

    @Override
    public void close() throws Exception {
        s3AsyncClient.close();
        nettyHttpClient.close();
    }
}
