/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.exception.ManagementException;
import com.azure.core.management.profile.AzureProfile;
import com.azure.core.util.BinaryData;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.storage.StorageManager;
import com.azure.resourcemanager.storage.models.BlobTypes;
import com.azure.resourcemanager.storage.models.DateAfterModification;
import com.azure.resourcemanager.storage.models.ManagementPolicies;
import com.azure.resourcemanager.storage.models.ManagementPolicy;
import com.azure.resourcemanager.storage.models.ManagementPolicyAction;
import com.azure.resourcemanager.storage.models.ManagementPolicyBaseBlob;
import com.azure.resourcemanager.storage.models.ManagementPolicyDefinition;
import com.azure.resourcemanager.storage.models.ManagementPolicyFilter;
import com.azure.resourcemanager.storage.models.ManagementPolicyName;
import com.azure.resourcemanager.storage.models.ManagementPolicyRule;
import com.azure.resourcemanager.storage.models.ManagementPolicySchema;
import com.azure.resourcemanager.storage.models.RuleType;
import com.azure.resourcemanager.storage.models.StorageAccount;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.util.concurrent.RateLimiter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.FileStorageMetrics;
import io.lakestream.ursa.storage.IDGeneratorWithDate;
import io.lakestream.ursa.storage.impl.exception.FileStorageException;
import io.lakestream.ursa.storage.impl.exception.RetryableException;
import io.lakestream.ursa.utils.FutureUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.netty.internal.shaded.reactor.pool.PoolAcquirePendingLimitException;
import reactor.netty.internal.shaded.reactor.pool.PoolAcquireTimeoutException;
import reactor.netty.resources.ConnectionProvider;

@Slf4j
public class AzureFileStorage implements FileStorage {

    private final String resourceGroupName;
    private final String accountName;
    private final String blobContainer;
    @Getter
    private final String bucketPrefix;
    private final StorageManager storageManager;
    private BlobContainerAsyncClient storage;
    private final FileStorageMetrics metrics;
    private static final ManagementPolicyName policyName = ManagementPolicyName.fromString("ursa-retention");

    private RateLimiter rateLimiter = null;

    // used for the mocked test
    AzureFileStorage(StorageManager storageManager) {
        this.resourceGroupName = "DEFAULT";
        this.accountName = "ACCOUNT";
        this.blobContainer = "CONTAINER";
        this.bucketPrefix = "prefix";
        this.storageManager = storageManager;
        this.metrics = new FileStorageMetrics(InstrumentProvider.NOOP, Type.AZUREBLOB.name());
    }

    public AzureFileStorage(StorageConfig config, InstrumentProvider provider) {
        var bucketInfo = config.getBucket().trim().split("@");
        if (bucketInfo.length != 2) {
            throw new IllegalArgumentException("Invalid azure bucket format: " + config.getBucket()
                + " should be <account-name>@<container>");
        }
        this.accountName = bucketInfo[0];
        this.blobContainer = bucketInfo[1];
        this.bucketPrefix = config.getPrefix();
        this.storage = buildStorageClient(config).getBlobContainerAsyncClient(blobContainer);
        this.metrics = new FileStorageMetrics(provider, Type.AZUREBLOB.name());
        this.storageManager = buildStorageManager(config);
        this.resourceGroupName = getResourceGroupName();

        if (config.getCloudStorageOpsRateLimitPerSecond() > 0) {
            this.rateLimiter = RateLimiter.create(config.getCloudStorageOpsRateLimitPerSecond());
        }
    }


    private StorageManager buildStorageManager(StorageConfig config) {
        var disableCredential = Boolean.parseBoolean(config.getProperties().getProperty("disableCredential"));
        if (disableCredential) {
            return null;
        } else {
            try {
                AzureResourceManager arm = AzureResourceManager.authenticate(
                    new WorkloadIdentityCredentialBuilder().build(),
                    new AzureProfile(AzureEnvironment.AZURE)).withDefaultSubscription();
                AzureProfile profile = new AzureProfile(arm.tenantId(), arm.subscriptionId(), AzureEnvironment.AZURE);
                return StorageManager.authenticate(new DefaultAzureCredentialBuilder().build(), profile);
            } catch (Exception e) {
                log.warn("Failed to authenticate the azure storage manager with the default credential, "
                    + "will try to authenticate with the environment variables", e);
            }
        }
        return null;
    }

    private BlobServiceAsyncClient buildStorageClient(StorageConfig config) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
        builder.httpClient(bulidHttpClient(config));

        var disableCredential = Boolean.parseBoolean(config.getProperties().getProperty("disableCredential"));
        if (!disableCredential) {
            builder.credential(new DefaultAzureCredentialBuilder().build());
        }

        var connectionString = config.getProperties().getProperty("connectionString");
        if (StringUtils.isNotEmpty(connectionString)) {
            builder.connectionString(connectionString);
        } else {
            var endpoint = String.format("https://%s.blob.core.windows.net", accountName);
            builder.endpoint(endpoint);
            if (StringUtils.isNotEmpty(config.getCloudStorageEndpoint())) {
                builder.endpoint(config.getCloudStorageEndpoint());
            }
        }

        return builder.buildAsyncClient();
    }

    private HttpClient bulidHttpClient(StorageConfig config) {
        var builder = ConnectionProvider.builder("ursa-azure-connection-pool")
            .maxConnections(config.getCloudStorageMaxConcurrencyRequest());

        if (config.getCloudStorageMaxPendingConnectionAcquires() > 0) {
            builder.pendingAcquireMaxCount(config.getCloudStorageMaxPendingConnectionAcquires());
        }

        if (config.getCloudStorageMaxPendingAcquireTimeoutInMs() > 0) {
            builder.pendingAcquireTimeout(
                Duration.ofMillis(config.getCloudStorageMaxPendingAcquireTimeoutInMs()));
        }

        return new NettyAsyncHttpClientBuilder()
            .connectionProvider(builder.build())
            .build();
    }

    private String getResourceGroupName() {
        if (storageManager == null) {
            return "DEFAULT";
        }
        var storageAccounts = storageManager.storageAccounts().list().stream().toList();
        for (StorageAccount storageAccount : storageAccounts) {
            log.info("Found the storage account {} in the resource group {}", storageAccount.name(),
                storageAccount.resourceGroupName());
            if (storageAccount.name().equalsIgnoreCase(accountName)) {
                return storageAccount.resourceGroupName();
            }
        }
        return "DEFAULT";
    }

    @Override
    public void put(ByteBuf data, String location) throws IOException {
        try {
            putAsync(data, location).get();
        } catch (Exception e) {
            final String errorMsg = "Failed to put object to the Azure bucket "
                + blobContainer + " with key " + location;
            if (e instanceof ExecutionException) {
                throw new IOException(errorMsg, e.getCause());
            } else {
                throw new IOException(errorMsg, e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> putAsync(ByteBuf data, String location) {
        long start = System.nanoTime();
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        return storage.getBlobAsyncClient(getLocation(location))
            .upload(BinaryData.fromByteBuffer(data.nioBuffer())).toFuture()
            .handle((__, t) -> {
                if (t != null) {
                    metrics.getWriteStorageLatency().recordFailure(System.nanoTime() - start);
                    handleRetriableException(t);
                    return null;
                } else {
                    metrics.getWriteBytesCount().add(data.readableBytes());
                    metrics.getWriteStorageLatency().recordSuccess(System.nanoTime() - start);
                    return null;
                }
            });
    }

    @Override
    public ByteBuf get(String location) throws IOException {
        try {
            return getAsync(location).get();
        } catch (Exception e) {
            final String errorMsg = "Failed to get object from the Azure bucket "
                + blobContainer + " with key " + location;
            if (e instanceof ExecutionException) {
                throw new IOException(errorMsg, e.getCause());
            } else {
                throw new IOException(errorMsg, e);
            }
        }
    }

    @Override
    public CompletableFuture<ByteBuf> getAsync(String location) {
        long start = System.nanoTime();
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }

        return storage.getBlobAsyncClient(getLocation(location))
            .downloadContent().toFuture()
            .thenApply(BinaryData::toByteBuffer)
            .thenApply(byteBuffer -> Unpooled.directBuffer(byteBuffer.remaining()).writeBytes(byteBuffer))
            .handle((data, t) -> {
                if (t != null) {
                    metrics.getReadStorageLatency().recordFailure(System.nanoTime() - start);
                    handleRetriableException(t);
                    return null;
                } else {
                    metrics.getReadBytesCount().add(data.readableBytes());
                    metrics.getReadStorageLatency().recordSuccess(System.nanoTime() - start);
                    return data;
                }
            });
    }

    private void handleRetriableException(Throwable throwable) {
        Throwable t = unwrapException(throwable);
        if (t instanceof PoolAcquireTimeoutException || t instanceof PoolAcquirePendingLimitException) {
            throw new CompletionException(new RetryableException(t));
        }
        throw new CompletionException(t);
    }

    private Throwable unwrapException(Throwable throwable) {
        if (throwable instanceof CompletionException) {
            return unwrapException(throwable.getCause());
        }
        return throwable;
    }

    @Override
    public void delete(String location) throws IOException {
        throw new UnsupportedOperationException("AzureFileStorage does not support delete");
    }

    @Override
    public CompletableFuture<Void> deleteAsync(List<String> locations) {
        long start = System.nanoTime();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String location : locations) {
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            CompletableFuture<Void> future = storage.getBlobAsyncClient(getLocation(location))
                    .delete()
                    .toFuture()
                    .exceptionally(t -> {
                        if (isBlobNotFound(t)) {
                            return null;
                        }
                        throw new CompletionException(t);
                    });

            futures.add(future);
        }
        return FutureUtils.waitForAll(futures)
                .handle((__, t) -> {
                    if (t != null) {
                        metrics.getDeleteStorageLatency().recordFailure(System.nanoTime() - start);
                        handleRetriableException(t);
                        throw new CompletionException(t);
                    } else {
                        metrics.getDeleteStorageLatency().recordSuccess(System.nanoTime() - start);
                        metrics.getRequests().increment(Attributes.of(AttributeKey.stringKey("operation"), "delete"));
                        return null;
                    }
                });
    }

    private boolean isBlobNotFound(Throwable throwable) {
        Throwable t = unwrapException(throwable);
        return t instanceof BlobStorageException exception && exception.getStatusCode() == 404;
    }

    /**
     * https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview#known-issues-and-limitations.
     *
     * @param prefixes The prefixes of the data to delete.
     * @return
     * @throws IOException
     */
    @Override
    public CompletableFuture<Void> deleteWithDatePrefixes(Set<String> prefixes) throws IOException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (storageManager == null) {
            future.completeExceptionally(new FileStorageException("Storage manager is not initialized"));
            return future;
        }

        log.info("Delete the prefixes {} in the azure storage account {}, resource group {}", prefixes,
            accountName, resourceGroupName);
        var mp = storageManager.managementPolicies();
        return mp.getAsync(resourceGroupName, accountName).toFuture()
            .handle((policy, e) -> {
                if (e instanceof ManagementException me) {
                    if (me.getResponse().getStatusCode() == 404) {
                        log.info("The management policy {} is not found in the storage account {},"
                            + " will create a new one", policyName, accountName);
                        return null;
                    }
                    throw new CompletionException(e);
                }
                return policy;
            })
            .thenCompose(policy -> {
                if (policy == null) {
                    return createRules(mp, prefixes);
                } else {
                    return updateRules(policy, prefixes);
                }
            }).thenApply(__ -> null);
    }

    CompletableFuture<ManagementPolicy> createRules(ManagementPolicies mp, Set<String> prefixes) {
        if (prefixes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // add the bucket prefix for each prefix
        prefixes = prefixes.stream().map(prefix -> this.bucketPrefix + "/" + prefix).collect(Collectors.toSet());

        if (prefixes.size() < 10) {
            var ruleName = LIFECYCLE_RULE_ID_PREFIX + UUID.randomUUID();
            return mp.define("ursa-retention")
                .withExistingStorageAccount(resourceGroupName, accountName)
                .defineRule(ruleName)
                .withLifecycleRuleType()
                .withBlobTypeToFilterFor(BlobTypes.BLOCK_BLOB)
                .withPrefixesToFilterFor(prefixes.stream().toList())
                .withDeleteActionOnBaseBlob(0f)
                .attach().createAsync().toFuture();
        } else {
            List<String> prefixesList = new ArrayList<>(prefixes);
            prefixesList.sort(String::compareTo);
            var idx = 0;
            var gap = 10;
            ManagementPolicy.DefinitionStages.WithCreate ruleDefine = null;
            while (idx < prefixesList.size()) {
                var subList = prefixesList.subList(idx, Math.min(idx + gap, prefixesList.size()));
                var ruleName = LIFECYCLE_RULE_ID_PREFIX + UUID.randomUUID();
                if (ruleDefine == null) {
                    ruleDefine = mp.define("ursa-retention")
                        .withExistingStorageAccount(resourceGroupName, accountName)
                        .defineRule(ruleName)
                        .withLifecycleRuleType()
                        .withBlobTypeToFilterFor(BlobTypes.BLOCK_BLOB)
                        .withPrefixesToFilterFor(subList)
                        .withDeleteActionOnBaseBlob(0f)
                        .attach();
                } else {
                    ruleDefine.defineRule(ruleName)
                        .withLifecycleRuleType()
                        .withBlobTypeToFilterFor(BlobTypes.BLOCK_BLOB)
                        .withPrefixesToFilterFor(subList)
                        .withDeleteActionOnBaseBlob(0f)
                        .attach();
                }
                idx += gap;
            }
            return ruleDefine.createAsync().toFuture();
        }
    }

    // update rules includes three parts:
    // 1. filter out the existing rules
    // 2. expired the rules that are over than 7 days
    // 3. add the new prefixes
    CompletableFuture<ManagementPolicy> updateRules(ManagementPolicy managementPolicy, Set<String> prefixes) {
        var schema = updatePolicySchemaByPrefixes(managementPolicy, prefixes);
        return managementPolicy.update().withPolicy(schema).applyAsync().toFuture();
    }

    ManagementPolicySchema updatePolicySchemaByPrefixes(ManagementPolicy managementPolicy, Set<String> prefixes) {
        // split the lifecycle rules into two parts:
        // 1. ursa related rules
        // 2. other rules
        List<ManagementPolicyRule> otherRules = new ArrayList<>();
        Set<String> ursaExistingPrefixes = new HashSet<>();
        var policySchema = managementPolicy.policy();

        for (ManagementPolicyRule rule : policySchema.rules()) {
            if (rule.name().startsWith(LIFECYCLE_RULE_ID_PREFIX)) {
                ursaExistingPrefixes.addAll(rule.definition().filters().prefixMatch());
            } else {
                otherRules.add(rule);
            }
        }

        // merge new prefixes and existing prefixes
        Set<String> finalPrefixes = new HashSet<>();
        finalPrefixes.addAll(prefixes);
        finalPrefixes.addAll(ursaExistingPrefixes);

        // join all rules into one set
        otherRules.addAll(getUrsaRules(finalPrefixes));

        return policySchema.withRules(otherRules);
    }

    List<ManagementPolicyRule> getUrsaRules(Set<String> prefixes) {
        // delete the prefixes that are over than 7 days
        List<String> prefixesList = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        for (String prefix : prefixes) {
            var p = prefix;
            if (p.startsWith(bucketPrefix)) {
                p = p.substring(bucketPrefix.length() + 1);
            }
            if (!IDGeneratorWithDate.isDatePrefixOverThan(p, Duration.ofDays(7), now)) {
                prefixesList.add(bucketPrefix + "/" + p);
            }
        }

        // build the new rules
        List<ManagementPolicyRule> ursaRules = new ArrayList<>();
        var idx = 0;
        var gap = 10;
        prefixesList.sort(String::compareTo);
        while (idx < prefixesList.size()) {
            var subList = prefixesList.subList(idx, Math.min(idx + gap, prefixesList.size()));
            var ruleName = LIFECYCLE_RULE_ID_PREFIX + UUID.randomUUID();
            ManagementPolicyRule rule = new ManagementPolicyRule();
            rule.withName(ruleName)
                .withEnabled(true)
                .withType(RuleType.LIFECYCLE)
                .withDefinition(new ManagementPolicyDefinition()
                    .withActions(new ManagementPolicyAction()
                        .withBaseBlob(new ManagementPolicyBaseBlob()
                            .withDelete(new DateAfterModification()
                                .withDaysAfterModificationGreaterThan(0f))))
                    .withFilters(new ManagementPolicyFilter()
                        .withBlobTypes(List.of(BlobTypes.BLOCK_BLOB.toString()))
                        .withPrefixMatch(subList)));
            ursaRules.add(rule);
            idx += gap;
        }

        return ursaRules;
    }

    private String getLocation(String location) {
        return StringUtils.isEmpty(bucketPrefix) ? location : bucketPrefix + "/" + location;
    }

    @Override
    public void close() throws Exception {
    }
}
