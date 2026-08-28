/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;

/**
 * Factory class to manage shared Iceberg catalog instances.
 * This class ensures that catalogs with identical configurations are reused
 * to avoid creating too many catalog instances and optimize resource usage.
 */
@Slf4j
public class CatalogFactory {
    protected static final String DEFAULT_CATALOG_NAME = "default";
    private static final ConcurrentHashMap<CatalogKey, ReferencedCatalog> catalogCache = new ConcurrentHashMap<>();

    /**
     * Configuration key record for catalog identification.
     */
    record CatalogKey(
        Optional<String> catalogName,
        String catalogType,
        IcebergCatalogBackendType catalogBackendType,
        Map<String, String> icebergProperties,
        int hadoopConfigHash
    ) {
        @Override
        public String toString() {
            // Mask sensitive properties
            Map<String, String> maskedProperties = new TreeMap<>();
            if (icebergProperties != null) {
                for (Map.Entry<String, String> entry : icebergProperties.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    // Mask values for keys that might contain sensitive data
                    if (key.toLowerCase(Locale.ROOT).contains("credential")
                            || key.toLowerCase(Locale.ROOT).contains("password")
                            || key.toLowerCase(Locale.ROOT).contains("secret")
                            || key.toLowerCase(Locale.ROOT).contains("key")
                            || key.toLowerCase(Locale.ROOT).contains("token")) {
                        maskedProperties.put(key, "***MASKED***");
                    } else {
                        maskedProperties.put(key, value);
                    }
                }
            }
            return "CatalogKey["
                    + "catalogName=" + catalogName
                    + ", catalogType=" + catalogType
                    + ", catalogBackendType=" + catalogBackendType
                    + ", icebergProperties=" + maskedProperties
                    + ", hadoopConfigHash=" + hadoopConfigHash
                    + "]";
        }
    }

    /**
     * Get or create a catalog instance based on configuration, and atomically retain a
     * reference for the caller. The returned catalog has refCount incremented; the caller
     * MUST call {@link ReferencedCatalog#release()} exactly once when done.
     *
     * <p>Retain happens under the cache lock so no concurrent evictor can observe refCount=0
     * and close the catalog between lookup and retain.
     *
     * @param configuration the lakehouse configuration
     * @return a shared catalog instance, already retained for the caller
     */
    public static ReferencedCatalog getCatalog(LakehouseConfiguration configuration) {
        CatalogKey catalogKey = generateCatalogKey(configuration);
        synchronized (catalogCache) {
            var referencedCatalog = catalogCache.get(catalogKey);
            if (referencedCatalog == null || referencedCatalog.isExpired() || referencedCatalog.isClosed()) {
                if (referencedCatalog != null) {
                    log.info("Catalog instance for key {} is expired: {} or closed: {}, removing from cache",
                            catalogKey, referencedCatalog.isExpired(), referencedCatalog.isClosed());
                    catalogCache.remove(catalogKey);
                    referencedCatalog.safeClose();
                }
                referencedCatalog = buildReferencedCatalog(catalogKey, configuration);
                catalogCache.put(catalogKey, referencedCatalog);
            }
            referencedCatalog.retain();
            return referencedCatalog;
        }
    }

    private static ReferencedCatalog buildReferencedCatalog(CatalogKey catalogKey,
                                                            LakehouseConfiguration configuration) {
        var catalog = CatalogUtil.buildIcebergCatalog(getEffectiveCatalogName(configuration),
            configuration.getIcebergProperties(catalogKey.catalogName),
            configuration.getHadoopConfiguration());

        // Wrap the catalog with retry logic for NotAuthorizedException
        int maxRetries = configuration.getCatalogRetryMaxAttempts();
        long retryDelayMs = configuration.getCatalogRetryDelayMs();
        if (maxRetries > 0) {
            catalog = new RetryableCatalog(catalog, maxRetries, retryDelayMs);
            log.info("Created retryable catalog instance for key: {}, max retries: {}, delay: {}ms",
                    catalogKey, maxRetries, retryDelayMs);
        }

        log.info("Created new catalog instance for key: {}, expire after {} s",
                catalogKey, configuration.getCatalogMaxOpenTime().toSeconds());
        return new ReferencedCatalog(catalog, configuration.getCatalogMaxOpenTime());
    }

    /**
     * Release a caller's reference to {@code referencedCatalog} and, if it was the last
     * reference and the catalog has expired, evict it from the cache and close it. The
     * release and eviction both happen under the cache lock, mirroring the retain performed
     * by {@link #getCatalog(LakehouseConfiguration)}.
     *
     * <p>The specific instance must be passed in (not looked up by key) because the cache
     * slot may have been replaced by a later {@code getCatalog} call after expiration.
     *
     * @param configuration the lakehouse configuration
     * @param referencedCatalog the exact instance previously returned by {@code getCatalog}
     * @throws IllegalStateException if reference count becomes negative (indicates double release)
     */
    public static void releaseCatalog(LakehouseConfiguration configuration, ReferencedCatalog referencedCatalog) {
        CatalogKey catalogKey = generateCatalogKey(configuration);
        synchronized (catalogCache) {
            int refCount = referencedCatalog.release();
            if (refCount == 0 && referencedCatalog.isExpired()) {
                // Only clear the cache slot if it still holds THIS instance — getCatalog
                // may have replaced it with a fresh catalog after expiration.
                catalogCache.remove(catalogKey, referencedCatalog);
                // Close the underlying catalog; otherwise its Closeable resources
                // (HTTP clients, connection pools, threads) leak.
                referencedCatalog.safeClose();
            }
        }
    }

    /**
     * Generate a unique key for catalog configuration.
     * This key is used to identify catalogs with identical configurations.
     *
     * @param configuration the lakehouse configuration
     * @return a unique CatalogKey representing the catalog configuration
     */
    static CatalogKey generateCatalogKey(LakehouseConfiguration configuration) {
        // Create immutable copy of iceberg properties sorted for consistent comparison
        Optional<String> catalogName = configuration.getCatalogName();
        Map<String, String> icebergProps = configuration.getIcebergProperties(catalogName);
        Map<String, String> sortedProps = icebergProps != null && !icebergProps.isEmpty()
            ? new TreeMap<>(icebergProps) : new TreeMap<>();

        // Generate consistent hash for Hadoop configuration based on actual content
        int hadoopConfigHash = generateHadoopConfigHash(configuration.getHadoopConfiguration());

        return new CatalogKey(
            catalogName,
            configuration.getIcebergCatalogType(catalogName),
            configuration.getIcebergCatalogBackendType(catalogName),
            sortedProps,
            hadoopConfigHash
        );
    }

    /**
     * Generate a consistent hash code for Hadoop Configuration based on its content.
     * This ensures that two Configuration objects with the same properties will have
     * the same hash code, regardless of object identity.
     */
    protected static int generateHadoopConfigHash(Configuration hadoopConfig) {
        if (hadoopConfig == null) {
            return 0;
        }

        TreeMap<String, String> configMap = new TreeMap<>();
        for (Map.Entry<String, String> entry : hadoopConfig) {
            if (isRelevantHadoopConfigKey(entry.getKey())) {
                configMap.put(entry.getKey(), entry.getValue());
            }
        }

        return configMap.isEmpty() ? 0 : configMap.hashCode();
    }

    private static boolean isRelevantHadoopConfigKey(String key) {
        String lowerKey = key.toLowerCase(Locale.ROOT);
        return !lowerKey.startsWith("map.")
                && !lowerKey.startsWith("mapred.")
                && !lowerKey.startsWith("mapreduce.")
                && !lowerKey.startsWith("yarn.");
    }

    /**
     * Get the number of cached catalogs.
     *
     * @return the number of currently cached catalog instances
     */
    @VisibleForTesting
    static int getCachedCatalogCount() {
        return catalogCache.size();
    }

    /**
     * Clear all cached catalogs (for testing purposes).
     * This method closes all cached catalogs and clears the cache.
     * Should only be used in test scenarios.
     */
    @VisibleForTesting
    static void clearCache() {
        synchronized (catalogCache) {
            catalogCache.values().forEach(ReferencedCatalog::safeClose);
            catalogCache.clear();
        }
    }

    /**
     * Check if a catalog exists in cache for the given configuration.
     *
     * @param configuration the lakehouse configuration
     * @return true if a catalog exists in cache for this configuration
     */
    @VisibleForTesting
    static boolean hasCachedCatalog(LakehouseConfiguration configuration) {
        CatalogKey catalogKey = generateCatalogKey(configuration);
        return catalogCache.containsKey(catalogKey);
    }

    static String getEffectiveCatalogName(LakehouseConfiguration configuration) {
        return configuration.getCatalogName()
            .filter(name -> !name.isBlank())
            .orElse(DEFAULT_CATALOG_NAME);
    }
}
