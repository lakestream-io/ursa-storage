/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.iceberg.IcebergCatalogBackendType.HADOOP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class CatalogFactoryTest {

    private LakehouseConfiguration mockConfiguration;
    private Catalog mockCatalog;
    private MockedStatic<CatalogUtil> catalogUtilMock;

    @BeforeEach
    void setUp() {
        // Clear cache before each test
        CatalogFactory.clearCache();

        // Setup mocks
        mockConfiguration = mock(LakehouseConfiguration.class);
        mockCatalog = mock(Catalog.class);

        // Setup default configuration behavior
        when(mockConfiguration.getCatalogName()).thenReturn(Optional.of("ursa"));
        when(mockConfiguration.getIcebergCatalogType(any())).thenReturn("hadoop");
        when(mockConfiguration.getIcebergCatalogBackendType(any())).thenReturn(HADOOP);
        when(mockConfiguration.getIcebergProperties(any())).thenReturn(createDefaultIcebergProperties());
        when(mockConfiguration.getHadoopConfiguration()).thenReturn(new Configuration(false));
        when(mockConfiguration.getCatalogMaxOpenTime()).thenReturn(Duration.ofDays(365));
    }

    @AfterEach
    void tearDown() {
        CatalogFactory.clearCache();
        if (catalogUtilMock != null) {
            catalogUtilMock.close();
        }
    }

    void mockCatalogUtil() {
        // Mock CatalogUtil to always return our mock catalog
        catalogUtilMock = mockStatic(CatalogUtil.class);
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(
            eq("ursa"), // Match the exact name used in CatalogFactory
            any(Map.class),
            any(Configuration.class)
        )).thenReturn(mockCatalog);
    }

    private Map<String, String> createDefaultIcebergProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "hadoop"); // Use hadoop instead of hive to avoid classpath issues
        props.put("warehouse", "/tmp/warehouse");
        return props;
    }

    @Test
    void testGetCatalog_CreateNewCatalog() {
        mockCatalogUtil();
        // When
        var result = CatalogFactory.getCatalog(mockConfiguration);

        // Then
        assertNotNull(result);
        assertSame(mockCatalog, result.getCatalog());
        assertEquals(1, CatalogFactory.getCachedCatalogCount());

        // Verify CatalogUtil was called
        catalogUtilMock.verify(() -> CatalogUtil.buildIcebergCatalog(
            eq("ursa"),
            any(Map.class),
            any(Configuration.class)
        ));
    }

    @Test
    void testGetCatalog_ReuseExistingCatalog() {
        mockCatalogUtil();
        // Given - First call creates catalog
        var firstResult = CatalogFactory.getCatalog(mockConfiguration);

        // Reset the mock to avoid verification issues
        catalogUtilMock.reset();
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(
            any(String.class),
            any(Map.class),
            any(Configuration.class)
        )).thenReturn(mockCatalog);

        // When - Second call with same configuration
        var secondResult = CatalogFactory.getCatalog(mockConfiguration);

        // Then
        assertSame(firstResult, secondResult);
        assertEquals(1, CatalogFactory.getCachedCatalogCount());

        // Verify CatalogUtil was not called again (catalog was reused)
        catalogUtilMock.verifyNoInteractions();
    }

    @Test
    void testGetCatalog_DifferentConfigurations() {
        mockCatalogUtil();
        // Given
        LakehouseConfiguration secondConfig = mock(LakehouseConfiguration.class);
        when(secondConfig.getCatalogName()).thenReturn(Optional.of("ursa2"));
        when(secondConfig.getIcebergCatalogType(any())).thenReturn("hadoop");
        when(secondConfig.getIcebergCatalogBackendType(any())).thenReturn(HADOOP);
        when(secondConfig.getIcebergProperties(any())).thenReturn(Map.of("warehouse", "/different/path"));
        when(secondConfig.getHadoopConfiguration()).thenReturn(new Configuration());

        Catalog secondMockCatalog = mock(Catalog.class);
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(
            eq("ursa"),
            any(Map.class),
            any(Configuration.class)
        )).thenReturn(mockCatalog);
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(
            eq("ursa2"),
            any(Map.class),
            any(Configuration.class)
        )).thenReturn(secondMockCatalog);

        // When
        var firstCatalog = CatalogFactory.getCatalog(mockConfiguration);
        var secondCatalog = CatalogFactory.getCatalog(secondConfig);

        // Then
        assertNotSame(firstCatalog, secondCatalog);
        assertEquals(2, CatalogFactory.getCachedCatalogCount());
    }

    @Test
    void testReleaseCatalog_LastReference() throws IOException {
        mockCatalogUtil();
        // Given
        Catalog closeableCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(
            any(String.class),
            any(Map.class),
            any(Configuration.class)
        )).thenReturn(closeableCatalog);

        var catalog = CatalogFactory.getCatalog(mockConfiguration);
        catalog.retain();
        assertTrue(CatalogFactory.hasCachedCatalog(mockConfiguration));

        // When
        CatalogFactory.releaseCatalog(mockConfiguration, catalog);

        // Then
        assertTrue(CatalogFactory.hasCachedCatalog(mockConfiguration));
        assertEquals(1, CatalogFactory.getCachedCatalogCount());
        // not call close due to the catalog not expired
        verify((Closeable) closeableCatalog, never()).close();
    }

    @Test
    void testReleaseCatalog_MultipleReferences() throws IOException {
        mockCatalogUtil();
        // Given
        CatalogFactory.CatalogKey catalogKey = CatalogFactory.generateCatalogKey(mockConfiguration);
        var catalog = CatalogFactory.getCatalog(mockConfiguration);
        catalog.retain();

        // When - Release one reference
        CatalogFactory.releaseCatalog(mockConfiguration, catalog);

        assertTrue(CatalogFactory.hasCachedCatalog(mockConfiguration));
        assertEquals(1, CatalogFactory.getCachedCatalogCount());
    }

    @Test
    void testReleaseCatalog_CloseException() throws IOException {
        mockCatalogUtil();
        // Given
        Catalog closeableCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        doThrow(new IOException("Close failed")).when((Closeable) closeableCatalog).close();

        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any()))
            .thenReturn(closeableCatalog);

        var catalog = CatalogFactory.getCatalog(mockConfiguration);
        catalog.retain();

        // When/Then - Should not throw exception
        assertDoesNotThrow(() -> CatalogFactory.releaseCatalog(mockConfiguration, catalog));

        // Catalog should still be cached (refCount still > 0, not expired)
        assertTrue(CatalogFactory.hasCachedCatalog(mockConfiguration));
    }

    @Test
    void testIncrementRef() {
        mockCatalogUtil();
        // Given - getCatalog() retains once internally; retain() adds a second
        var catalog = CatalogFactory.getCatalog(mockConfiguration);
        catalog.retain();

        // Then
        assertEquals(2, catalog.getRefCount());
    }

    @Test
    void testGenerateCatalogKey() {
        // Given
        Map<String, String> icebergProps = new HashMap<>();
        icebergProps.put("type", "hadoop");
        icebergProps.put("warehouse", "/tmp/warehouse");

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("test.property", "test.value");

        when(mockConfiguration.getIcebergCatalogType(any())).thenReturn("hadoop");
        when(mockConfiguration.getIcebergCatalogBackendType(any())).thenReturn(HADOOP);
        when(mockConfiguration.getIcebergProperties(any())).thenReturn(icebergProps);
        when(mockConfiguration.getHadoopConfiguration()).thenReturn(hadoopConfig);

        // When
        CatalogFactory.CatalogKey key = CatalogFactory.generateCatalogKey(mockConfiguration);

        // Then
        assertNotNull(key);
        assertEquals("hadoop", key.catalogType());
        assertEquals(HADOOP, key.catalogBackendType());
        assertEquals("/tmp/warehouse", key.icebergProperties().get("warehouse"));
        assertTrue(key.hadoopConfigHash() != 0);
    }

    @Test
    void testGenerateCatalogKey_NullProperties() {
        // Given
        when(mockConfiguration.getIcebergCatalogType(any())).thenReturn("hadoop");
        when(mockConfiguration.getIcebergCatalogBackendType(any())).thenReturn(HADOOP);
        when(mockConfiguration.getIcebergProperties(any())).thenReturn(null);
        when(mockConfiguration.getHadoopConfiguration()).thenReturn(null);

        // When
        CatalogFactory.CatalogKey key = CatalogFactory.generateCatalogKey(mockConfiguration);

        // Then
        assertNotNull(key);
        assertEquals("hadoop", key.catalogType());
        assertEquals(HADOOP, key.catalogBackendType());
        assertTrue(key.icebergProperties().isEmpty());
        assertEquals(0, key.hadoopConfigHash());
    }

    @Test
    void testGenerateCatalogKey_Consistency() {
        // Given - Same configuration
        CatalogFactory.CatalogKey key1 = CatalogFactory.generateCatalogKey(mockConfiguration);
        CatalogFactory.CatalogKey key2 = CatalogFactory.generateCatalogKey(mockConfiguration);

        // Then - Keys should be identical
        assertEquals(key1, key2);
        assertEquals("hadoop", key1.catalogType());
        assertEquals(HADOOP, key1.catalogBackendType());
    }

    @Test
    void testGenerateCatalogKey_SortedProperties() {
        // Given
        Map<String, String> props1 = new HashMap<>();
        props1.put("z-prop", "z-value");
        props1.put("a-prop", "a-value");
        props1.put("m-prop", "m-value");

        Map<String, String> props2 = new HashMap<>();
        props2.put("a-prop", "a-value");
        props2.put("m-prop", "m-value");
        props2.put("z-prop", "z-value");

        LakehouseConfiguration config1 = mock(LakehouseConfiguration.class);
        LakehouseConfiguration config2 = mock(LakehouseConfiguration.class);

        when(config1.getCatalogName()).thenReturn(Optional.of("test"));
        when(config1.getIcebergCatalogType(any())).thenReturn("test");
        when(config1.getIcebergCatalogBackendType(any())).thenReturn(HADOOP);
        when(config1.getIcebergProperties(any())).thenReturn(props1);
        when(config1.getHadoopConfiguration()).thenReturn(new Configuration());

        when(config2.getCatalogName()).thenReturn(Optional.of("test"));
        when(config2.getIcebergCatalogType(any())).thenReturn("test");
        when(config2.getIcebergCatalogBackendType(any())).thenReturn(HADOOP);
        when(config2.getIcebergProperties(any())).thenReturn(props2);
        when(config2.getHadoopConfiguration()).thenReturn(new Configuration());

        // When
        CatalogFactory.CatalogKey key1 = CatalogFactory.generateCatalogKey(config1);
        CatalogFactory.CatalogKey key2 = CatalogFactory.generateCatalogKey(config2);

        // Then - Keys should be identical (properties are sorted)
        assertEquals(key1, key2);
    }

    // Concurrency Tests

    @Test
    void testConcurrentGetCatalog() throws InterruptedException {
        mockCatalogUtil();
        // Given
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<ReferencedCatalog> results = new ArrayList<>();
        List<Exception> exceptions = new ArrayList<>();
        var createdCatalog = CatalogFactory.getCatalog(mockConfiguration);
        createdCatalog.retain();

        // When - Multiple threads try to get the same catalog simultaneously
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    var catalog = CatalogFactory.getCatalog(mockConfiguration);
                    synchronized (results) {
                        results.add(catalog);
                    }
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Then
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: " + exceptions);
        assertEquals(threadCount, results.size());
        // All results should be the same catalog instance
        for (ReferencedCatalog result : results) {
            assertSame(createdCatalog, result);
        }
        assertEquals(1, CatalogFactory.getCachedCatalogCount());
    }

    @Test
    void testConcurrentMultipleCatalogs() throws InterruptedException {
        // Given
        int configCount = 5;
        int threadsPerConfig = 4;
        ExecutorService executor = Executors.newFixedThreadPool(configCount * threadsPerConfig);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(configCount * threadsPerConfig);
        List<Exception> exceptions = new ArrayList<>();
        Map<Integer, List<ReferencedCatalog>> resultsByConfig = new ConcurrentHashMap<>();

        LakehouseConfiguration[] configs = new LakehouseConfiguration[configCount];
        ReferencedCatalog[] catalogs = new ReferencedCatalog[configCount];

        // Setup different configurations and expected catalogs
        for (int i = 0; i < configCount; i++) {
            configs[i] = mock(LakehouseConfiguration.class);
            resultsByConfig.put(i, new ArrayList<>());

            when(configs[i].getCatalogName()).thenReturn(Optional.of("catalog" + i));
            when(configs[i].getIcebergCatalogType(any())).thenReturn("hadoop");
            when(configs[i].getIcebergCatalogBackendType(any())).thenReturn(HADOOP);
            when(configs[i].getCatalogMaxOpenTime()).thenReturn(Duration.ofDays(365));

            // Create different properties for each config to ensure different cache keys
            Map<String, String> props = new HashMap<>();
            props.put("type", "hadoop");
            props.put("warehouse", "/tmp/warehouse" + i); // Different warehouse paths
            props.put("unique.prop", "value" + i); // Add unique property
            when(configs[i].getIcebergProperties(any())).thenReturn(props);
            when(configs[i].getHadoopConfiguration()).thenReturn(new Configuration());
            var c = CatalogFactory.getCatalog(configs[i]);

            catalogs[i] = CatalogFactory.getCatalog(configs[i]);
        }

        // When - Multiple threads access different catalogs concurrently
        for (int configIndex = 0; configIndex < configCount; configIndex++) {
            final int finalConfigIndex = configIndex;
            for (int threadIndex = 0; threadIndex < threadsPerConfig; threadIndex++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        var catalog = CatalogFactory.getCatalog(configs[finalConfigIndex]);
                        synchronized (resultsByConfig.get(finalConfigIndex)) {
                            resultsByConfig.get(finalConfigIndex).add(catalog);
                        }
                    } catch (Exception e) {
                        synchronized (exceptions) {
                            exceptions.add(e);
                        }
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Then
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: " + exceptions);
        assertEquals(configCount, CatalogFactory.getCachedCatalogCount());

        // Verify each configuration got the correct catalog
        for (int i = 0; i < configCount; i++) {
            List<ReferencedCatalog> results = resultsByConfig.get(i);
            assertEquals(threadsPerConfig, results.size());
            for (ReferencedCatalog result : results) {
                assertSame(catalogs[i], result,
                    "Config " + i + " should return catalog " + i + " but got different instance, is expired: " + result.isExpired());
            }
        }
    }

    @Test
    void testConcurrentGetAndRelease() throws InterruptedException {
        mockCatalogUtil();
        // Given
        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch getsDoneLatch = new CountDownLatch(threadCount);
        CountDownLatch releasesDoneLatch = new CountDownLatch(threadCount);
        List<Exception> exceptions = new ArrayList<>();

        Catalog closeableCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any()))
            .thenReturn(closeableCatalog);

        List<ReferencedCatalog> retained = new ArrayList<>();

        // When - First all get operations, then all release operations
        for (int i = 0; i < threadCount; i++) {
            // Get catalog and increment ref
            executor.submit(() -> {
                try {
                    startLatch.await();
                    var c = CatalogFactory.getCatalog(mockConfiguration);
                    c.retain();
                    synchronized (retained) {
                        retained.add(c);
                    }
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                } finally {
                    getsDoneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(getsDoneLatch.await(5, TimeUnit.SECONDS));

        // Now do release operations
        for (int i = 0; i < threadCount; i++) {
            final ReferencedCatalog toRelease = retained.get(i);
            executor.submit(() -> {
                try {
                    CatalogFactory.releaseCatalog(mockConfiguration, toRelease);
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                } finally {
                    releasesDoneLatch.countDown();
                }
            });
        }

        assertTrue(releasesDoneLatch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        // Then - No exceptions should occur
        assertTrue(exceptions.isEmpty(), "Exceptions occurred: " + exceptions);
    }


    @Test
    void testGenerateHadoopConfigHash() throws Exception {
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.defaultFS", "hdfs://localhost:9000");
        hadoopConfig.set("fs.xx", "abc");

        Configuration hadoopConfig2 = new Configuration();
        hadoopConfig2.set("fs.defaultFS", "hdfs://localhost:9000");
        hadoopConfig2.set("fs.xx", "abc");
        int code1 = CatalogFactory.generateHadoopConfigHash(hadoopConfig);
        int code2 = CatalogFactory.generateHadoopConfigHash(hadoopConfig2);
        assertEquals(code1, code2);
    }

    @Test
    void testGenerateHadoopConfigHash_IgnoresMapReduceAndYarnDefaults() {
        Configuration hadoopConfig = new Configuration(false);
        hadoopConfig.set("fs.defaultFS", "hdfs://localhost:9000");
        hadoopConfig.set("fs.s3a.endpoint.region", "us-east-2");

        Configuration hadoopConfigWithLaterLoadedDefaults = new Configuration(false);
        hadoopConfigWithLaterLoadedDefaults.set("fs.defaultFS", "hdfs://localhost:9000");
        hadoopConfigWithLaterLoadedDefaults.set("fs.s3a.endpoint.region", "us-east-2");
        hadoopConfigWithLaterLoadedDefaults.set("map.sort.class", "org.apache.hadoop.util.QuickSort");
        hadoopConfigWithLaterLoadedDefaults.set("mapreduce.framework.name", "local");
        hadoopConfigWithLaterLoadedDefaults.set("mapred.job.tracker", "local");
        hadoopConfigWithLaterLoadedDefaults.set("yarn.resourcemanager.hostname", "0.0.0.0");

        int code1 = CatalogFactory.generateHadoopConfigHash(hadoopConfig);
        int code2 = CatalogFactory.generateHadoopConfigHash(hadoopConfigWithLaterLoadedDefaults);
        assertEquals(code1, code2);
    }

    @Test
    void testRefreshCatalogForPolarisWhenIntervalExceeded() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("compactionBackendStorageType", "AzureLocal");
        properties.put("compactionBucket", "test@test");
        properties.put("catalogMaxOpenTimeInSeconds", "1"); // 1 second for quick testing
        properties.put("iceberg.catalog.polaris.type", "polaris");
        properties.put("iceberg.catalog.polaris.warehouse", "/tmp/polaris-warehouse");
        properties.put("iceberg.catalog.polaris.catalog-backend", "polaris");
        properties.put("catalog.name", "polaris");

        LakehouseConfiguration polarisConfig = new LakehouseConfiguration(properties);

        try (
            // Mock the CatalogUtil to return a new catalog instance
            MockedStatic<CatalogUtil> catalogUtilMock = mockStatic(CatalogUtil.class)
        ) {
            var firstCatalog = mock(RESTCatalog.class);
            var secondCatalog = mock(RESTCatalog.class);
            catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(
                eq("polaris"),
                any(Map.class),
                any(Configuration.class)
            )).thenReturn(firstCatalog).thenReturn(secondCatalog);

            // Get initial catalog
            ReferencedCatalog initialCatalog = CatalogFactory.getCatalog(polarisConfig);
            assertNotNull(initialCatalog);
            assertEquals(1, CatalogFactory.getCachedCatalogCount());

            // Wait for refresh interval to pass (make catalog expired)
            Thread.sleep(1100); // Wait slightly longer than 1 second

            // Get catalog again - should trigger refresh because catalog is expired
            ReferencedCatalog refreshedCatalog = CatalogFactory.getCatalog(polarisConfig);

            // Verify new catalog was created and cached
            assertNotNull(refreshedCatalog);
            assertNotSame(initialCatalog, refreshedCatalog);
            assertEquals(1, CatalogFactory.getCachedCatalogCount());
        }

    }

    @Test
    void testRefreshCatalogForPolarisWhenIntervalNotExceeded() {
        Properties properties = new Properties();
        properties.put("compactionBackendStorageType", "AzureLocal");
        properties.put("compactionBucket", "test@test");
        properties.put("catalogMaxOpenTimeInSeconds", "1"); // 1 second for quick testing
        properties.put("iceberg.catalog.polaris.type", "polaris");
        properties.put("iceberg.catalog.polaris.warehouse", "/tmp/polaris-warehouse");
        properties.put("iceberg.catalog.polaris.catalog-backend", "polaris");
        properties.put("catalog.name", "polaris");

        LakehouseConfiguration polarisConfig = new LakehouseConfiguration(properties);

        try (
            // Mock the CatalogUtil to return a new catalog instance
            MockedStatic<CatalogUtil> catalogUtilMock = mockStatic(CatalogUtil.class)
        ) {
            var firstCatalog = mock(RESTCatalog.class);
            var secondCatalog = mock(RESTCatalog.class);
            catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(
                eq("polaris"),
                any(Map.class),
                any(Configuration.class)
            )).thenReturn(firstCatalog).thenReturn(secondCatalog);

            // Get catalog twice in quick succession
            ReferencedCatalog firstGetCatalog = CatalogFactory.getCatalog(polarisConfig);
            ReferencedCatalog secondGetCatalog = CatalogFactory.getCatalog(polarisConfig);

            // Should return same catalog instance (no refresh because not expired)
            assertSame(firstGetCatalog, secondGetCatalog);
            assertEquals(1, CatalogFactory.getCachedCatalogCount());
        }

    }

    @Test
    void testCatalogKeyToStringMasksSensitiveProperties() {
        // Create test properties with sensitive data
        Map<String, String> icebergProps = new HashMap<>();
        icebergProps.put("uri", "https://example.com/catalog");
        icebergProps.put("credential", "PQY6nHBSfOlQwmzrrFYQBIF6d7k=:LIQC/dcY2bodYfTRKA0whhxDWPuGg3lIGDQFtaxCEvw=");
        icebergProps.put("password", "mysecretpassword");
        icebergProps.put("secret", "verysecretvalue");
        icebergProps.put("api-key", "12345-abcde-67890");
        icebergProps.put("token", "bearer-token-value");
        icebergProps.put("warehouse", "test-warehouse");

        // Create CatalogKey
        CatalogFactory.CatalogKey catalogKey = new CatalogFactory.CatalogKey(
                Optional.of("test-catalog"),
                "rest",
                HADOOP,
                icebergProps,
                12345
        );

        // Test toString output
        String toStringOutput = catalogKey.toString();
        // Verify non-sensitive properties are not masked
        assertTrue(toStringOutput.contains("uri=https://example.com/catalog"));
        assertTrue(toStringOutput.contains("warehouse=test-warehouse"));
        assertTrue(toStringOutput.contains("catalogName=Optional[test-catalog]"));
        assertTrue(toStringOutput.contains("catalogType=rest"));
        // Verify sensitive properties are masked
        assertTrue(toStringOutput.contains("credential=***MASKED***"));
        assertTrue(toStringOutput.contains("password=***MASKED***"));
        assertTrue(toStringOutput.contains("secret=***MASKED***"));
        assertTrue(toStringOutput.contains("api-key=***MASKED***"));
        assertTrue(toStringOutput.contains("token=***MASKED***"));
        // Verify actual sensitive values are not in the output
        assertFalse(toStringOutput.contains("PQY6nHBSfOlQwmzrrFYQBIF6d7k"));
        assertFalse(toStringOutput.contains("mysecretpassword"));
        assertFalse(toStringOutput.contains("verysecretvalue"));
        assertFalse(toStringOutput.contains("12345-abcde-67890"));
        assertFalse(toStringOutput.contains("bearer-token-value"));
    }

    @Test
    void testGetCatalog_ReplacesClosedCatalog() throws IOException {
        mockCatalogUtil();
        // Given - Create a catalog that will be closed
        Catalog firstCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        when(firstCatalog.name()).thenReturn("test-catalog");
        Catalog secondCatalog = mock(Catalog.class);
        when(secondCatalog.name()).thenReturn("test-catalog-new");
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any()))
            .thenReturn(firstCatalog)
            .thenReturn(secondCatalog);
        // First get catalog
        var refCatalog1 = CatalogFactory.getCatalog(mockConfiguration);
        refCatalog1.retain();
        assertEquals(1, CatalogFactory.getCachedCatalogCount());
        // Close the catalog directly
        refCatalog1.close();
        assertTrue(refCatalog1.isClosed());
        // Get catalog again - should create new one since previous is closed
        var refCatalog2 = CatalogFactory.getCatalog(mockConfiguration);
        assertNotSame(refCatalog1, refCatalog2);
        assertFalse(refCatalog2.isClosed());
        assertEquals(1, CatalogFactory.getCachedCatalogCount());
        // Verify first catalog was closed
        verify((Closeable) firstCatalog).close();
    }

    @Test
    void testGetCatalog_HandlesExpiredAndClosedCatalogGracefully() throws InterruptedException, IOException {
        mockCatalogUtil();
        // Create a short-lived catalog configuration
        when(mockConfiguration.getCatalogMaxOpenTime()).thenReturn(Duration.ofMillis(50));

        // Create closeable catalogs
        Catalog firstCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        when(firstCatalog.name()).thenReturn("expired-catalog");
        Catalog secondCatalog = mock(Catalog.class);
        when(secondCatalog.name()).thenReturn("new-catalog");

        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any()))
            .thenReturn(firstCatalog)
            .thenReturn(secondCatalog);

        // Get first catalog (getCatalog retains internally → refCount=1)
        var refCatalog1 = CatalogFactory.getCatalog(mockConfiguration);
        assertEquals(1, CatalogFactory.getCachedCatalogCount());

        // Wait for expiration
        Thread.sleep(100);
        assertTrue(refCatalog1.isExpired());

        // Release our hold so the eviction path in the next getCatalog can close it.
        refCatalog1.release();
        verify((Closeable) firstCatalog).close();

        // Get catalog again - should replace expired catalog
        var refCatalog2 = CatalogFactory.getCatalog(mockConfiguration);
        assertNotSame(refCatalog1, refCatalog2);
        assertEquals(1, CatalogFactory.getCachedCatalogCount());
    }

    @Test
    void testSafeCloseLogging() throws IOException {
        mockCatalogUtil();
        // Create a closeable catalog
        Catalog closeableCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
        when(closeableCatalog.name()).thenReturn("test-catalog");
        catalogUtilMock.when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any()))
            .thenReturn(closeableCatalog);

        // getCatalog retains once internally; retain three more times → refCount=4
        var refCatalog = CatalogFactory.getCatalog(mockConfiguration);
        refCatalog.retain();
        refCatalog.retain();
        refCatalog.retain();

        // Try to safe close - should not close due to positive ref count
        refCatalog.safeClose();
        // Verify catalog was not closed
        verify((Closeable) closeableCatalog, never()).close();
        assertEquals(4, refCatalog.getRefCount());
        assertFalse(refCatalog.isClosed());

        // Release all four references
        refCatalog.release();
        refCatalog.release();
        refCatalog.release();
        refCatalog.release();
        assertEquals(0, refCatalog.getRefCount());

        // Now safe close should work
        refCatalog.safeClose();
        verify((Closeable) closeableCatalog).close();
        assertTrue(refCatalog.isClosed());
    }

    @Test
    void testGetDefaultCatalogName() {
        // Test with empty optional
        LakehouseConfiguration config = mock(LakehouseConfiguration.class);
        when(config.getCatalogName()).thenReturn(Optional.empty());
        assertEquals("default", CatalogFactory.getEffectiveCatalogName(config));

        // Test with blank string
        when(config.getCatalogName()).thenReturn(Optional.of("   "));
        assertEquals("default", CatalogFactory.getEffectiveCatalogName(config));

        // Test with valid name
        when(config.getCatalogName()).thenReturn(Optional.of("my-catalog"));
        assertEquals("my-catalog", CatalogFactory.getEffectiveCatalogName(config));
    }
}
