/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.IDGeneratorWithDate;
import io.lakestream.ursa.storage.impl.exception.FileStorageException;
import io.lakestream.ursa.utils.FutureUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;



@Slf4j
@Testcontainers
public class TestGCSFileStorage extends MetricsTestBase {

    @Container
    static final GenericContainer<?> FAKE_GCS = new GenericContainer<>("fsouza/fake-gcs-server")
        .withExposedPorts(4443)
        .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
            "/bin/fake-gcs-server",
            "-scheme", "http"
        ));

    private static GCSFileStorage fileStorage;
    private static Storage storageClient;
    private static StorageConfig config = new StorageConfig();

    @BeforeAll
    static void setup() throws Exception {
        String fakeGcsExternalUrl = "http://" + FAKE_GCS.getHost() + ":" + FAKE_GCS.getFirstMappedPort();
        updateExternalUrlWithContainerUrl(fakeGcsExternalUrl);

        config.setBucket("test-gcs");
        config.setPrefix("google-cloud");
        config.setCloudStorageEndpoint(fakeGcsExternalUrl);
        config.setWriteBufferSegment(2);
        Properties properties = new Properties();
        properties.put("disableCredential", "true");
        config.setProperties(properties);

        fileStorage = new GCSFileStorage(config, InstrumentProvider.NOOP);

        storageClient = StorageOptions.newBuilder()
            .setHost(fakeGcsExternalUrl)
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();

        // create bucket
        storageClient.create(BucketInfo.newBuilder(config.getBucket()).build());
    }

    private static void updateExternalUrlWithContainerUrl(String fakeGcsExternalUrl) throws Exception {
        String modifyExternalUrlRequestUri = fakeGcsExternalUrl + "/_internal/config";
        String updateExternalUrlJson = "{"
            + "\"externalUrl\": \"" + fakeGcsExternalUrl + "\""
            + "}";

        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(modifyExternalUrlRequestUri))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(updateExternalUrlJson))
            .build();
        HttpResponse<Void> response = HttpClient.newBuilder().build()
            .send(req, HttpResponse.BodyHandlers.discarding());

        if (response.statusCode() != 200) {
            throw new RuntimeException(
                "error updating fake-gcs-server with external url, response status code " + response.statusCode() + " != 200");
        }
    }

    @Test
    public void testReadAndWrite() throws Exception {
        String location = "test-dev";
        // write
        int count = 10;
        for (int i = 0; i < count; ++i) {
            ByteBuf buf = Unpooled.buffer(1024);
            buf.writeBytes(("test-" + i).getBytes());
            try {
                fileStorage.putAsync(buf, location + "-" + i).get();
            } catch (Exception e) {
                log.error("Failed to write data to S3", e);
                fail();
            } finally {
                buf.release();
            }
        }

        for (int i = 0; i < count; ++i) {
            ByteBuf readBuf = null;
            try {
                readBuf = fileStorage.getAsync(location + "-" + i).get();
                assertNotNull(readBuf);
                assertEquals("test-" + i, readBuf.toString(StandardCharsets.UTF_8));
            } catch (Exception e) {
                log.error("Failed to read data from S3", e);
                fail();
            } finally {
                if (readBuf != null) {
                    readBuf.release();
                }
            }
        }
    }

    @Test
    void getNonExistsFiles() {
        try {
            fileStorage.getAsync("non-exists").get();
            fail();
        } catch (Exception e) {
            assertThat(e.getCause()).isInstanceOf(FileStorageException.class);
        }
    }

    @Test
    public void testMetrics() throws Exception {
        var storage = new GCSFileStorage(config, getInstrumentProvider());
        String location = "test-metrics";
        ByteBuf buf = Unpooled.buffer(1024);
        buf.writeBytes("test-metrics".getBytes());
        storage.put(buf, location);
        buf.release();
        var metrics = inMemoryMetricReader.collectAllMetrics();
        assertThat(metrics).anySatisfy(metric -> {
            OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.write.bytes.count");
            OpenTelemetryAssertions.assertThat(metric).hasLongSumSatisfying(it -> {
                it.hasPointsSatisfying(points -> {
                    points.hasValue(12);
                });
            });
        });
        assertThat(metrics).anySatisfy(metric -> {
            OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.write.duration");
        });

        storage.get(location).release();
        metrics = inMemoryMetricReader.collectAllMetrics();
        assertThat(metrics).anySatisfy(metric -> {
            OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.read.bytes.count");
            OpenTelemetryAssertions.assertThat(metric).hasLongSumSatisfying(it -> {
                it.hasPointsSatisfying(points -> {
                    points.hasValue(12);
                });
            });
        });
        assertThat(metrics).anySatisfy(metric -> {
            OpenTelemetryAssertions.assertThat(metric).hasName("ursa.storage.backend.read.duration");
        });
    }

    @Test
    public void testUpdateRulesWithEmptyRules() throws Exception {
        List<? extends BucketInfo.LifecycleRule> existingRules = Collections.emptyList();
        var prefixes = getPrefixes(10, LocalDateTime.now());

        var rules = fileStorage.updateRules(existingRules, prefixes);

        assertEquals(1, rules.size());
        assertEquals(BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction(), rules.get(0).getAction());
        assertNotNull(rules.get(0).getCondition());
        assertEquals(0, rules.get(0).getCondition().getAge());
        assertEquals(10, rules.get(0).getCondition().getMatchesPrefix().size());
        checkRulesAreExpected(prefixes, config.getPrefix(), rules, 0);
    }

    @Test
    public void testExpireRules() throws Exception {
        var existingPrefixes = getPrefixes(10, LocalDateTime.now().minusDays(10))
            .stream().map(p -> config.getPrefix() + "/" + p).toList();
        var action = BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction();
        var condition = BucketInfo.LifecycleRule.LifecycleCondition.newBuilder()
            .setAge(0)
            .setMatchesPrefix(existingPrefixes)
            .build();
        var rule = new BucketInfo.LifecycleRule(action, condition);
        List<? extends BucketInfo.LifecycleRule> existingRules = List.of(rule);

        var newPrefixes = getPrefixes(3, LocalDateTime.now());
        var rules = fileStorage.updateRules(existingRules, newPrefixes);

        assertEquals(1, rules.size());
        assertEquals(BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction(), rules.get(0).getAction());
        assertNotNull(rules.get(0).getCondition());
        assertEquals(0, rules.get(0).getCondition().getAge());
        assertEquals(3, rules.get(0).getCondition().getMatchesPrefix().size());
        checkRulesAreExpected(newPrefixes, config.getPrefix(), rules, 0);
    }

    @Test
    public void testBulkDelete() throws Exception {
        String location = "test-bulk-delete";
        // write
        int count = 10;
        var locations = new ArrayList<String>(count);
        for (int i = 0; i < count; ++i) {
            ByteBuf buf = Unpooled.buffer(1024);
            buf.writeBytes(("test-" + i).getBytes());
            try {
                fileStorage.putAsync(buf, location + "-" + i).get();
                locations.add(config.getPrefix() + "/" + location + "-" + i);
            } catch (Exception e) {
                log.error("Failed to write data to S3", e);
                fail();
            } finally {
                buf.release();
            }
        }

        fileStorage.deleteAsync(locations).get();

        for (int i = 0; i < count; ++i) {
            ByteBuf readBuf = null;
            try {
                readBuf = fileStorage.getAsync(location + "-" + i).get();
                fail("The file should have been deleted: " + location + "-" + i);
            } catch (Throwable e) {
                var realCause = FutureUtils.unwrapCompletionException(e);
                if (realCause instanceof FileStorageException fileStorageException) {
                    // Expected, since the files should be deleted
                    assertThat(fileStorageException.getMessage()).contains("does not exist");
                } else {
                    log.error("Failed to read data from S3", e);
                    fail();
                }
            } finally {
                if (readBuf != null) {
                    readBuf.release();
                }
            }
        }

        // Should not throw exception if files do not exist
        fileStorage.deleteAsync(List.of("no-exist-1", "no-exist-2")).get();
    }

    void checkRulesAreExpected(Set<String> prefix, String bucketPrefix,
                               List<? extends BucketInfo.LifecycleRule> rules, int otherRulesCount) {
        Set<String> expectedPrefixes = new HashSet<>(prefix);
        List<String> prefixInRules = new ArrayList<>();
        for (BucketInfo.LifecycleRule rule : rules) {
            for (String matchesPrefix : rule.getCondition().getMatchesPrefix()) {
                var p = matchesPrefix.replace(bucketPrefix + "/", "");
                prefixInRules.add(p);
                expectedPrefixes.remove(p);
            }
        }
        assertEquals(0, expectedPrefixes.size());
        assertEquals(prefix.size(), prefixInRules.size() - otherRulesCount, prefixInRules.toString());
    }

    Set<String> getPrefixes(int n, LocalDateTime dateTime) throws Exception {
        IDGenerator idGenerator = IDGenerator.create("dateuuid", "", null);

        // mock prefixes
        Set<String> prefixes = new HashSet<>();
        for (int i = 0; i < n; i++) {
            prefixes.add(IDGeneratorWithDate.getDatePrefix(dateTime.minusHours(i)));
        }
        return prefixes;
    }
}
