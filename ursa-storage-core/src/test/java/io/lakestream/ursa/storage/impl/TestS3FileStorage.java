/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.IDGeneratorWithDate;
import io.lakestream.ursa.storage.S3BasedTestClass;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

@Slf4j
public class TestS3FileStorage extends S3BasedTestClass {

    StorageConfig getStorageConfig() {
        StorageConfig config = new StorageConfig();
        config.setS3Region(localStack.getRegion());
        config.setS3Bucket(bucket);
        config.setS3Prefix("test");
        config.setCloudStorageEndpoint(localStack.getEndpoint().toString());
        config.setS3AccessKeyId(localStack.getAccessKey());
        config.setS3SecretAccessKey(localStack.getSecretKey());
        return config;
    }

    String getPrefix(StorageConfig config) {
        return config.getS3Prefix();
    }

    void setPrefix(StorageConfig config, String prefix) {
        config.setS3Prefix(prefix);
    }

    String getBucket(StorageConfig config) {
        return config.getS3Bucket();
    }

    void setBucket(StorageConfig config, String bucket) {
        config.setS3Bucket(bucket);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        StorageConfig config = getStorageConfig();

        @Cleanup
        S3FileStorage s3BackendStorage = new S3FileStorage(config, InstrumentProvider.NOOP);

        String location = "test-dev";
        // write
        int count = 10;
        for (int i = 0; i < count; ++i) {
            ByteBuf buf = Unpooled.buffer(1024);
            buf.writeBytes(("test-" + i).getBytes());
            try {
                s3BackendStorage.put(buf, location + "-" + i);
            } catch (IOException e) {
                log.error("Failed to write data to S3", e);
                fail();
            } finally {
                buf.release();
            }
        }

        List<S3Object> objectList = s3Client.listObjectsV2(b -> b.bucket(getBucket(config))).contents();
        assertEquals(count, objectList.size());
        for (int i = 0; i < count; ++i) {
            assertEquals(getPrefix(config) + "/" + location + "-" + i, objectList.get(i).key());
        }

        // read
        for (int i = 0; i < count; ++i) {
            ByteBuf readBuf = null;
            try {
                readBuf = s3BackendStorage.get(location + "-" + i);
                assertNotNull(readBuf);
                assertEquals("test-" + i, readBuf.toString(StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.error("Failed to read data from S3", e);
                fail();
            } finally {
                if (readBuf != null) {
                    readBuf.release();
                }
            }
        }


        // delete
        for (int i = 0; i < count; ++i) {
            try {
                s3BackendStorage.delete(location + "-" + i);
            } catch (IOException e) {
                log.error("Failed to delete data from S3", e);
                fail();
            }
        }

        objectList = s3Client.listObjectsV2(b -> b.bucket(getBucket(config))).contents();
        assertEquals(0, objectList.size());
    }

    @Test
    public void testDeleteWithLifecycle() throws Exception {
        StorageConfig config = getStorageConfig();
        @Cleanup
        S3FileStorage s3BackendStorage = new S3FileStorage(config, InstrumentProvider.NOOP);

        s3BackendStorage.deleteWithDatePrefixes(Set.of("prefix")).get();

        GetBucketLifecycleConfigurationRequest request = GetBucketLifecycleConfigurationRequest
            .builder().bucket(bucket).build();
        List<LifecycleRule> rules = s3Client.getBucketLifecycleConfiguration(request).rules();
        assertEquals(1, rules.size());
        Integer days = rules.get(0).expiration().days();
        assertEquals(1, days);
        String prefix = rules.get(0).filter().prefix();
        assertEquals(getPrefix(config) + "/prefix", prefix);
    }

    @Test
    public void testDeleteWithExistingLifeCyclePolicies() throws Exception {
        var putLifecycleConfigReq = PutBucketLifecycleConfigurationRequest.builder()
            .bucket(bucket)
            .lifecycleConfiguration(b -> b.rules(
                LifecycleRule.builder()
                    .id("rule1")
                    .filter(LifecycleRuleFilter.builder().prefix("prefix1").build())
                    .expiration(e -> e.days(1))
                    .status("Enabled")
                    .build()
            )).build();
        s3Client.putBucketLifecycleConfiguration(putLifecycleConfigReq);

        var listLifecycleConfigReq = GetBucketLifecycleConfigurationRequest.builder()
            .bucket(bucket)
            .build();
        var listLifecycleConfigResp = s3Client.getBucketLifecycleConfiguration(listLifecycleConfigReq);
        assertEquals(1, listLifecycleConfigResp.rules().size());
        assertEquals("rule1", listLifecycleConfigResp.rules().get(0).id());

        // set the delete policy for the bucket with prefix
        StorageConfig config = getStorageConfig();
        @Cleanup
        S3FileStorage s3BackendStorage = new S3FileStorage(config, InstrumentProvider.NOOP);
        IDGenerator idGenerator = IDGenerator.create("dateuuid", "", null);
        final String fileId = idGenerator.generate();
        final LocalDateTime timePrefix = IDGeneratorWithDate.getDatePrefix(fileId);
        final String prefix = IDGeneratorWithDate.getDatePrefix(timePrefix);
        try {
            s3BackendStorage.deleteWithDatePrefixes(Set.of(prefix)).get();
        } catch (Exception e) {
            log.error("Failed to delete with date prefixes", e);
            fail();
        }

        // check the lifecycle policy
        listLifecycleConfigResp = s3Client.getBucketLifecycleConfiguration(listLifecycleConfigReq);
        List<LifecycleRule> rules = new ArrayList<>(listLifecycleConfigResp.rules());

        assertEquals(2, rules.size());

        // check the new rule
        var rule1 = rules.get(0);
        assertEquals("rule1", rule1.id());

        var rule2 = rules.get(1);
        assertEquals(s3BackendStorage.getLifecycleRuleID(prefix), rule2.id());
        assertEquals(1, rule2.expiration().days());
        assertEquals(getPrefix(config) + "/" + prefix, rule2.filter().prefix());

        // delete again
        try {
            s3BackendStorage.deleteWithDatePrefixes(Set.of(prefix)).get();
        } catch (Exception e) {
            log.error("Failed to delete with date prefixes", e);
            fail();
        }

        // check the lifecycle policy
        listLifecycleConfigResp = s3Client.getBucketLifecycleConfiguration(listLifecycleConfigReq);
        rules = new ArrayList<>(listLifecycleConfigResp.rules());

        assertEquals(2, rules.size());

        // check the new rule
        rule1 = rules.get(0);
        assertEquals("rule1", rule1.id());

        rule2 = rules.get(1);
        assertEquals(1, rule2.expiration().days());
        assertEquals(getPrefix(config) + "/" + prefix, rule2.filter().prefix());
    }

    @Test
    public void testBuildRules_expireTheExistingRules() throws Exception {
        StorageConfig config = getStorageConfig();
        @Cleanup
        S3FileStorage s3BackendStorage = new S3FileStorage(config, InstrumentProvider.NOOP);
        IDGenerator idGenerator = IDGenerator.create("dateuuid", "", null);

        // mock prefixes
        List<String> prefixes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            prefixes.add(IDGeneratorWithDate.getDatePrefix(LocalDateTime.now().minusDays(8).minusHours(i)));
        }

        // build rules
        List<LifecycleRule> rules = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            LifecycleRule rule = LifecycleRule.builder()
                .id(s3BackendStorage.getLifecycleRuleID(prefixes.get(i)))
                .filter(LifecycleRuleFilter.builder().prefix(getPrefix(config) + "/" + prefixes.get(i)).build())
                .build();
            rules.add(rule);
        }

        List<LifecycleRule> results = s3BackendStorage.buildRules(new HashSet<>(prefixes), rules);
        assertEquals(0, results.size());
    }

    @Test
    public void testBuildRules_includeBucketPrefix() throws Exception {
        StorageConfig config = getStorageConfig();
        setPrefix(config, "directory/A");
        setBucket(config, "dummy");
        @Cleanup
        S3FileStorage s3BackendStorage = new S3FileStorage(config, InstrumentProvider.NOOP);

        // mock prefixes
        List<String> prefixes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            prefixes.add(IDGeneratorWithDate.getDatePrefix(LocalDateTime.now().minusHours(i)));
        }
        prefixes.sort(String::compareTo);

        // build rules
        List<LifecycleRule> rules = s3BackendStorage.buildRules(new HashSet<>(prefixes), new ArrayList<>());
        assertEquals(10, rules.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(1, rules.get(i).expiration().days());
            assertEquals(getPrefix(config) + "/" + prefixes.get(i), rules.get(i).filter().prefix());
        }
    }

    class MockedResponseTransformer extends ByteBufAsyncResponseTransformer {

        boolean releaseException() {
            return super.hasReleaseBufferException;
        }


        @Override
        public void onResponse(GetObjectResponse response) {
            super.onResponse(response);
            throw new RuntimeException("Mock error");
        }
    }

    @Test
    public void testBufRefWhenReadFailed() throws Exception {

        StorageConfig config = getStorageConfig();
        @Cleanup
        S3FileStorage s3BackendStorage = new S3FileStorage(config, InstrumentProvider.NOOP);

        String location = "test-ref";
        ByteBuf data = Unpooled.wrappedBuffer("test".getBytes());
        try {
            s3BackendStorage.put(data, location);
        } finally {
            data.release();
        }
        var responseTransformer = new MockedResponseTransformer();
        try {
            s3BackendStorage.getWithMetadataAsync(location, responseTransformer).get();
        } catch (Exception e) {
            assertFalse(responseTransformer.releaseException());
        }


    }

    @Test
    public void testBulkDelete() throws Exception {
        StorageConfig config = getStorageConfig();

        @Cleanup
        S3FileStorage s3BackendStorage = new S3FileStorage(config, InstrumentProvider.NOOP);

        String location = "test-bulk-delete";
        // write
        int count = 10;
        var locations = new ArrayList<String>(count);
        for (int i = 0; i < count; ++i) {
            ByteBuf buf = Unpooled.buffer(1024);
            buf.writeBytes(("test-" + i).getBytes());
            try {
                s3BackendStorage.put(buf, location + "-" + i);
                locations.add(config.getPrefix() + "/" + location + "-" + i);
            } catch (IOException e) {
                log.error("Failed to write data to S3", e);
                fail();
            } finally {
                buf.release();
            }
        }

        s3BackendStorage.deleteAsync(locations).get();

        // read
        for (int i = 0; i < count; ++i) {
            ByteBuf readBuf = null;
            try {
                readBuf = s3BackendStorage.get(location + "-" + i);
                fail();
            } catch (IOException e) {
                assertThat(e.getCause().getMessage()).contains("does not exist");
            } finally {
                if (readBuf != null) {
                    readBuf.release();
                }
            }
        }

        // Should not throw exception if files do not exist
        s3BackendStorage.deleteAsync(List.of("no-exist-1", "no-exist-2")).get();
    }

}
