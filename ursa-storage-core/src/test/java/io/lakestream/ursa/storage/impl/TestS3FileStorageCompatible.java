/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

public class TestS3FileStorageCompatible extends TestS3FileStorage {

    StorageConfig getStorageConfig() {
        StorageConfig config = new StorageConfig();
        config.setS3Region(localStack.getRegion());
        config.setBucket(bucket);
        config.setPrefix("test");
        config.setCloudStorageEndpoint(localStack.getEndpoint().toString());
        config.setS3AccessKeyId(localStack.getAccessKey());
        config.setS3SecretAccessKey(localStack.getSecretKey());
        return config;
    }

    String getPrefix(StorageConfig config) {
        return config.getPrefix();
    }

    void setPrefix(StorageConfig config, String prefix) {
        config.setPrefix(prefix);
    }

    String getBucket(StorageConfig config) {
        return config.getBucket();
    }

    void setBucket(StorageConfig config, String bucket) {
        config.setBucket(bucket);
    }
}
