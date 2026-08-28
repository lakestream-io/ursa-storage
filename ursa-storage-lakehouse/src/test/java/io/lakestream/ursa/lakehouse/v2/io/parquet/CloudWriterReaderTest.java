/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.test.containers.util.S3Container;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

@Tag("lakehouse")
public class CloudWriterReaderTest extends UrsaParquetFileWriterReaderTest {

    private static S3Container s3Container;
    private static String bucket;

    @BeforeAll
    public static void setUp() {
        s3Container = new S3Container(Optional.empty());
        s3Container.start();
        bucket = "test-" + RandomStringUtils.secure().nextNumeric(4);
        s3Container.prepareBucket(bucket);
    }

    @BeforeEach
    @Override
    public void setup() throws IOException {
        Properties properties = new Properties();
        properties.put("compactionBucket", bucket);
        properties.put("compactionPrefix", "test");
        properties.put("compactionBackendStorageType", "S3");
        configuration = new LakehouseConfiguration(properties);
        var hadoopConf = configuration.getHadoopConfiguration();
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        hadoopConf.set("fs.s3a.endpoint", s3Container.endpoint().toString());
        hadoopConf.set("fs.s3a.access.key", "fake");
        hadoopConf.set("fs.s3a.secret.key", "fake");
        uri = URI.create(configuration.getStoragePath());
    }
}
