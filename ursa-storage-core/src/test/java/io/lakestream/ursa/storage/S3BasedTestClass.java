/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Slf4j
@Getter
public class S3BasedTestClass {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("localstack/localstack");
    private static final String tag = "3.6";

    public LocalStackContainer localStack;
    public S3Client s3Client;
    public String bucket = "test-dev";

    @BeforeEach
    public void setup() throws Exception {
        localStack = new LocalStackContainer(DEFAULT_IMAGE_NAME.withTag(tag))
            .withServices(LocalStackContainer.Service.S3);
        localStack.start();

        s3Client = S3Client.builder()
            .endpointOverride(localStack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("fake", "fake")))
            .region(Region.of(localStack.getRegion()))
            .build();
        s3Client.createBucket(b -> b.bucket(bucket));

    }

    @AfterEach
    public void cleanup() throws Exception {
        if (s3Client != null) {
            try {
                s3Client.close();
            } catch (Exception e) {
                log.error("Failed to close s3 client", e);
            }
        }
        if (localStack != null) {
            try {
                localStack.stop();
            } catch (Exception e) {
                log.error("Failed to stop local stack", e);
            }
        }
    }
}
