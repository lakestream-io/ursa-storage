/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test.containers.util;

import java.net.URI;
import java.util.Optional;
import lombok.Cleanup;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3Container extends LocalStackContainer {

    private static final DockerImageName IMAGE =
        DockerImageName.parse("localstack/localstack")
            .withTag("3.6");


    public S3Container(Optional<Network> network) {
        super(IMAGE);
        network.ifPresent(this::withNetwork);
    }

    public void prepareBucket(String bucket) {
        @Cleanup
        var s3Client = S3Client.builder()
            .endpointOverride(endpoint())
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("fake", "fake")))
            .region(Region.of(this.getRegion()))
            .build();
        s3Client.createBucket(b -> b.bucket(bucket));
    }

    public URI endpoint() {
        return this.getEndpointOverride(LocalStackContainer.Service.S3);
    }

}
