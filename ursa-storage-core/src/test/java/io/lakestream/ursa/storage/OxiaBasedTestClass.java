/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class OxiaBasedTestClass {

    protected OxiaContainer oxiaContainer;
    protected FailureInjectedOxiaClient oxiaClient;

    @BeforeEach
    public void setup() {
        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:latest"));
        oxiaContainer.setCommand("oxia standalone -s 1 --wal-sync-data=false");
        oxiaContainer.start();
        String serviceAddress = oxiaContainer.getServiceAddress();
        AsyncOxiaClient asyncOxiaClient = OxiaClientBuilder.create(serviceAddress)
                .asyncClient()
                .join();
        oxiaClient = new RealOxiaClient(asyncOxiaClient);
    }

    @AfterEach
    public void cleanup() throws Exception {
        try {
            oxiaClient.close();
        } catch (Exception e) {
            log.error("Failed to close client", e);
        }
        oxiaContainer.stop();
    }
}
