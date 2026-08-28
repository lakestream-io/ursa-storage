/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.Position;
import io.lakestream.ursa.storage.IDGenerator;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class TestPositionImpl {

    @Test
    public void testPositionImpl() {
        Position position = new Position("test", 1, Position.FileType.RAW);
        Assertions.assertEquals("test", position.location());
        Assertions.assertEquals(1, position.indexId());


        Position position2 = Position.parseV2Format("a-test-1-RAW");
        Assertions.assertEquals("a-test", position2.location());
        Assertions.assertEquals(1, position2.indexId());
        Assertions.assertEquals(Position.FileType.RAW, position2.fileType());
    }


    @CsvSource({"uuid", "random", "memory", "oxia"})
    @ParameterizedTest
    public void testCompatibility(String idGeneratorType) throws Exception {
        @Cleanup("stop")
        OxiaContainer oxiaContainer = null;
        @Cleanup
        AsyncOxiaClient oxiaClient = null;
        if (idGeneratorType.equals("oxia")) {
            oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:latest"));
            oxiaContainer.start();
            oxiaClient = OxiaClientBuilder.create(oxiaContainer.getServiceAddress()).asyncClient().get();
        }
        IDGenerator generator = IDGenerator.create(idGeneratorType, "id-generator", oxiaClient);
        String location = generator.generate();
        Position position = new Position(location, 1, Position.FileType.RAW);

        Assertions.assertEquals(location, position.location());
        Assertions.assertEquals(1, position.indexId());

        String p = position.toV1Format();
        log.info("position: {}", p);
        Position position1 = Position.parseV1Format(p);
        Assertions.assertEquals(location, position1.location());
        Assertions.assertEquals(1, position1.indexId());
        Assertions.assertEquals(Position.FileType.RAW, position1.fileType());
    }
}
