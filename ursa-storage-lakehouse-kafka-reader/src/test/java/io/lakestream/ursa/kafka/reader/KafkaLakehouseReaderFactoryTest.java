/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.ursa.metrics.InstrumentProvider;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaLakehouseReaderFactoryTest {

    @Test
    void failedInitializationCanBeRetriedAndCloseIsSafe() {
        KafkaLakehouseReaderFactory factory = new KafkaLakehouseReaderFactory();
        Properties properties = new Properties();
        properties.setProperty("lakehouseIOThreadNum", "0");

        assertThatThrownBy(() -> factory.initialize(properties, InstrumentProvider.NOOP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");

        properties.setProperty("lakehouseIOThreadNum", "1");
        assertThatCode(() -> factory.initialize(properties, InstrumentProvider.NOOP)).doesNotThrowAnyException();
        assertThatThrownBy(() -> factory.initialize(properties, InstrumentProvider.NOOP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already initialized");

        factory.close();
        assertThatCode(factory::close).doesNotThrowAnyException();
        assertThatThrownBy(() -> factory.open("default/orders"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }
}
