/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.lakestream.api.materialization.TableIdentifier;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * The commit side resolves its table from the log name alone, so this has to answer the same way the
 * materialization policy does for the same stream - with and without a naming template.
 */
class StreamTableNamingTest {

    private static final String LOG_NAME =
        "default/orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w-partition-0";

    @Test
    void withoutATemplateItNamesTheTableAfterTheStream() {
        TableIdentifier table = StreamTableNaming.resolve(LOG_NAME, new Properties());

        assertThat(table.namespace()).isEqualTo("default");
        assertThat(table.name()).isEqualTo("orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w");
    }

    @Test
    void aTemplateMayNameTheTableFromAStreamProperty() {
        Properties properties = new Properties();
        properties.setProperty(StreamTableNaming.TABLE_NAME_TEMPLATE_PROPERTY,
            "${stream.property.lakestream.kafka.topic.name}");
        properties.setProperty("lakestream.kafka.topic.name", "orders");

        TableIdentifier table = StreamTableNaming.resolve(LOG_NAME, properties);

        assertThat(table.namespace()).isEqualTo("default");
        assertThat(table.name()).isEqualTo("orders");
    }

    @Test
    void aBlankTemplateFallsBackToTheStreamName() {
        Properties properties = new Properties();
        properties.setProperty(StreamTableNaming.TABLE_NAME_TEMPLATE_PROPERTY, "   ");

        assertThat(StreamTableNaming.resolve(LOG_NAME, properties).name())
            .isEqualTo("orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w");
    }

    @Test
    void itResolvesTheCanonicalNameFormTheSameWay() {
        TableIdentifier table = StreamTableNaming.resolve("public/default/orders-partition-7", new Properties());

        assertThat(table.namespace()).isEqualTo("public/default");
        assertThat(table.name()).isEqualTo("orders");
    }
}
