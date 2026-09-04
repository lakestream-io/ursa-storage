/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.api.SourceMetadataProperties;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMode;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * The commit side prefers the final identity persisted by materialization and retains template and
 * stream-name resolution for tasks created by older versions.
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
    void newExternalWriterDefaultsToSourceLogicalName() {
        Properties properties = new Properties();
        properties.setProperty("streamTableMode", "EXTERNAL");
        properties.setProperty(SourceMetadataProperties.LOGICAL_NAME_PROPERTY, "orders");
        properties.setProperty("lakestream.kafka.topic.name", "legacy-orders");

        TableIdentifier table = StreamTableNaming.resolveForWriter(LOG_NAME, properties);

        assertThat(table).isEqualTo(new TableIdentifier("default", "orders"));
        // An already-written legacy task has no persisted destination. Its committer must retain the
        // historical storage-name fallback rather than guessing that its writer used the new default.
        assertThat(StreamTableNaming.resolve(LOG_NAME, properties).name())
                .isEqualTo("orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w");
    }

    @Test
    void knownExternalWriterUsesLogicalDefaultWhenLegacyTaskOmitsMode() {
        Properties properties = new Properties();
        properties.setProperty(SourceMetadataProperties.LOGICAL_NAME_PROPERTY, "orders");

        assertThat(StreamTableNaming.resolveForWriter(LOG_NAME, properties, TableMode.EXTERNAL))
                .isEqualTo(new TableIdentifier("default", "orders"));
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
    void resolvedIdentifierWinsOverTemplateAndLogName() {
        Properties properties = new Properties();
        properties.putAll(StreamTableNaming.withResolvedTableIdentifier(
                Map.of(StreamTableNaming.TABLE_NAME_TEMPLATE_PROPERTY, "${stream.name}_legacy"),
                new TableIdentifier("analytics", "orders_archive")));

        TableIdentifier table = StreamTableNaming.resolve(LOG_NAME, properties);

        assertThat(table).isEqualTo(new TableIdentifier("analytics", "orders_archive"));
    }

    @Test
    void resolvedIdentifierPreservesANameThatLooksLikeAPartitionedLog() {
        Properties properties = new Properties();
        properties.putAll(StreamTableNaming.withResolvedTableIdentifier(
                Map.of(), new TableIdentifier("analytics", "orders-partition-0")));

        assertThat(StreamTableNaming.resolve(LOG_NAME, properties))
                .isEqualTo(new TableIdentifier("analytics", "orders-partition-0"));
    }

    @Test
    void deadLetterTableKeepsTheResolvedNamespaceAndAppendsTheSuffix() {
        assertThat(StreamTableNaming.deadLetterTable(
                new TableIdentifier("analytics", "orders"), "_dlt"))
                .isEqualTo(new TableIdentifier("analytics", "orders_dlt"));
    }

    @Test
    void incompleteResolvedIdentifierIsRejectedInsteadOfSplittingWriterAndCommitter() {
        Properties properties = new Properties();
        properties.setProperty(StreamTableNaming.RESOLVED_TABLE_NAME_PROPERTY, "orders");

        assertThatThrownBy(() -> StreamTableNaming.resolve(LOG_NAME, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incomplete resolved table identifier");
    }

    @Test
    void aBlankTemplateIsRejected() {
        Properties properties = new Properties();
        properties.setProperty(StreamTableNaming.TABLE_NAME_TEMPLATE_PROPERTY, "   ");

        assertThatThrownBy(() -> StreamTableNaming.resolve(LOG_NAME, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Interpolated table name is empty");
    }

    @Test
    void itResolvesTheCanonicalNameFormTheSameWay() {
        TableIdentifier table = StreamTableNaming.resolve("public/default/orders-partition-7", new Properties());

        assertThat(table.namespace()).isEqualTo("public/default");
        assertThat(table.name()).isEqualTo("orders");
    }
}
