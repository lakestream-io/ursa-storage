/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.materialization.CommitConfig;
import io.lakestream.api.materialization.FrameworkConf;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.WriteMode;
import io.lakestream.ursa.materialization.FailureMessageHandler;
import io.lakestream.ursa.materialization.MaterializationMetrics;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.TableMaterializerFactory;
import io.lakestream.ursa.materialization.serde.SchemaEvolutionManager;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSourceMetadata;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link ClickHouseTableMaterializerFactory}.
 *
 * <p>{@code create()} flows through {@link DriverManager#getConnection(String, Properties)}. To
 * exercise that path without a real ClickHouse running, the suite registers a stub JDBC
 * {@link Driver} that recognises {@code jdbc:ursa-test:clickhouse://…} URLs and hands back a
 * Mockito-backed {@link Connection}. That keeps the test fully in-process while still using the
 * production {@link ClickHouseConnectionFactory}.
 */
class ClickHouseTableMaterializerFactoryTest {

    private static StubDriver driver;

    @BeforeAll
    static void registerDriver() throws Exception {
        driver = new StubDriver();
        DriverManager.registerDriver(driver);
    }

    @AfterAll
    static void deregisterDriver() throws Exception {
        if (driver != null) {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void catalogTypeIsClickHouse() {
        assertThat(new ClickHouseTableMaterializerFactory().catalogType())
                .isEqualTo(TableCatalogType.CLICKHOUSE);
    }

    @Test
    void schemaServiceReturnsClickHouseTableSchemaService() {
        TableCatalog catalog = new TableCatalog(
                "ch",
                TableCatalogType.CLICKHOUSE,
                Map.of("dsn", "jdbc:ursa-test:clickhouse://localhost:8123/analytics"),
                Map.of());
        TableMaterializationPolicy policy = withTableId("analytics", "events");
        assertThat(new ClickHouseTableMaterializerFactory()
                .schemaService(policy, catalog, fakeStream("public/default", "events")))
                .isInstanceOf(ClickHouseTableSchemaService.class);
    }

    @Test
    void createReturnsClickHouseTableMaterializerWithAppendDefaults() {
        TableCatalog catalog = new TableCatalog(
                "ch",
                TableCatalogType.CLICKHOUSE,
                Map.of(
                        "dsn", "jdbc:ursa-test:clickhouse://localhost:8123/analytics",
                        "user", "default",
                        "password", "secret"),
                Map.of());
        TableMaterializationPolicy policy = withTableId("analytics", "events");

        TableMaterializer<?> materializer = new ClickHouseTableMaterializerFactory()
                .create(policy, catalog, fakeStream("public/default", "events"), runtime());

        assertThat(materializer).isInstanceOf(ClickHouseTableMaterializer.class);
        ClickHouseTableMaterializer ch = (ClickHouseTableMaterializer) materializer;
        assertThat(ch.engine()).isEqualTo(ClickHouseTableEngine.MERGE_TREE);
        // ClickHouse evolution defaults: strict (only addColumn / addNullableColumn).
        assertThat(materializer.supportedEvolutions().addColumn()).contains(Boolean.TRUE);
        assertThat(materializer.supportedEvolutions().widenType()).contains(Boolean.FALSE);
    }

    @Test
    void createPicksReplacingMergeTreeForUpsertPolicy() {
        TableCatalog catalog = new TableCatalog(
                "ch",
                TableCatalogType.CLICKHOUSE,
                Map.of("dsn", "jdbc:ursa-test:clickhouse://localhost:8123/analytics"),
                Map.of());
        TableMaterializationPolicy policy = upsertPolicy("analytics", "events");

        TableMaterializer<?> materializer = new ClickHouseTableMaterializerFactory()
                .create(policy, catalog, fakeStream("public/default", "events"), runtime());

        ClickHouseTableMaterializer ch = (ClickHouseTableMaterializer) materializer;
        assertThat(ch.engine()).isEqualTo(ClickHouseTableEngine.REPLACING_MERGE_TREE);
    }

    @Test
    void createClosesJdbcConnectionWhenMaterializerConstructionFails() throws Exception {
        TableCatalog catalog = new TableCatalog(
                "ch",
                TableCatalogType.CLICKHOUSE,
                Map.of("dsn", "jdbc:ursa-test:clickhouse://localhost:8123/analytics"),
                Map.of());
        TableMaterializationPolicy invalidBatchSize = withBatchSize("analytics", "events", 0);

        assertThatThrownBy(() -> new ClickHouseTableMaterializerFactory()
                .create(invalidBatchSize, catalog, fakeStream("public/default", "events"), runtime()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchSize must be > 0");

        verify(driver.lastConnection()).close();
    }

    @Test
    void createFoldsNamespaceEncodedTableNameToDottedIdentifier() {
        // The framework encodes the stream namespace into the table name as
        // ${stream.namespace}.${stream.name},
        // so the resolved name arrives with the namespace path separator, e.g. "public/test_v7.test_v4".
        // ClickHouse has no path separator: the sink must fold '/' to '.' so the destination is the single
        // dotted identifier "public.test_v7.test_v4" in the configured database.
        TableCatalog catalog = new TableCatalog(
                "ch",
                TableCatalogType.CLICKHOUSE,
                Map.of("dsn", "jdbc:ursa-test:clickhouse://localhost:8123/default"),
                Map.of());
        TableMaterializationPolicy policy = withTableId("default", "public/test_v7.test_v4");

        TableMaterializer<?> materializer = new ClickHouseTableMaterializerFactory()
                .create(policy, catalog, fakeStream("public/test_v7", "test_v4"), runtime());

        ClickHouseTableMaterializer ch = (ClickHouseTableMaterializer) materializer;
        assertThat(ch.tableIdentifier().namespace()).isEqualTo("default");
        assertThat(ch.tableIdentifier().name()).isEqualTo("public.test_v7.test_v4");
    }

    @Test
    void recreatedKafkaStreamsUseLogicalTopicOnlyForSchemaLookup() {
        StreamMetadata oldStream = fakeStream("default", "orders-old-topic-id");
        StreamMetadata newStream = fakeStream("default", "orders-new-topic-id");
        MaterializationRuntime kafkaRuntime = runtimeWith(mock(KafkaSchemaService.class))
                .withTaskProperties(Map.of(KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "orders"));

        assertThat(oldStream.identifier().fullName()).isNotEqualTo(newStream.identifier().fullName());
        assertThat(ClickHouseTableMaterializerFactory.sourceTopic(oldStream, kafkaRuntime))
                .isEqualTo("orders");
        assertThat(ClickHouseTableMaterializerFactory.sourceTopic(newStream, kafkaRuntime))
                .isEqualTo("orders");
    }

    @Test
    void storagePartitionFallbackDerivesLogicalSchemaTopic() {
        StreamMetadata stream = fakeStream("default", "orders-partition-3");
        MaterializationRuntime kafkaRuntime = runtimeWith(mock(KafkaSchemaService.class));

        assertThat(ClickHouseTableMaterializerFactory.sourceTopic(stream, kafkaRuntime))
                .isEqualTo("orders");
    }

    @Test
    void jsonFallbackDoesNotRequireKafkaSchemaService() {
        TableCatalog catalog = new TableCatalog(
                "ch",
                TableCatalogType.CLICKHOUSE,
                Map.of("dsn", "jdbc:ursa-test:clickhouse://localhost:8123/analytics"),
                Map.of());
        TableMaterializationPolicy policy = withTableId("analytics", "events");
        MaterializationRuntime rt = runtime();

        TableMaterializer<?> materializer = new ClickHouseTableMaterializerFactory()
                .create(policy, catalog, fakeStream("public/test_v7", "events"), rt);

        assertThat(materializer).isInstanceOf(ClickHouseTableMaterializer.class);
    }

    @Test
    void factoryRegisteredViaServiceLoader() {
        ServiceLoader<TableMaterializerFactory> loader =
                ServiceLoader.load(TableMaterializerFactory.class);
        Set<Class<?>> discovered = new HashSet<>();
        for (TableMaterializerFactory factory : loader) {
            discovered.add(factory.getClass());
        }
        assertThat(discovered).contains(ClickHouseTableMaterializerFactory.class);
    }

    @Test
    void serviceLoaderProducesExactlyOneClickHouseFactory() {
        ServiceLoader<TableMaterializerFactory> loader =
                ServiceLoader.load(TableMaterializerFactory.class);
        long count = 0;
        for (TableMaterializerFactory factory : loader) {
            if (factory.catalogType() == TableCatalogType.CLICKHOUSE) {
                count++;
            }
        }
        assertThat(count).isEqualTo(1L);
    }

    private static TableMaterializationPolicy withTableId(String ns, String name) {
        return new TableMaterializationPolicy(
                Optional.of("ch"),
                Optional.empty(),
                Optional.of(new TableIdentifier(ns, name)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }

    private static TableMaterializationPolicy upsertPolicy(String ns, String name) {
        FrameworkConf framework = new FrameworkConf(
                Optional.of(WriteMode.UPSERT),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new CommitConfig(
                        Optional.empty(), Optional.empty(), Optional.of(250))));
        return new TableMaterializationPolicy(
                Optional.of("ch"),
                Optional.empty(),
                Optional.of(new TableIdentifier(ns, name)),
                Optional.empty(),
                Optional.of(framework),
                Optional.empty(),
                Optional.of(List.of("id")),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }

    private static TableMaterializationPolicy withBatchSize(String ns, String name, int batchSize) {
        FrameworkConf framework = new FrameworkConf(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new CommitConfig(
                        Optional.empty(), Optional.empty(), Optional.of(batchSize))));
        return new TableMaterializationPolicy(
                Optional.of("ch"),
                Optional.empty(),
                Optional.of(new TableIdentifier(ns, name)),
                Optional.empty(),
                Optional.of(framework),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }

    private static StreamMetadata fakeStream(String namespace, String name) {
        StreamMetadata metadata = mock(StreamMetadata.class);
        when(metadata.identifier()).thenReturn(StreamIdentifier.of(namespace, name));
        return metadata;
    }

    @SuppressWarnings("unchecked")
    private static MaterializationRuntime runtime() {
        return runtimeWith((SchemaService<Object>) mock(SchemaService.class));
    }

    private static MaterializationRuntime runtimeWith(SchemaService<?> schemaService) {
        return new MaterializationRuntime(
                schemaService,
                mock(SchemaEvolutionManager.class),
                Executors.newSingleThreadExecutor(),
                LoggerFactory.getLogger(ClickHouseTableMaterializerFactoryTest.class),
                MaterializationMetrics.noop(),
                FailureMessageHandler.noop());
    }

    /**
     * In-process JDBC stub: recognises {@code jdbc:ursa-test:clickhouse://…} and returns a
     * Mockito-mocked {@link Connection} (with {@code prepareStatement} pre-wired) so the factory
     * test exercises the production code path without a live ClickHouse instance.
     */
    private static final class StubDriver implements Driver {
        private volatile Connection lastConnection;

        private Connection lastConnection() {
            return lastConnection;
        }

        @Override
        public Connection connect(String url, Properties info) throws java.sql.SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            try {
                Connection c = mock(Connection.class);
                PreparedStatement ps = mock(PreparedStatement.class);
                when(c.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(ps);
                lastConnection = c;
                return c;
            } catch (Exception e) {
                throw new java.sql.SQLException(e);
            }
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:ursa-test:clickhouse:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getLogger("StubDriver");
        }
    }
}
