/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.CommitConfig;
import io.lakestream.api.materialization.FrameworkConf;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableNaming;
import io.lakestream.api.materialization.WriteMode;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationContext;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration tests for the ClickHouse stream-to-table
 * materialization stack. Spins up a real ClickHouse via Testcontainers and
 * exercises both {@link ClickHouseTableMaterializer} and
 * {@link ClickHouseTableSchemaService} against a live JDBC connection.
 *
 * <p>Tagged {@code clickhouse} so the default surefire run (which excludes
 * that tag in this module's pom) skips Docker. Opt in with:
 * <pre>
 *     mvn -B -ntp test -pl ursa-storage-clickhouse -Dgroups=clickhouse
 * </pre>
 */
@Tag("clickhouse")
@Testcontainers
class ClickHouseTableMaterializerEndToEndTest {

    private static final DockerImageName CLICKHOUSE_IMAGE =
            DockerImageName.parse("clickhouse/clickhouse-server:24.10")
                    .asCompatibleSubstituteFor("clickhouse/clickhouse-server");

    @Container
    static final ClickHouseContainer CLICKHOUSE = new ClickHouseContainer(CLICKHOUSE_IMAGE);

    private Connection connection;

    @BeforeEach
    void openConnection() throws SQLException {
        connection = E2EHelpers.openConnection(CLICKHOUSE);
    }

    @AfterEach
    void closeConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void appendModeMaterializesAllRecords() throws Exception {
        TableIdentifier tableId = new TableIdentifier("default", "events_append");
        TableMaterializationPolicy policy = E2EHelpers.appendPolicy(tableId);

        // Bootstrap the destination table via the schema service. The append-only
        // table only needs (user_id, message) columns and no primary key.
        ClickHouseTableSchemaService schemaService = E2EHelpers.newSchemaService(
                E2EHelpers.openConnection(CLICKHOUSE), tableId, policy,
                "default/events_append");
        ClickHouseSchema schema = new ClickHouseSchema(
                List.of(new ClickHouseColumn("user_id", "Int64", false),
                        new ClickHouseColumn("message", "String", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        schemaService.evolveTableSchema(E2EHelpers.versions(1L, schema));

        // Write 1000 records and commit.
        try (ClickHouseTableMaterializer materializer = E2EHelpers.newMaterializer(
                E2EHelpers.openConnection(CLICKHOUSE), tableId, policy)) {
            for (long i = 0; i < 1000; i++) {
                String json = "{\"user_id\":" + i + ",\"message\":\"m" + i + "\"}";
                materializer.write(E2EHelpers.jsonEntry(json),
                        E2EHelpers.context(tableId.namespace() + "/" + tableId.name(), i));
            }
            materializer.commit();
        }

        // Verify the row count and one row's contents.
        assertThat(E2EHelpers.countRows(connection, tableId)).isEqualTo(1000L);
        Map<String, Object> sample = E2EHelpers.queryFirst(connection,
                "SELECT user_id, message FROM `" + tableId.namespace() + "`.`"
                        + tableId.name() + "` WHERE user_id = 42");
        assertThat(sample).containsEntry("user_id", 42L);
        assertThat(sample).containsEntry("message", "m42");
    }

    @Test
    void upsertModeWithReplacingMergeTreeDeduplicates() throws Exception {
        TableIdentifier tableId = new TableIdentifier("default", "events_upsert");
        TableMaterializationPolicy policy = E2EHelpers.upsertPolicy(tableId, List.of("user_id"));

        ClickHouseTableSchemaService schemaService = E2EHelpers.newSchemaService(
                E2EHelpers.openConnection(CLICKHOUSE), tableId, policy,
                "default/events_upsert");
        ClickHouseSchema schema = new ClickHouseSchema(
                List.of(new ClickHouseColumn("user_id", "Int64", false),
                        new ClickHouseColumn("message", "String", false)),
                List.of("user_id"),
                ClickHouseTableEngine.REPLACING_MERGE_TREE);
        schemaService.evolveTableSchema(E2EHelpers.versions(1L, schema));

        // 500 records spread over 250 distinct user_ids; the *latest* write per
        // user_id should win after the engine merges.
        try (ClickHouseTableMaterializer materializer = E2EHelpers.newMaterializer(
                E2EHelpers.openConnection(CLICKHOUSE), tableId, policy)) {
            for (long i = 0; i < 500; i++) {
                long userId = i % 250;
                String json = "{\"user_id\":" + userId + ",\"message\":\"v" + i + "\"}";
                materializer.write(E2EHelpers.jsonEntry(json),
                        E2EHelpers.context(tableId.namespace() + "/" + tableId.name(), i));
            }
            materializer.commit();
        }

        // Force a merge so dedup is observed even without waiting for background compaction.
        try (Statement st = connection.createStatement()) {
            st.execute("OPTIMIZE TABLE `" + tableId.namespace() + "`.`"
                    + tableId.name() + "` FINAL");
        }

        // SELECT ... FINAL guarantees the dedup view even if a background merge is still pending.
        long deduped = E2EHelpers.countSingleLong(connection,
                "SELECT count() FROM `" + tableId.namespace() + "`.`"
                        + tableId.name() + "` FINAL");
        assertThat(deduped).isEqualTo(250L);
    }

    @Test
    void schemaEvolutionAddColumnIsApplied() throws Exception {
        TableIdentifier tableId = new TableIdentifier("default", "events_evolve_add");
        TableMaterializationPolicy policy = E2EHelpers.appendPolicy(tableId);
        String streamId = "default/events_evolve_add";

        // V1: [id, name] — write 100 rows.
        ClickHouseTableSchemaService schemaService = E2EHelpers.newSchemaService(
                E2EHelpers.openConnection(CLICKHOUSE), tableId, policy, streamId);
        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("name", "String", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        schemaService.evolveTableSchema(E2EHelpers.versions(1L, v1));

        try (ClickHouseTableMaterializer materializer = E2EHelpers.newMaterializer(
                E2EHelpers.openConnection(CLICKHOUSE), tableId, policy)) {
            for (long i = 0; i < 100; i++) {
                String json = "{\"id\":" + i + ",\"name\":\"n" + i + "\"}";
                materializer.write(E2EHelpers.jsonEntry(json),
                        E2EHelpers.context(streamId, i));
            }
            materializer.commit();
        }

        // V2: [id, name, email (Nullable)] — evolve.
        ClickHouseSchema v2 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("name", "String", false),
                        new ClickHouseColumn("email", "Nullable(String)", true)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        schemaService.evolveTableSchema(E2EHelpers.versions(1L, v1, 2L, v2));

        // Verify the email column now exists in the live table.
        Map<String, String> liveColumns = E2EHelpers.describeColumns(connection, tableId);
        assertThat(liveColumns).containsKey("email");
        assertThat(liveColumns.get("email")).contains("String");

        // V1 rows should report NULL for the email column.
        long nullEmails = E2EHelpers.countSingleLong(connection,
                "SELECT count() FROM `" + tableId.namespace() + "`.`"
                        + tableId.name() + "` WHERE email IS NULL");
        assertThat(nullEmails).isEqualTo(100L);
    }

    @Test
    void schemaEvolutionDropColumnIsRejected() throws Exception {
        TableIdentifier tableId = new TableIdentifier("default", "events_evolve_drop");
        TableMaterializationPolicy policy = E2EHelpers.appendPolicy(tableId);
        String streamId = "default/events_evolve_drop";

        ClickHouseTableSchemaService schemaService = E2EHelpers.newSchemaService(
                E2EHelpers.openConnection(CLICKHOUSE), tableId, policy, streamId);
        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("name", "String", false),
                        new ClickHouseColumn("email", "String", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        schemaService.evolveTableSchema(E2EHelpers.versions(1L, v1));

        ClickHouseSchema v2 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("name", "String", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);

        assertThatThrownBy(() ->
                schemaService.evolveTableSchema(E2EHelpers.versions(1L, v1, 2L, v2)))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode())
                            .isEqualTo(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE);
                    assertThat(me.getMessage()).contains("drop-column");
                });

        // The email column must still be present in the live table.
        Map<String, String> liveColumns = E2EHelpers.describeColumns(connection, tableId);
        assertThat(liveColumns).containsKey("email");
    }

    @Test
    void namespacePolicyMaterializesAllStreams() throws Exception {
        // Simulate a namespace-level policy by using the same TableNaming template
        // to derive three TableIdentifiers from three StreamIdentifiers. The
        // orchestrator (T10) performs the full resolution; here we exercise the
        // contract at the materializer + naming layer.
        TableNaming naming = new TableNaming(Optional.empty(), "ns_${stream.name}");
        TableMaterializationPolicy namespacePolicy = E2EHelpers.namespacePolicy(naming);

        List<StreamIdentifier> streams = List.of(
                StreamIdentifier.of("default", "orders"),
                StreamIdentifier.of("default", "payments"),
                StreamIdentifier.of("default", "shipments"));

        for (StreamIdentifier stream : streams) {
            TableIdentifier tableId = naming.toTableIdentifier(stream);
            TableMaterializationPolicy policy = E2EHelpers.policyForStream(
                    namespacePolicy, tableId);
            ClickHouseTableSchemaService schemaService = E2EHelpers.newSchemaService(
                    E2EHelpers.openConnection(CLICKHOUSE), tableId, policy,
                    stream.fullName());
            ClickHouseSchema schema = new ClickHouseSchema(
                    List.of(new ClickHouseColumn("event_id", "Int64", false),
                            new ClickHouseColumn("payload", "String", false)),
                    List.of(),
                    ClickHouseTableEngine.MERGE_TREE);
            schemaService.evolveTableSchema(E2EHelpers.versions(1L, schema));

            try (ClickHouseTableMaterializer materializer = E2EHelpers.newMaterializer(
                    E2EHelpers.openConnection(CLICKHOUSE), tableId, policy)) {
                for (long i = 0; i < 10; i++) {
                    String json = "{\"event_id\":" + i + ",\"payload\":\""
                            + stream.name() + "-" + i + "\"}";
                    materializer.write(E2EHelpers.jsonEntry(json),
                            E2EHelpers.context(stream.fullName(), i));
                }
                materializer.commit();
            }
        }

        // Verify all three tables exist and are populated.
        for (StreamIdentifier stream : streams) {
            TableIdentifier tableId = naming.toTableIdentifier(stream);
            assertThat(E2EHelpers.tableExists(connection, tableId))
                    .as("table %s.%s exists", tableId.namespace(), tableId.name())
                    .isTrue();
            assertThat(E2EHelpers.countRows(connection, tableId))
                    .as("table %s.%s row count", tableId.namespace(), tableId.name())
                    .isEqualTo(10L);
        }
    }

    @Test
    void streamOptOutSkipsMaterialization() throws Exception {
        // Stream-level enabled=false must short-circuit resolution to Optional.empty()
        // regardless of the namespace baseline. No table should be created.
        TableNaming naming = new TableNaming(Optional.empty(), "off_${stream.name}");
        TableMaterializationPolicy namespacePolicy = new TableMaterializationPolicy(
                Optional.of("ch"),
                Optional.of(naming),
                Optional.empty(),
                Optional.of(Boolean.TRUE),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());
        TableMaterializationPolicy streamPolicy = new TableMaterializationPolicy(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Boolean.FALSE),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());

        StreamIdentifier stream = StreamIdentifier.of("default", "ignored");
        // The catalog lookup is irrelevant because resolution short-circuits before it is invoked.
        var resolved = TableMaterializationPolicy.resolve(
                Optional.of(namespacePolicy),
                Optional.of(streamPolicy),
                stream,
                name -> Optional.of(new TableCatalog(
                        "ch", TableCatalogType.CLICKHOUSE, Map.of(), Map.of())));
        assertThat(resolved).isEmpty();

        // Verify no opt-out table was created.
        TableIdentifier expected = naming.toTableIdentifier(stream);
        assertThat(E2EHelpers.tableExists(connection, expected)).isFalse();
    }

    /** Test-package helpers for the e2e scenarios. */
    static final class E2EHelpers {

        private E2EHelpers() {
        }

        /** Opens a fresh JDBC connection from the running container. */
        static Connection openConnection(ClickHouseContainer container) throws SQLException {
            return java.sql.DriverManager.getConnection(
                    container.getJdbcUrl(),
                    container.getUsername(),
                    container.getPassword());
        }

        /** Builds an append-only policy targeting the given table id. */
        static TableMaterializationPolicy appendPolicy(TableIdentifier tableId) {
            FrameworkConf framework = new FrameworkConf(
                    Optional.of(WriteMode.APPEND),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new CommitConfig(
                            Optional.empty(), Optional.empty(), Optional.of(200))));
            return new TableMaterializationPolicy(
                    Optional.of("ch"),
                    Optional.empty(),
                    Optional.of(tableId),
                    Optional.empty(),
                    Optional.of(framework),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Map.of());
        }

        /** Builds an upsert policy with the given primary key. */
        static TableMaterializationPolicy upsertPolicy(TableIdentifier tableId,
                                                       List<String> primaryKey) {
            FrameworkConf framework = new FrameworkConf(
                    Optional.of(WriteMode.UPSERT),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new CommitConfig(
                            Optional.empty(), Optional.empty(), Optional.of(200))));
            return new TableMaterializationPolicy(
                    Optional.of("ch"),
                    Optional.empty(),
                    Optional.of(tableId),
                    Optional.empty(),
                    Optional.of(framework),
                    Optional.empty(),
                    Optional.of(primaryKey),
                    Optional.empty(),
                    Optional.empty(),
                    Map.of());
        }

        /** Builds a namespace baseline policy that owns the table-naming template. */
        static TableMaterializationPolicy namespacePolicy(TableNaming naming) {
            FrameworkConf framework = new FrameworkConf(
                    Optional.of(WriteMode.APPEND),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            return new TableMaterializationPolicy(
                    Optional.of("ch"),
                    Optional.of(naming),
                    Optional.empty(),
                    Optional.of(Boolean.TRUE),
                    Optional.of(framework),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Map.of());
        }

        /**
         * Returns a stream-level policy that inherits the namespace's catalog ref
         * but pins the resolved table identifier so the materializer + schema
         * service can be constructed without going through the full
         * {@link TableMaterializationPolicy#resolve} flow.
         */
        static TableMaterializationPolicy policyForStream(TableMaterializationPolicy namespacePolicy,
                                                          TableIdentifier resolvedTableId) {
            return new TableMaterializationPolicy(
                    namespacePolicy.catalogRef(),
                    Optional.empty(),
                    Optional.of(resolvedTableId),
                    Optional.empty(),
                    namespacePolicy.framework(),
                    namespacePolicy.evolution(),
                    namespacePolicy.primaryKey(),
                    namespacePolicy.baseSchemaVersion(),
                    namespacePolicy.table(),
                    Map.of());
        }

        /** Builds a fresh materializer with engine + primary key derived from the policy. */
        static ClickHouseTableMaterializer newMaterializer(Connection connection,
                                                            TableIdentifier tableId,
                                                            TableMaterializationPolicy policy) {
            ClickHouseTableEngine engine = ClickHouseTableEngine.forPolicy(policy);
            List<String> primaryKey = policy.primaryKey().orElseGet(List::of);
            int batchSize = policy.framework()
                    .flatMap(FrameworkConf::commit)
                    .flatMap(CommitConfig::batchSize)
                    .orElse(ClickHouseTableMaterializer.DEFAULT_BATCH_SIZE);
            return new ClickHouseTableMaterializer(
                    connection, tableId, engine, primaryKey, batchSize);
        }

        /** Builds a schema service with engine + primary key derived from the policy. */
        static ClickHouseTableSchemaService newSchemaService(Connection connection,
                                                              TableIdentifier tableId,
                                                              TableMaterializationPolicy policy,
                                                              String streamId) {
            ClickHouseTableEngine engine = ClickHouseTableEngine.forPolicy(policy);
            List<String> primaryKey = policy.primaryKey().orElseGet(List::of);
            return new ClickHouseTableSchemaService(
                    connection, tableId, engine, primaryKey, streamId);
        }

        /** Builds a framed single-message WAL {@link GenericEntry} (the production encoding). */
        static GenericEntry jsonEntry(String json) {
            return FramedEntries.of(json);
        }

        /** Returns a {@link MaterializationContext} for an unversioned stream record. */
        static MaterializationContext context(String streamFullName, long offset) {
            int slash = streamFullName.indexOf('/');
            String ns = slash < 0 ? "default" : streamFullName.substring(0, slash);
            String name = slash < 0 ? streamFullName : streamFullName.substring(slash + 1);
            return new MaterializationContext(
                    StreamIdentifier.of(ns, name),
                    offset,
                    0L,
                    Optional.empty(),
                    Map.of());
        }

        /** Builds a {@link TreeMap} for {@code ClickHouseTableSchemaService#evolveTableSchema}. */
        static TreeMap<Long, ClickHouseSchema> versions(Object... pairs) {
            TreeMap<Long, ClickHouseSchema> map = new TreeMap<>();
            for (int i = 0; i < pairs.length; i += 2) {
                map.put((Long) pairs[i], (ClickHouseSchema) pairs[i + 1]);
            }
            return map;
        }

        /** Returns the row count for the supplied table via {@code SELECT count()}. */
        static long countRows(Connection conn, TableIdentifier tableId) throws SQLException {
            return countSingleLong(conn,
                    "SELECT count() FROM `" + tableId.namespace() + "`.`"
                            + tableId.name() + "`");
        }

        /** Executes a single-column {@code SELECT count()} (or similar) and returns the value. */
        static long countSingleLong(Connection conn, String sql) throws SQLException {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                if (!rs.next()) {
                    return 0L;
                }
                return rs.getLong(1);
            }
        }

        /** Returns the first row of {@code sql} as a column-name to value map. */
        static Map<String, Object> queryFirst(Connection conn, String sql) throws SQLException {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                Map<String, Object> row = new LinkedHashMap<>();
                if (!rs.next()) {
                    return row;
                }
                int cols = rs.getMetaData().getColumnCount();
                for (int i = 1; i <= cols; i++) {
                    row.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
                }
                return row;
            }
        }

        /** Reads {@code system.columns} for the supplied table into an ordered name → type map. */
        static Map<String, String> describeColumns(Connection conn, TableIdentifier tableId)
                throws SQLException {
            Map<String, String> columns = new LinkedHashMap<>();
            String sql = "SELECT name, type FROM system.columns WHERE database = ? AND table = ? "
                    + "ORDER BY position";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, tableId.namespace());
                ps.setString(2, tableId.name());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        columns.put(rs.getString(1), rs.getString(2));
                    }
                }
            }
            return columns;
        }

        /** Returns whether {@code tableId} exists in {@code system.tables}. */
        static boolean tableExists(Connection conn, TableIdentifier tableId) throws SQLException {
            String sql = "SELECT count() FROM system.tables WHERE database = ? AND name = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, tableId.namespace());
                ps.setString(2, tableId.name());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return false;
                    }
                    return rs.getLong(1) > 0;
                }
            }
        }
    }
}
