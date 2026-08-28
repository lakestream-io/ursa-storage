/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

/**
 * Unit tests for {@link ClickHouseTableSchemaService} that exercise the JDBC
 * code paths through a Mockito-backed {@link Connection} with a small
 * in-memory state model.
 */
class ClickHouseTableSchemaServiceTest {

    private FakeClickHouseDb db;
    private Connection connection;
    private TableIdentifier tableId;

    @BeforeEach
    void setUp() throws Exception {
        db = new FakeClickHouseDb();
        connection = db.connection();
        tableId = new TableIdentifier("analytics", "events");
    }

    @Test
    void evolveCreatesTableWhenAbsent() throws Exception {
        ClickHouseTableSchemaService service = newService();

        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false)),
                List.of("id"),
                ClickHouseTableEngine.MERGE_TREE);

        Set<Long> applied = service.evolveTableSchema(sortedMap(1L, v1));

        assertThat(applied).containsExactly(1L);
        assertThat(db.executedDdl()).anyMatch(
                sql -> sql.startsWith("CREATE TABLE IF NOT EXISTS `analytics`.`events`")
                        && sql.contains("ENGINE = MergeTree"));
        assertThat(db.persistedVersions("analytics/events")).containsExactly(1L);
    }

    @Test
    void evolveAddsColumns() throws Exception {
        ClickHouseTableSchemaService service = newService();

        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        ClickHouseSchema v2 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false),
                        new ClickHouseColumn("b", "String", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);

        Set<Long> applied = service.evolveTableSchema(sortedMap(1L, v1, 2L, v2));

        assertThat(applied).containsExactlyInAnyOrder(1L, 2L);
        assertThat(db.executedDdl()).anyMatch(sql -> sql.contains("ADD COLUMN IF NOT EXISTS `b`"));
        assertThat(db.persistedVersions("analytics/events")).containsExactly(1L, 2L);
    }

    @Test
    void evolveRejectsDropColumn() {
        ClickHouseTableSchemaService service = newService();
        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false),
                        new ClickHouseColumn("b", "String", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        ClickHouseSchema v2 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);

        assertThatThrownBy(() -> service.evolveTableSchema(sortedMap(1L, v1, 2L, v2)))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode())
                            .isEqualTo(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE);
                    assertThat(me.getMessage()).contains("drop-column");
                });
    }

    @Test
    void evolveRejectsNarrowingTypeChange() {
        ClickHouseTableSchemaService service = newService();
        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        ClickHouseSchema v2 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int32", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);

        assertThatThrownBy(() -> service.evolveTableSchema(sortedMap(1L, v1, 2L, v2)))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode())
                            .isEqualTo(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE);
                    assertThat(me.getMessage()).contains("type-change");
                });
    }

    @Test
    void evolveRejectsWideningTypeChange() {
        // Widening is currently also rejected — see ClickHouseTableSchemaService.diff() docstring.
        ClickHouseTableSchemaService service = newService();
        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int32", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        ClickHouseSchema v2 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);

        assertThatThrownBy(() -> service.evolveTableSchema(sortedMap(1L, v1, 2L, v2)))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> assertThat(((MaterializationException) e).getExceptionCode())
                        .isEqualTo(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE));
    }

    @Test
    void getLatestSchemaVersionReadsFromVersionsTable() throws Exception {
        ClickHouseTableSchemaService service = newService();
        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false)),
                List.of("id"),
                ClickHouseTableEngine.MERGE_TREE);
        service.evolveTableSchema(sortedMap(1L, v1));

        assertThat(service.getLatestSchemaVersion()).isEqualTo(1L);
    }

    @Test
    void getLatestSchemaVersionReturnsMinusOneBeforeAnyEvolution() throws Exception {
        ClickHouseTableSchemaService service = newService();
        assertThat(service.getLatestSchemaVersion()).isEqualTo(-1L);
    }

    @Test
    void getTableSchemaRoundTripsJsonBlob() throws Exception {
        ClickHouseTableSchemaService service = newService();
        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("name", "Nullable(String)", true)),
                List.of("id"),
                ClickHouseTableEngine.REPLACING_MERGE_TREE);
        service.evolveTableSchema(sortedMap(1L, v1));

        ClickHouseSchema loaded = service.getTableSchema(1L);

        assertThat(loaded).isNotNull();
        assertThat(loaded.engine()).isEqualTo(ClickHouseTableEngine.REPLACING_MERGE_TREE);
        assertThat(loaded.primaryKey()).containsExactly("id");
        assertThat(loaded.columns()).hasSize(2);
        assertThat(loaded.columns().get(0).name()).isEqualTo("id");
        assertThat(loaded.columns().get(0).type()).isEqualTo("Int64");
        assertThat(loaded.columns().get(0).nullable()).isFalse();
        assertThat(loaded.columns().get(1).name()).isEqualTo("name");
        assertThat(loaded.columns().get(1).type()).isEqualTo("Nullable(String)");
        assertThat(loaded.columns().get(1).nullable()).isTrue();
    }

    @Test
    void getTableSchemaReturnsNullForUnknownVersion() throws Exception {
        ClickHouseTableSchemaService service = newService();
        // No evolution issued yet — query just shouldn't NPE.
        assertThat(service.getTableSchema(999L)).isNull();
    }

    @Test
    void evolveSkipsAlreadyPersistedVersions() throws Exception {
        ClickHouseTableSchemaService service = newService();
        ClickHouseSchema v1 = new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE);
        service.evolveTableSchema(sortedMap(1L, v1));
        db.clearDdlLog();

        // Re-applying v1 should be a no-op for the destination table — no CREATE TABLE for the
        // events table, no ALTER. The metadata table CREATE-IF-NOT-EXISTS still re-runs (idempotent
        // on the ClickHouse side), but no destination-table mutation should occur.
        Set<Long> applied = service.evolveTableSchema(sortedMap(1L, v1));

        assertThat(applied).isEmpty();
        assertThat(db.executedDdl())
                .noneMatch(s -> s.contains("`analytics`.`events`"))
                .noneMatch(s -> s.contains("ADD COLUMN"));
    }

    @Test
    void evolveEmptyMapReturnsEmptySet() throws Exception {
        ClickHouseTableSchemaService service = newService();
        Set<Long> applied = service.evolveTableSchema(new TreeMap<>());
        assertThat(applied).isEmpty();
    }

    @Test
    void ensureColumnsCreatesTableWhenAbsent() throws Exception {
        ClickHouseTableSchemaService service = newService();

        service.ensureColumns(new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false)),
                List.of("id"),
                ClickHouseTableEngine.MERGE_TREE));

        assertThat(db.executedDdl()).anyMatch(
                s -> s.startsWith("CREATE TABLE IF NOT EXISTS `analytics`.`events`"));
    }

    @Test
    void ensureColumnsAddsNewColumnRegardlessOfPersistedVersions() throws Exception {
        // Regression: a prior task created the table (column a) and persisted version 1. A later batch
        // introduces additionalLong1. The old version-gated path (evolveTableSchema) would compute a
        // synthetic version <= the persisted latest and SKIP the ALTER, so the INSERT that references
        // additionalLong1 failed with UNKNOWN_IDENTIFIER. ensureColumns must ALTER it in regardless of
        // any persisted version, driven purely by the live-table diff.
        ClickHouseTableSchemaService service = newService();
        service.evolveTableSchema(sortedMap(1L, new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE)));
        db.clearDdlLog();

        service.ensureColumns(new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false),
                        new ClickHouseColumn("additionalLong1", "Nullable(Int64)", true)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE));

        assertThat(db.executedDdl())
                .anyMatch(s -> s.contains("ADD COLUMN IF NOT EXISTS `additionalLong1`"));
    }

    @Test
    void ensureColumnsIsDropTolerant() throws Exception {
        // The row-shape target is a subset of the live table when a batch omits an optional field.
        // ensureColumns must not treat the absent column as a drop (no throw, no mutation).
        ClickHouseTableSchemaService service = newService();
        service.evolveTableSchema(sortedMap(1L, new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false),
                        new ClickHouseColumn("b", "String", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE)));
        db.clearDdlLog();

        service.ensureColumns(new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE));

        assertThat(db.executedDdl())
                .noneMatch(s -> s.contains("ADD COLUMN"))
                .noneMatch(s -> s.toUpperCase(java.util.Locale.ROOT).contains("DROP"));
    }

    @Test
    void ensureColumnsRejectsTypeChange() throws Exception {
        ClickHouseTableSchemaService service = newService();
        service.evolveTableSchema(sortedMap(1L, new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int64", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE)));

        assertThatThrownBy(() -> service.ensureColumns(new ClickHouseSchema(
                List.of(new ClickHouseColumn("a", "Int32", false)),
                List.of(),
                ClickHouseTableEngine.MERGE_TREE)))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode())
                            .isEqualTo(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE);
                    assertThat(me.getMessage()).contains("type-change");
                });
    }

    @Test
    void serializeAndDeserializeRoundTrip() {
        ClickHouseSchema schema = new ClickHouseSchema(
                List.of(new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("payload", "Map(String, String)", false)),
                List.of("id"),
                ClickHouseTableEngine.REPLACING_MERGE_TREE);

        String json = ClickHouseTableSchemaService.serialize(schema);
        ClickHouseSchema restored = ClickHouseTableSchemaService.deserialize(json);

        assertThat(restored).isEqualTo(schema);
    }

    @Test
    void evolutionPolicyIsClickHouseStrict() {
        ClickHouseTableSchemaService service = newService();
        assertThat(service.evolutionPolicy().addColumn()).contains(Boolean.TRUE);
        assertThat(service.evolutionPolicy().dropColumn()).contains(Boolean.FALSE);
        assertThat(service.evolutionPolicy().widenType()).contains(Boolean.FALSE);
    }

    private ClickHouseTableSchemaService newService() {
        return new ClickHouseTableSchemaService(
                connection, tableId, ClickHouseTableEngine.MERGE_TREE, List.of("id"),
                "analytics/events");
    }

    private static TreeMap<Long, ClickHouseSchema> sortedMap(Object... pairs) {
        TreeMap<Long, ClickHouseSchema> map = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((Long) pairs[i], (ClickHouseSchema) pairs[i + 1]);
        }
        return map;
    }

    /**
     * A tiny in-memory ClickHouse stand-in. Only models the subset of the JDBC
     * surface that {@link ClickHouseTableSchemaService} touches:
     * <ul>
     *   <li>{@code CREATE TABLE IF NOT EXISTS …} (just registers the table /
     *       initial column set).</li>
     *   <li>{@code ALTER TABLE … ADD COLUMN IF NOT EXISTS …} (mutates the
     *       column list).</li>
     *   <li>{@code INSERT INTO _ursa_schema_versions …} (records persisted
     *       schema blobs).</li>
     *   <li>{@code SELECT … FROM system.tables} (existence probe).</li>
     *   <li>{@code SELECT name, type FROM system.columns} (describe).</li>
     *   <li>{@code SELECT max(version) / schema_json FROM _ursa_schema_versions}
     *       (version lookups).</li>
     *   <li>{@code SELECT currentDatabase()}.</li>
     * </ul>
     */
    private static final class FakeClickHouseDb {

        /** Live tables keyed by {@code database.table}. */
        private final Map<String, LinkedHashMap<String, ColumnSpec>> tables = new LinkedHashMap<>();
        /**
         * Persisted {@code _ursa_schema_versions} rows keyed by stream_id.
         * Each row is {@code (version, schema_json)}.
         */
        private final Map<String, List<VersionRow>> versions = new LinkedHashMap<>();
        private final List<String> ddlLog = new ArrayList<>();

        Connection connection() throws Exception {
            Connection conn = mock(Connection.class);
            when(conn.prepareStatement(anyString())).thenAnswer(
                    (InvocationOnMock inv) -> preparedStatement(inv.getArgument(0)));
            when(conn.createStatement()).thenAnswer((InvocationOnMock inv) -> statement());
            return conn;
        }

        List<String> executedDdl() {
            return List.copyOf(ddlLog);
        }

        void clearDdlLog() {
            ddlLog.clear();
        }

        List<Long> persistedVersions(String streamId) {
            List<Long> out = new ArrayList<>();
            for (VersionRow row : versions.getOrDefault(streamId, List.of())) {
                out.add(row.version);
            }
            return out;
        }

        private Statement statement() throws Exception {
            Statement st = mock(Statement.class, RETURNS_DEEP_STUBS);
            when(st.execute(anyString())).thenAnswer((InvocationOnMock inv) -> {
                handleDdl(inv.getArgument(0));
                return false;
            });
            when(st.executeQuery(anyString())).thenAnswer((InvocationOnMock inv) -> {
                String sql = inv.getArgument(0);
                if (sql.toLowerCase().contains("currentdatabase")) {
                    return resultSetOf(List.of(List.of((Object) "default")));
                }
                throw new UnsupportedOperationException("Unhandled SQL via Statement: " + sql);
            });
            return st;
        }

        private PreparedStatement preparedStatement(String sql) throws Exception {
            PreparedStatement ps = mock(PreparedStatement.class);
            ParameterBindings bindings = new ParameterBindings();
            when(ps.executeUpdate()).thenAnswer((InvocationOnMock inv) -> {
                handleUpdate(sql, bindings);
                return 1;
            });
            when(ps.execute()).thenAnswer((InvocationOnMock inv) -> {
                handleUpdate(sql, bindings);
                return false;
            });
            when(ps.executeQuery())
                    .thenAnswer((InvocationOnMock inv) -> handleQuery(sql, bindings));
            // Capture string / long parameter bindings via Answer.
            when(ps.toString()).thenReturn(sql);
            org.mockito.Mockito.doAnswer((InvocationOnMock inv) -> {
                bindings.put(inv.getArgument(0), inv.getArgument(1));
                return null;
            }).when(ps).setString(anyInt(), anyString());
            org.mockito.Mockito.doAnswer((InvocationOnMock inv) -> {
                bindings.put(inv.getArgument(0), inv.getArgument(1));
                return null;
            }).when(ps).setLong(anyInt(), anyLong());
            return ps;
        }

        private void handleDdl(String sql) {
            ddlLog.add(sql);
            String trimmed = sql.trim();
            if (trimmed.toUpperCase().startsWith("CREATE TABLE")) {
                handleCreateTable(trimmed);
            } else if (trimmed.toUpperCase().startsWith("ALTER TABLE")) {
                handleAlterAddColumn(trimmed);
            } else {
                throw new UnsupportedOperationException("Unhandled DDL: " + sql);
            }
        }

        private void handleUpdate(String sql, ParameterBindings bindings) {
            ddlLog.add(sql);
            String upper = sql.toUpperCase();
            if (upper.startsWith("INSERT INTO `_URSA_SCHEMA_VERSIONS`")) {
                String streamId = (String) bindings.get(1);
                Long version = (Long) bindings.get(2);
                String json = (String) bindings.get(3);
                versions.computeIfAbsent(streamId, k -> new ArrayList<>())
                        .add(new VersionRow(version, json));
            } else {
                throw new UnsupportedOperationException("Unhandled update: " + sql);
            }
        }

        private ResultSet handleQuery(String sql, ParameterBindings bindings) {
            String upper = sql.toUpperCase();
            if (upper.contains("FROM SYSTEM.TABLES")) {
                String database = (String) bindings.get(1);
                String table = (String) bindings.get(2);
                long count = tables.containsKey(database + "." + table) ? 1L : 0L;
                return resultSetOf(List.of(List.of((Object) count)));
            }
            if (upper.contains("FROM SYSTEM.COLUMNS")) {
                String database = (String) bindings.get(1);
                String table = (String) bindings.get(2);
                LinkedHashMap<String, ColumnSpec> cols = tables.get(database + "." + table);
                if (cols == null) {
                    return resultSetOf(List.of());
                }
                List<List<Object>> rows = new ArrayList<>();
                for (ColumnSpec spec : cols.values()) {
                    rows.add(List.of(spec.name, spec.type));
                }
                return resultSetOf(rows);
            }
            if (upper.contains("MAX(VERSION) FROM `_URSA_SCHEMA_VERSIONS`")) {
                String streamId = (String) bindings.get(1);
                List<VersionRow> rows = versions.getOrDefault(streamId, List.of());
                if (rows.isEmpty()) {
                    return resultSetOf(List.of(nullableRow((Object) null)));
                }
                long max = rows.stream().mapToLong(r -> r.version).max().orElse(-1L);
                return resultSetOf(List.of(List.of((Object) max)));
            }
            if (upper.contains("SCHEMA_JSON FROM `_URSA_SCHEMA_VERSIONS`")) {
                String streamId = (String) bindings.get(1);
                Long version = (Long) bindings.get(2);
                List<VersionRow> rows = versions.getOrDefault(streamId, List.of());
                for (VersionRow row : rows) {
                    if (row.version.equals(version)) {
                        return resultSetOf(List.of(List.of((Object) row.json)));
                    }
                }
                return resultSetOf(List.of());
            }
            throw new UnsupportedOperationException("Unhandled query: " + sql);
        }

        private void handleCreateTable(String sql) {
            // Two shapes:
            //   CREATE TABLE IF NOT EXISTS `db`.`name` ( ... ) ENGINE = ...
            //   CREATE TABLE IF NOT EXISTS `name` ( ... ) ENGINE = ...   (no db prefix)
            String marker = "CREATE TABLE IF NOT EXISTS";
            int idStart = sql.toUpperCase().indexOf(marker) + marker.length();
            String rest = sql.substring(idStart).trim();
            // rest starts with the first backtick-quoted identifier.
            int firstOpen = rest.indexOf('`');
            int firstClose = rest.indexOf('`', firstOpen + 1);
            String firstIdent = rest.substring(firstOpen + 1, firstClose);
            String database;
            String table;
            int afterIdent;
            if (firstClose + 1 < rest.length() && rest.charAt(firstClose + 1) == '.') {
                // `db`.`table` ( ... )
                database = firstIdent;
                int secondOpen = rest.indexOf('`', firstClose + 1);
                int secondClose = rest.indexOf('`', secondOpen + 1);
                table = rest.substring(secondOpen + 1, secondClose);
                afterIdent = secondClose;
            } else {
                // `table` ( ... ) — assume default database
                database = "default";
                table = firstIdent;
                afterIdent = firstClose;
            }

            String key = database + "." + table;
            LinkedHashMap<String, ColumnSpec> cols =
                    tables.computeIfAbsent(key, k -> new LinkedHashMap<>());

            int colsStart = rest.indexOf('(', afterIdent) + 1;
            int colsEnd = findMatchingParen(rest, colsStart - 1);
            String inner = rest.substring(colsStart, colsEnd);
            for (String entry : splitTopLevel(inner)) {
                String t = entry.trim();
                if (t.isEmpty()) {
                    continue;
                }
                String name;
                String type;
                if (t.startsWith("`")) {
                    int nameEnd = t.indexOf('`', 1);
                    name = t.substring(1, nameEnd);
                    type = t.substring(nameEnd + 1).trim();
                } else {
                    int space = t.indexOf(' ');
                    name = t.substring(0, space);
                    type = t.substring(space + 1).trim();
                }
                cols.put(name, new ColumnSpec(name, type));
            }
        }

        private void handleAlterAddColumn(String sql) {
            // ALTER TABLE `db`.`name` ADD COLUMN IF NOT EXISTS `c` Type
            int dbStart = sql.indexOf('`') + 1;
            int dbEnd = sql.indexOf('`', dbStart);
            String database = sql.substring(dbStart, dbEnd);
            int tableStart = sql.indexOf('`', dbEnd + 1) + 1;
            int tableEnd = sql.indexOf('`', tableStart);
            String table = sql.substring(tableStart, tableEnd);

            String marker = "IF NOT EXISTS";
            int colStart = sql.indexOf('`', sql.indexOf(marker) + marker.length()) + 1;
            int colEnd = sql.indexOf('`', colStart);
            String name = sql.substring(colStart, colEnd);
            String type = sql.substring(colEnd + 1).trim();

            LinkedHashMap<String, ColumnSpec> cols = tables.get(database + "." + table);
            if (cols == null) {
                throw new IllegalStateException("ALTER on missing table " + database + "." + table);
            }
            cols.putIfAbsent(name, new ColumnSpec(name, type));
        }

        private static int findMatchingParen(String s, int openIdx) {
            int depth = 0;
            for (int i = openIdx; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
            throw new IllegalStateException("Unbalanced parens: " + s);
        }

        private static List<String> splitTopLevel(String s) {
            List<String> out = new ArrayList<>();
            int depth = 0;
            StringBuilder buf = new StringBuilder();
            for (char c : s.toCharArray()) {
                if (c == '(') {
                    depth++;
                    buf.append(c);
                } else if (c == ')') {
                    depth--;
                    buf.append(c);
                } else if (c == ',' && depth == 0) {
                    out.add(buf.toString());
                    buf.setLength(0);
                } else {
                    buf.append(c);
                }
            }
            if (buf.length() > 0) {
                out.add(buf.toString());
            }
            return out;
        }

        private static List<Object> nullableRow(Object value) {
            List<Object> row = new ArrayList<>();
            row.add(value);
            return row;
        }

        private static ResultSet resultSetOf(List<List<Object>> rows) {
            ResultSet rs = mock(ResultSet.class);
            Iterator<List<Object>> it = rows.iterator();
            List<Object>[] currentHolder = new List[] {null};
            try {
                when(rs.next()).thenAnswer((InvocationOnMock inv) -> {
                    if (it.hasNext()) {
                        currentHolder[0] = it.next();
                        return true;
                    }
                    currentHolder[0] = null;
                    return false;
                });
                when(rs.getString(anyInt())).thenAnswer((InvocationOnMock inv) -> {
                    int idx = inv.getArgument(0);
                    if (currentHolder[0] == null) {
                        throw new NoSuchElementException("getString outside row");
                    }
                    Object value = currentHolder[0].get(idx - 1);
                    return value == null ? null : value.toString();
                });
                when(rs.getLong(anyInt())).thenAnswer((InvocationOnMock inv) -> {
                    int idx = inv.getArgument(0);
                    if (currentHolder[0] == null) {
                        throw new NoSuchElementException("getLong outside row");
                    }
                    Object value = currentHolder[0].get(idx - 1);
                    return value == null ? 0L : ((Number) value).longValue();
                });
                when(rs.wasNull()).thenAnswer((InvocationOnMock inv) -> {
                    // Only the max(version) query asks; null-row carries a null element.
                    return currentHolder[0] != null && currentHolder[0].get(0) == null;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return rs;
        }

        private static final class ColumnSpec {
            final String name;
            final String type;

            ColumnSpec(String name, String type) {
                this.name = name;
                this.type = type;
            }
        }

        private static final class VersionRow {
            final Long version;
            final String json;

            VersionRow(Long version, String json) {
                this.version = version;
                this.json = json;
            }
        }

        private static final class ParameterBindings {
            private final Map<Integer, Object> values = new LinkedHashMap<>();

            void put(int idx, Object value) {
                values.put(idx, value);
            }

            Object get(int idx) {
                return values.get(idx);
            }
        }
    }
}
