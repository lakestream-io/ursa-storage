/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import lombok.extern.slf4j.Slf4j;

/**
 * ClickHouse implementation of {@link TableSchemaService} keyed on AVRO source
 * schemas.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>{@link #getLatestSchemaVersion()} consults the
 *       {@code _ursa_schema_versions} table (created on demand) to surface the
 *       highest version currently applied. When the destination table doesn't
 *       exist yet (or no version row has been recorded), returns {@code -1}.</li>
 *   <li>{@link #evolveTableSchema(SortedMap)} walks the supplied versions in
 *       ascending order; the first call issues
 *       {@code CREATE TABLE ... ENGINE = <engine>} from the highest version
 *       supplied and subsequent versions are diff'd against the current table
 *       state. Additions are applied via
 *       {@code ALTER TABLE ... ADD COLUMN IF NOT EXISTS}. Drops and type
 *       changes are rejected with
 *       {@link MaterializationException} carrying
 *       {@link ExceptionCode#MESSAGE_SCHEMA_INCOMPATIBLE}.</li>
 *   <li>{@link #getTableSchema(Long)} reads the JSON blob persisted for that
 *       version from {@code _ursa_schema_versions} and deserialises it back
 *       into a {@link ClickHouseSchema}.</li>
 * </ul>
 *
 * <p>{@code _ursa_schema_versions} schema (created lazily on first use):
 * <pre>
 *   CREATE TABLE IF NOT EXISTS _ursa_schema_versions (
 *       stream_id   String,
 *       version     Int64,
 *       schema_json String
 *   ) ENGINE = ReplacingMergeTree
 *   ORDER BY (stream_id, version);
 * </pre>
 *
 * <p>The service does not own JDBC connection lifecycle: the {@link Connection}
 * passed at construction time is owned by the orchestrator / materializer.
 */
@Slf4j
public final class ClickHouseTableSchemaService implements TableSchemaService<Long, ClickHouseSchema> {

    /** Name of the metadata table used to persist per-version schema blobs. */
    public static final String VERSIONS_TABLE = "_ursa_schema_versions";

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Connection connection;
    private final TableIdentifier tableIdentifier;
    private final ClickHouseTableEngine engine;
    private final List<String> primaryKey;
    private final EvolutionPolicy evolutionPolicy;
    private final String streamId;

    /**
     * @param connection      JDBC connection (caller-owned; not closed here)
     * @param tableIdentifier destination table (namespace + name)
     * @param engine          engine assumed when creating the table
     * @param primaryKey      ordered list of column names that form
     *                        {@code ORDER BY} / {@code PRIMARY KEY}; may be empty
     * @param streamId        identifier persisted into {@code _ursa_schema_versions}
     *                        so multiple streams targeting the metadata table do
     *                        not collide
     */
    public ClickHouseTableSchemaService(Connection connection,
                                        TableIdentifier tableIdentifier,
                                        ClickHouseTableEngine engine,
                                        List<String> primaryKey,
                                        String streamId) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.tableIdentifier = Objects.requireNonNull(tableIdentifier, "tableIdentifier");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.evolutionPolicy = EvolutionPolicy.forClickHouse();
    }

    @Override
    public Set<Long> evolveTableSchema(SortedMap<Long, ClickHouseSchema> schemaWithVersions)
            throws Exception {
        Objects.requireNonNull(schemaWithVersions, "schemaWithVersions");
        Set<Long> applied = new HashSet<>();
        if (schemaWithVersions.isEmpty()) {
            return applied;
        }
        ensureVersionsTable();

        boolean tableExists = tableExists();
        List<Long> versions = new ArrayList<>(schemaWithVersions.keySet());
        versions.sort(Comparator.naturalOrder());

        if (!tableExists) {
            // First-version-wins seeding: the *first* version is used to create the table; the
            // subsequent versions are then diff'd against it via ALTER. This matches the loop
            // semantics described on the TableSchemaService interface (each missing version is
            // evolved in turn) and avoids reaching for the highest version when intermediate
            // versions add columns incrementally.
            Long firstVersion = versions.get(0);
            ClickHouseSchema firstSchema = schemaWithVersions.get(firstVersion);
            createTable(firstSchema);
            persistVersion(firstVersion, firstSchema);
            applied.add(firstVersion);
            tableExists = true;
        }

        // Start from the schema currently in the table (live column set) and evolve forward.
        Map<String, ClickHouseColumn> current = describeColumns();
        long latestPersisted = readLatestVersion();
        for (Long version : versions) {
            if (version <= latestPersisted) {
                // Already applied in a previous run; skip without touching the table.
                continue;
            }
            ClickHouseSchema target = schemaWithVersions.get(version);
            List<ClickHouseColumn> additions = diff(current, target);
            for (ClickHouseColumn added : additions) {
                addColumn(added);
                current.put(added.name(), added);
            }
            persistVersion(version, target);
            latestPersisted = version;
            applied.add(version);
        }
        return applied;
    }

    /**
     * Ensures the live destination table contains every column in {@code desired}: creates the table
     * when absent, otherwise issues {@code ALTER TABLE ... ADD COLUMN IF NOT EXISTS} for any column not
     * already present.
     *
     * <p>Unlike {@link #evolveTableSchema}, this is driven purely by the diff against the LIVE table and
     * carries no source-version bookkeeping. The row-shape-driven materializer builds {@code desired}
     * from whichever fields are non-null in a batch, so the same source schema version can legitimately
     * yield different column sets; version-gated skipping (as in {@code evolveTableSchema}) would then
     * wrongly skip a genuinely new column, and the INSERT that references it fails at the driver with
     * {@code UNKNOWN_IDENTIFIER}. It is also drop-tolerant: columns present in the live table but absent
     * from {@code desired} are left untouched, since the table accumulates the union of every column
     * ever seen and each INSERT binds only the columns present in its own batch. A type change on an
     * existing column is still rejected per the ClickHouse evolution policy.
     *
     * @param desired the column set the current batch needs to INSERT
     */
    public void ensureColumns(ClickHouseSchema desired) throws Exception {
        Objects.requireNonNull(desired, "desired");
        if (!tableExists()) {
            createTable(desired);
            return;
        }
        Map<String, ClickHouseColumn> current = describeColumns();
        for (ClickHouseColumn col : desired.columns()) {
            ClickHouseColumn live = current.get(col.name());
            if (live == null) {
                addColumn(col);
            } else if (!normalise(live.type()).equals(normalise(col.type()))) {
                throw new MaterializationException(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE,
                        "ClickHouse rejects type-change evolution: column '" + col.name()
                                + "' changes from " + live.type() + " to " + col.type()
                                + " for " + tableIdentifier.namespace() + "." + tableIdentifier.name());
            }
        }
    }

    @Override
    public ClickHouseSchema getTableSchema(Long schemaVersion) throws Exception {
        Objects.requireNonNull(schemaVersion, "schemaVersion");
        ensureVersionsTable();
        String sql = "SELECT schema_json FROM `" + VERSIONS_TABLE + "` FINAL "
                + "WHERE stream_id = ? AND version = ? LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, streamId);
            ps.setLong(2, schemaVersion);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                String json = rs.getString(1);
                return deserialize(json);
            }
        } catch (SQLException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_READ_ERROR,
                    "Failed to read schema version " + schemaVersion + " for stream " + streamId
                            + " from " + VERSIONS_TABLE + ": " + e.getMessage(),
                    e);
        }
    }

    @Override
    public Long getLatestSchemaVersion() throws Exception {
        if (!versionsTableExists()) {
            return -1L;
        }
        if (!tableExists()) {
            return -1L;
        }
        return readLatestVersion();
    }

    /** Returns the policy this service enforces (visible for tests). */
    public EvolutionPolicy evolutionPolicy() {
        return evolutionPolicy;
    }

    // ----- Internals -----

    /** Diffs the live column map against the target schema, returning columns to add. */
    private List<ClickHouseColumn> diff(Map<String, ClickHouseColumn> current,
                                        ClickHouseSchema target) {
        // Pass 1: detect drops. Compare lowercased names so the diff is case-insensitive (ClickHouse
        // identifiers are case-sensitive but practical column maps are often canonicalised).
        Set<String> targetNames = new HashSet<>();
        for (ClickHouseColumn c : target.columns()) {
            targetNames.add(c.name());
        }
        for (String existing : current.keySet()) {
            if (existing.startsWith("_")) {
                // Skip ClickHouse-internal / Ursa-internal columns (e.g. _ingested_at).
                continue;
            }
            if (!targetNames.contains(existing)) {
                if (!evolutionPolicy.dropColumn().orElse(false)) {
                    throw new MaterializationException(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE,
                            "ClickHouse rejects drop-column evolution: column '" + existing
                                    + "' is present in the live table but missing from the target "
                                    + "schema for " + tableIdentifier.namespace() + "."
                                    + tableIdentifier.name());
                }
            }
        }

        // Pass 2: detect type changes and accumulate additions.
        List<ClickHouseColumn> additions = new ArrayList<>();
        for (ClickHouseColumn col : target.columns()) {
            ClickHouseColumn live = current.get(col.name());
            if (live == null) {
                additions.add(col);
                continue;
            }
            if (!normalise(live.type()).equals(normalise(col.type()))) {
                // ClickHouse evolution policy is strict: any type change (narrow or widen) is
                // rejected. Future enhancement could permit widening via ALTER MODIFY COLUMN.
                throw new MaterializationException(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE,
                        "ClickHouse rejects type-change evolution: column '" + col.name()
                                + "' changes from " + live.type() + " to " + col.type()
                                + " in target schema for " + tableIdentifier.namespace() + "."
                                + tableIdentifier.name());
            }
        }
        return additions;
    }

    /** Canonical type-string form for the diff: case-insensitive, whitespace-stripped. */
    private static String normalise(String type) {
        return type.replaceAll("\\s+", "").toLowerCase(Locale.ROOT);
    }

    /** Executes {@code CREATE TABLE IF NOT EXISTS …} for the destination. */
    private void createTable(ClickHouseSchema schema) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ")
                .append(ClickHouseIdentifiers.quote(tableIdentifier.namespace())).append('.')
                .append(ClickHouseIdentifiers.quote(tableIdentifier.name())).append(" (");
        boolean first = true;
        for (ClickHouseColumn col : schema.columns()) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(ClickHouseIdentifiers.quote(col.name())).append(' ')
                    .append(ClickHouseIdentifiers.validateType(col.type()));
            first = false;
        }
        sql.append(") ENGINE = ").append(engineKeyword(engine));
        if (!schema.primaryKey().isEmpty()) {
            sql.append(" ORDER BY (");
            for (int i = 0; i < schema.primaryKey().size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(ClickHouseIdentifiers.quote(schema.primaryKey().get(i)));
            }
            sql.append(')');
        } else if (engine == ClickHouseTableEngine.MERGE_TREE) {
            // ClickHouse MergeTree requires an ORDER BY; default to tuple() for keyless tables.
            sql.append(" ORDER BY tuple()");
        }
        executeUpdate(sql.toString(), "CREATE TABLE");
    }

    /** Executes a single {@code ALTER TABLE ... ADD COLUMN IF NOT EXISTS …}. */
    private void addColumn(ClickHouseColumn col) {
        String sql = "ALTER TABLE " + ClickHouseIdentifiers.quote(tableIdentifier.namespace()) + "."
                + ClickHouseIdentifiers.quote(tableIdentifier.name()) + " ADD COLUMN IF NOT EXISTS "
                + ClickHouseIdentifiers.quote(col.name()) + " " + ClickHouseIdentifiers.validateType(col.type());
        executeUpdate(sql, "ALTER TABLE ADD COLUMN");
    }

    /** Writes the {@code (stream_id, version, schema_json)} row to the metadata table. */
    private void persistVersion(Long version, ClickHouseSchema schema) {
        String json = serialize(schema);
        String sql = "INSERT INTO `" + VERSIONS_TABLE
                + "` (stream_id, version, schema_json) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, streamId);
            ps.setLong(2, version);
            ps.setString(3, json);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_WRITE_ERROR,
                    "Failed to persist schema version " + version + " for stream " + streamId
                            + " into " + VERSIONS_TABLE + ": " + e.getMessage(),
                    e);
        }
    }

    /** Idempotently creates {@link #VERSIONS_TABLE}. */
    private void ensureVersionsTable() {
        String sql = "CREATE TABLE IF NOT EXISTS `" + VERSIONS_TABLE + "` ("
                + "stream_id String, version Int64, schema_json String"
                + ") ENGINE = ReplacingMergeTree ORDER BY (stream_id, version)";
        executeUpdate(sql, "CREATE _ursa_schema_versions");
    }

    /** {@code SELECT count() …} on system.tables to test for existence. */
    private boolean tableExists() {
        return existsInSystemTables(tableIdentifier.namespace(), tableIdentifier.name());
    }

    /** Whether the metadata table itself has been created in the current database. */
    private boolean versionsTableExists() {
        return existsInSystemTables(currentDatabase(), VERSIONS_TABLE);
    }

    private boolean existsInSystemTables(String database, String name) {
        String sql = "SELECT count() FROM system.tables WHERE database = ? AND name = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, database);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return false;
                }
                return rs.getLong(1) > 0;
            }
        } catch (SQLException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_READ_ERROR,
                    "Failed to query system.tables for " + database + "." + name + ": "
                            + e.getMessage(),
                    e);
        }
    }

    /** Reads the live column set from {@code system.columns} into an ordered map. */
    private Map<String, ClickHouseColumn> describeColumns() {
        Map<String, ClickHouseColumn> columns = new LinkedHashMap<>();
        if (!tableExists()) {
            return columns;
        }
        String sql = "SELECT name, type FROM system.columns WHERE database = ? AND table = ? "
                + "ORDER BY position";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableIdentifier.namespace());
            ps.setString(2, tableIdentifier.name());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    String type = rs.getString(2);
                    if (name == null || type == null) {
                        // Defensive: system.columns rows should always have non-null name/type,
                        // but a misbehaving driver could surface nulls; skip rather than NPE.
                        continue;
                    }
                    boolean nullable = type.startsWith("Nullable(");
                    columns.put(name, new ClickHouseColumn(name, type, nullable));
                }
            }
        } catch (SQLException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_READ_ERROR,
                    "Failed to describe ClickHouse columns for " + tableIdentifier.namespace()
                            + "." + tableIdentifier.name() + ": " + e.getMessage(),
                    e);
        }
        return columns;
    }

    /**
     * Returns the highest persisted version for {@link #streamId} or {@code -1}
     * when no rows exist. The query runs against {@link #VERSIONS_TABLE} with a
     * {@code FINAL} qualifier so {@code ReplacingMergeTree} merges duplicate
     * inserts.
     */
    private long readLatestVersion() {
        String sql = "SELECT max(version) FROM `" + VERSIONS_TABLE + "` FINAL "
                + "WHERE stream_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, streamId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return -1L;
                }
                long max = rs.getLong(1);
                if (rs.wasNull()) {
                    return -1L;
                }
                return max;
            }
        } catch (SQLException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_READ_ERROR,
                    "Failed to read latest schema version for stream " + streamId + " from "
                            + VERSIONS_TABLE + ": " + e.getMessage(),
                    e);
        }
    }

    /** {@code SELECT currentDatabase()} — used to scope {@code system.tables} lookups. */
    private String currentDatabase() {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT currentDatabase()")) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return "default";
        } catch (SQLException e) {
            // Non-fatal: fall back to "default" so the existence probe still runs.
            log.debug("Could not resolve currentDatabase(); falling back to 'default'", e);
            return "default";
        }
    }

    private static String engineKeyword(ClickHouseTableEngine engine) {
        return switch (engine) {
            case MERGE_TREE -> "MergeTree";
            case REPLACING_MERGE_TREE -> "ReplacingMergeTree";
        };
    }

    private void executeUpdate(String sql, String label) {
        log.debug("ClickHouse {} SQL: {}", label, sql);
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_WRITE_ERROR,
                    label + " failed for " + tableIdentifier.namespace() + "."
                            + tableIdentifier.name() + ": " + e.getMessage(),
                    e);
        }
    }

    /**
     * Serialises a {@link ClickHouseSchema} into the JSON blob persisted in
     * {@link #VERSIONS_TABLE}. The serialised shape is deliberately compact:
     * {@code {"engine":..., "primaryKey":[...], "columns":[{"name":..,"type":..,"nullable":..}]}}
     */
    static String serialize(ClickHouseSchema schema) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("engine", schema.engine().name());
        root.put("primaryKey", schema.primaryKey());
        List<Map<String, Object>> cols = new ArrayList<>();
        for (ClickHouseColumn col : schema.columns()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", col.name());
            entry.put("type", col.type());
            entry.put("nullable", col.nullable());
            cols.add(entry);
        }
        root.put("columns", cols);
        try {
            return JSON.writeValueAsString(root);
        } catch (IOException e) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "Failed to serialise ClickHouseSchema: " + e.getMessage(), e);
        }
    }

    /** Reverse of {@link #serialize(ClickHouseSchema)}. */
    static ClickHouseSchema deserialize(String json) {
        try {
            Map<String, Object> root = JSON.readValue(json, new TypeReference<>() { });
            ClickHouseTableEngine engine = ClickHouseTableEngine.valueOf(
                    (String) root.getOrDefault("engine", ClickHouseTableEngine.MERGE_TREE.name()));
            @SuppressWarnings("unchecked")
            List<String> primaryKey = (List<String>) root.getOrDefault("primaryKey", List.of());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> cols =
                    (List<Map<String, Object>>) root.getOrDefault("columns", List.of());
            List<ClickHouseColumn> columns = new ArrayList<>();
            for (Map<String, Object> entry : cols) {
                columns.add(new ClickHouseColumn(
                        (String) entry.get("name"),
                        (String) entry.get("type"),
                        Boolean.TRUE.equals(entry.get("nullable"))));
            }
            return new ClickHouseSchema(columns, primaryKey, engine);
        } catch (IOException e) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "Failed to deserialise ClickHouseSchema: " + e.getMessage(), e);
        }
    }

}
