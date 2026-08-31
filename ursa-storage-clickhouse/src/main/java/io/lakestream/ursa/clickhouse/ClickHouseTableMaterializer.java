/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.CommitResult;
import io.lakestream.ursa.materialization.MaterializationContext;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.MissingSchemaVersionTracker;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.util.EntryUtils;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link TableMaterializer} that writes buffered rows into a ClickHouse table
 * over a JDBC {@link Connection}.
 *
 * <p>Idempotency depends on the destination engine, chosen by
 * {@link ClickHouseTableEngine#forPolicy(io.lakestream.api.materialization.TableMaterializationPolicy)}:
 * <ul>
 *   <li>{@code ReplacingMergeTree} ordered by the policy's primary key, with an
 *       implicit {@code _ingested_at DateTime DEFAULT now()} version column,
 *       gives upsert semantics with at-least-once retries.</li>
 *   <li>{@code MergeTree} is plain append-only; the orchestrator's retry path
 *       provides at-least-once delivery without dedup.</li>
 * </ul>
 *
 * <p>DDL (table create / alter) is owned by
 * {@link ClickHouseTableSchemaService}; the materializer assumes the target
 * table already exists at the time of {@link #write} and surfaces a
 * {@link MaterializationException} with
 * {@link ExceptionCode#LAKEHOUSE_WRITE_ERROR} when the INSERT fails. The
 * orchestrator's retry/error-mode policy decides how the failure propagates
 * upstream.
 *
 * <p>Row decoding has two paths: when {@link MaterializationContext#sourceSchemaVersion()}
 * is set <em>and</em> a {@link ClickHouseTableSchemaService} was provided at
 * construction, the persisted {@link ClickHouseSchema} drives the column
 * mapping (missing fields land as nulls, dotted column names resolve nested
 * JSON paths). Otherwise the payload is parsed as a flat JSON object — useful
 * for unversioned / raw streams.
 */
@Slf4j
public final class ClickHouseTableMaterializer implements TableMaterializer<GenericEntry> {

    /** Default batch size when no policy override is supplied. */
    public static final int DEFAULT_BATCH_SIZE = 1_000;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Connection connection;
    private final TableIdentifier tableIdentifier;
    private final ClickHouseTableEngine engine;
    private final List<String> primaryKey;
    private final int batchSize;
    private final EvolutionPolicy supportedEvolutions;
    @Nullable
    private final ClickHouseTableSchemaService schemaService;
    /**
     * Schema-aware source decoder. When set, {@link #write} decodes each message via the framework
     * encoder (topic-schema driven Kafka data). When null, the JSON fallback is used.
     */
    @Nullable
    private final EntryEncoder<Map<String, Object>> rowEncoder;
    @Nullable
    private final String sourceTopic;
    private final List<Map<String, Object>> buffer = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean committed = new AtomicBoolean(false);
    private long totalRecords;
    private long totalBytes;
    /**
     * Columns already created/evolved in the destination table. The schema-aware path derives the
     * table from the decoded row shape (see {@link #ensureDestinationTable}); this tracks what has
     * been materialised so a repeated flush with the same columns skips the DDL round-trip.
     */
    private final Set<String> ensuredColumns = new LinkedHashSet<>();

    /**
     * Package-private constructor so the factory can inject a stub
     * {@link Connection} in tests without going through
     * {@link ClickHouseConnectionFactory}.
     *
     * <p>Delegates to {@link #ClickHouseTableMaterializer(Connection,
     * TableIdentifier, ClickHouseTableEngine, List, int,
     * ClickHouseTableSchemaService)} with a {@code null} schema service so the
     * JSON fallback decoder is used. T11 tests and callers that don't yet
     * provide a schema service continue to use this overload.
     */
    ClickHouseTableMaterializer(Connection connection,
                                TableIdentifier tableIdentifier,
                                ClickHouseTableEngine engine,
                                List<String> primaryKey,
                                int batchSize) {
        this(connection, tableIdentifier, engine, primaryKey, batchSize, null);
    }

    /**
     * Full constructor accepting an optional {@link ClickHouseTableSchemaService}.
     *
     * <p>When {@code schemaService} is non-null and the per-record
     * {@link MaterializationContext#sourceSchemaVersion() source schema
     * version} is set, {@link #decodeRow(GenericEntry, MaterializationContext)}
     * uses the persisted {@link ClickHouseSchema} to align AVRO record fields
     * with ClickHouse columns. When either is absent, the JSON fallback path
     * (T11 placeholder) handles the row.
     */
    ClickHouseTableMaterializer(Connection connection,
                                TableIdentifier tableIdentifier,
                                ClickHouseTableEngine engine,
                                List<String> primaryKey,
                                int batchSize,
                                @Nullable ClickHouseTableSchemaService schemaService) {
        this(connection, tableIdentifier, engine, primaryKey, batchSize, schemaService, null, null);
    }

    /**
     * Fullest constructor: adds the schema-aware source decoder. When {@code rowEncoder} is
     * non-null, {@link #write} routes each entry through the framework encoder
     * ({@code sourceTopic} is the topic the encoder resolves the schema for). When null, the JSON
     * fallback decoder is used.
     */
    @SuppressWarnings("ParameterNumber")
    ClickHouseTableMaterializer(Connection connection,
                                TableIdentifier tableIdentifier,
                                ClickHouseTableEngine engine,
                                List<String> primaryKey,
                                int batchSize,
                                @Nullable ClickHouseTableSchemaService schemaService,
                                @Nullable EntryEncoder<Map<String, Object>> rowEncoder,
                                @Nullable String sourceTopic) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.tableIdentifier = Objects.requireNonNull(tableIdentifier, "tableIdentifier");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0, got " + batchSize);
        }
        this.batchSize = batchSize;
        this.supportedEvolutions = EvolutionPolicy.forClickHouse();
        this.schemaService = schemaService;
        this.rowEncoder = rowEncoder;
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void write(GenericEntry record, MaterializationContext context) {
        Objects.requireNonNull(record, "record");
        if (context == null) {
            release(record);
            throw new NullPointerException("context");
        }
        if (closed.get()) {
            try {
                throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                        "write() after close() is not allowed");
            } finally {
                release(record);
            }
        }
        if (committed.get()) {
            try {
                throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                        "write() after commit() is not allowed");
            } finally {
                release(record);
            }
        }
        // Account for the storage-frame bytes before handing the entry to a schema-aware encoder,
        // which owns and releases its input payload.
        totalBytes += estimateBytes(record);
        // A storage entry may carry multiple Kafka records.
        for (Map<String, Object> row : decodeRows(record, context)) {
            if (row.isEmpty()) {
                continue;
            }
            buffer.add(row);
            totalRecords++;
            if (buffer.size() >= batchSize) {
                flush();
            }
        }
    }

    /**
     * Decodes a {@link GenericEntry} into one ClickHouse row per contained Kafka record.
     *
     * <p>When a {@link #rowEncoder} is wired, decoding goes through the framework encoder, which
     * uses the topic's registered schema to decode Kafka records (Avro/JSON/...). Otherwise the JSON
     * fallback parses the native Kafka MemoryRecords payload via
     * {@link EntryUtils#entryToKafkaMessage}.
     */
    private List<Map<String, Object>> decodeRows(GenericEntry entry, MaterializationContext context) {
        if (rowEncoder != null) {
            return decodeRowsWithEncoder(entry);
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            EntryUtils.entryToKafkaMessage(entry.entry(), message -> {
                byte[] data = message.getData();
                if (data == null || data.length == 0) {
                    return;
                }
                rows.add(decodeBytes(data, context));
            });
        } catch (MaterializationException e) {
            throw e;
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                    "Failed to decode entry for " + tableIdentifier.namespace() + "."
                            + tableIdentifier.name() + ": " + e.getMessage(), e);
        } finally {
            // The orchestrator gives each sink an owned retained duplicate. EntryUtils only reads
            // that duplicate, so the fallback path must consume its reference just like the
            // schema-aware KafkaEntryEncoder does.
            entry.entry().payload().release();
        }
        return rows;
    }

    /**
     * Schema-driven decode through the topic schema.
     */
    private List<Map<String, Object>> decodeRowsWithEncoder(GenericEntry entry) {
        List<Map<String, Object>> rows = new ArrayList<>();
        EntryEncoderContext ctx = EntryEncoderContext.builder()
                .missingSchemaVersionTracker(new MissingSchemaVersionTracker())
                .build();
        try {
            rowEncoder.encode(sourceTopic, entry, new ResultConsumer<MaterializationRecord<Map<String, Object>>>() {
                @Override
                public void onResult(MaterializationRecord<Map<String, Object>> record) {
                    rows.add(record.record());
                }

                @Override
                public void onErrorWithCtx(Object errorContext, Throwable throwable) {
                    try {
                        throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                                "Failed to decode message for " + tableIdentifier.namespace() + "."
                                        + tableIdentifier.name() + ": " + throwable.getMessage(), throwable);
                    } finally {
                        if (errorContext instanceof GenericEntry genericEntry) {
                            genericEntry.entry().payload().release();
                        }
                    }
                }
            }, schemaService, ctx);
        } catch (MaterializationException e) {
            throw e;
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                    "Failed to decode entry for " + tableIdentifier.namespace() + "."
                            + tableIdentifier.name() + ": " + e.getMessage(), e);
        }
        return rows;
    }

    @Override
    public CommitResult commit() {
        if (closed.get()) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "commit() after close() is not allowed");
        }
        if (committed.get()) {
            // Idempotent: T11 chooses to return a zero-record CommitResult so the framework's
            // retry path is a no-op rather than double-flushing the (already empty) buffer.
            return new CommitResult(0L, 0L, Map.of(
                    "clickhouse.engine", engine.name(),
                    "clickhouse.idempotent", "true"));
        }
        if (!buffer.isEmpty()) {
            flush();
        }
        committed.set(true);
        return new CommitResult(totalRecords, totalBytes, Map.of(
                "clickhouse.rows-inserted", Long.toString(totalRecords),
                "clickhouse.engine", engine.name(),
                "clickhouse.table",
                        tableIdentifier.namespace() + "." + tableIdentifier.name()));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Surface as a logged warning rather than throwing — the framework calls close()
                // from finally blocks where a second exception would mask the original failure.
                log.warn("Failed to close ClickHouse JDBC connection for table {}.{}: {}",
                        tableIdentifier.namespace(), tableIdentifier.name(), e.getMessage(), e);
            }
        }
    }

    @Override
    public EvolutionPolicy supportedEvolutions() {
        return supportedEvolutions;
    }

    /** Returns the destination table engine (visible for tests + commit metadata callers). */
    public ClickHouseTableEngine engine() {
        return engine;
    }

    /** Returns the resolved destination table identifier (visible for tests). */
    public TableIdentifier tableIdentifier() {
        return tableIdentifier;
    }

    /**
     * Flushes the in-memory buffer through a single batched
     * {@code INSERT INTO &lt;db&gt;.&lt;table&gt; (&lt;cols&gt;) VALUES (?, ?, ...)}
     * prepared statement. The column order is derived from the union of all
     * row key sets (first-seen wins); subsequent rows in the batch are aligned
     * against the same key order so a missing field becomes a JDBC null
     * binding.
     */
    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        // Union of column names, preserving first-seen order so the generated SQL is stable
        // across flushes that may see new optional fields.
        LinkedHashMap<String, Boolean> columns = new LinkedHashMap<>();
        for (Map<String, Object> row : buffer) {
            for (String key : row.keySet()) {
                columns.putIfAbsent(key, Boolean.TRUE);
            }
        }
        if (columns.isEmpty()) {
            // Nothing to insert — clear the buffer and bail.
            buffer.clear();
            return;
        }

        // Create (or column-evolve) the destination table before the INSERT. The JDBC driver
        // introspects the table during prepareStatement(), so an absent table fails with
        // UNKNOWN_TABLE; the schema-aware path is responsible for materialising it.
        ensureDestinationTable(columns.keySet());

        String sql = buildInsertSql(columns.keySet());
        log.debug("ClickHouse INSERT into {}.{} engine={} rows={} cols={}",
                tableIdentifier.namespace(), tableIdentifier.name(),
                engine, buffer.size(), columns.keySet());

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Map<String, Object> row : buffer) {
                int paramIdx = 1;
                for (String column : columns.keySet()) {
                    ps.setObject(paramIdx++, row.get(column));
                }
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_WRITE_ERROR,
                    "ClickHouse INSERT failed for table " + tableIdentifier.namespace() + "."
                            + tableIdentifier.name() + ": " + e.getMessage(),
                    e);
        } finally {
            buffer.clear();
        }
    }

    /**
     * Creates (or column-evolves) the destination table from the decoded row shape before the first
     * INSERT. The schema-aware (rowEncoder) path decodes rows directly from the source schema, so the
     * ClickHouse table is derived from the actual decoded columns/values rather than re-translating
     * the source schema. This also covers primitive sources (for example a Kafka string value, which
     * the encoder maps to a single {@code value} column) that have no top-level RECORD schema for
     * {@link AvroToClickHouseSchema} to translate. When no {@link ClickHouseTableSchemaService} is
     * wired (the JSON-fallback path) the destination table must already exist.
     */
    private void ensureDestinationTable(Set<String> columnNames) {
        if (schemaService == null || ensuredColumns.containsAll(columnNames)) {
            return;
        }
        LinkedHashSet<String> union = new LinkedHashSet<>(ensuredColumns);
        union.addAll(columnNames);
        Set<String> keyColumns = new HashSet<>(primaryKey);
        List<ClickHouseColumn> cols = new ArrayList<>(union.size());
        for (String name : union) {
            String base = inferClickHouseType(firstNonNullValue(name));
            boolean keyColumn = keyColumns.contains(name);
            // Key columns drive ORDER BY and must not be Nullable; value columns are nullable so a
            // message missing an optional field binds a JDBC null at INSERT time.
            String type = keyColumn ? base : "Nullable(" + base + ")";
            cols.add(new ClickHouseColumn(name, type, !keyColumn));
        }
        ClickHouseSchema schema = new ClickHouseSchema(cols, primaryKey, engine);
        // Ensure the live table has every column this batch needs, diffing against the actual table
        // (not a source-version cache): the same source schema version can yield different row shapes
        // depending on which optional fields are non-null, so a version-gated skip would miss a
        // genuinely new column and the INSERT would fail with UNKNOWN_IDENTIFIER. ADD COLUMN IF NOT
        // EXISTS keeps this idempotent across the single-use materializers of concurrent tasks.
        try {
            schemaService.ensureColumns(schema);
        } catch (MaterializationException e) {
            throw e;
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_WRITE_ERROR,
                    "Failed to create/evolve ClickHouse table " + tableIdentifier.namespace() + "."
                            + tableIdentifier.name() + ": " + e.getMessage(), e);
        }
        ensuredColumns.addAll(union);
    }

    /** First non-null value seen for {@code column} across the current buffer, or null. */
    @Nullable
    private Object firstNonNullValue(String column) {
        for (Map<String, Object> row : buffer) {
            Object value = row.get(column);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    /**
     * Maps a decoded Java value to the ClickHouse base type used when auto-creating the destination
     * table. Strings, decimals, temporal types and all-null columns fall back to {@code String} — the
     * JDBC driver binds the textual representation, which ClickHouse accepts.
     */
    private static String inferClickHouseType(@Nullable Object value) {
        if (value instanceof Integer) {
            return "Int32";
        } else if (value instanceof Long) {
            return "Int64";
        } else if (value instanceof Short) {
            return "Int16";
        } else if (value instanceof Byte) {
            return "Int8";
        } else if (value instanceof Boolean) {
            return "Bool";
        } else if (value instanceof Float) {
            return "Float32";
        } else if (value instanceof Double) {
            return "Float64";
        }
        return "String";
    }

    private String buildInsertSql(Iterable<String> columns) {
        StringBuilder cols = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        Iterator<String> it = columns.iterator();
        while (it.hasNext()) {
            String column = it.next();
            cols.append(ClickHouseIdentifiers.quote(column));
            placeholders.append('?');
            if (it.hasNext()) {
                cols.append(", ");
                placeholders.append(", ");
            }
        }
        return "INSERT INTO " + ClickHouseIdentifiers.quote(tableIdentifier.namespace()) + "."
                + ClickHouseIdentifiers.quote(tableIdentifier.name()) + " (" + cols + ") VALUES ("
                + placeholders + ")";
    }

    /**
     * Decodes a single message's value bytes into a flat row map.
     *
     * <p>Routing:
     * <ul>
     *   <li>When the {@code context.sourceSchemaVersion()} is set and the
     *       materializer was constructed with a non-null
     *       {@link ClickHouseTableSchemaService}, the schema service resolves
     *       the {@link ClickHouseSchema} for that version and the JSON payload
     *       is mapped into row entries using the persisted column order
     *       (missing fields land as nulls).</li>
     *   <li>Otherwise the JSON-payload fallback is used: the message value is
     *       parsed as a top-level JSON object, and each field becomes a column.</li>
     * </ul>
     */
    private Map<String, Object> decodeBytes(byte[] data, MaterializationContext context) {
        if (data == null || data.length == 0) {
            return Map.of();
        }
        String json = new String(data, StandardCharsets.UTF_8);

        Optional<Long> versionOpt = context.sourceSchemaVersion();
        if (schemaService != null && versionOpt.isPresent()) {
            return decodeRowWithSchema(json, versionOpt.get());
        }
        return decodeRowJsonFallback(json);
    }

    private Map<String, Object> decodeRowWithSchema(String json, long version) {
        ClickHouseSchema schema;
        try {
            schema = schemaService.getTableSchema(version);
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                    "Failed to resolve ClickHouse schema version " + version + " for "
                            + tableIdentifier.namespace() + "." + tableIdentifier.name() + ": "
                            + e.getMessage(),
                    e);
        }
        if (schema == null) {
            // No schema persisted for this version: degrade to the JSON fallback rather than
            // failing the record. The orchestrator's normal evolution path is responsible for
            // bringing the schema into the catalog before subsequent writes.
            log.debug("No ClickHouse schema persisted for version {} on {}.{}; "
                            + "falling back to JSON decoding",
                    version, tableIdentifier.namespace(), tableIdentifier.name());
            return decodeRowJsonFallback(json);
        }
        try {
            JsonNode root = JSON.readTree(json);
            if (root == null || !root.isObject()) {
                throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                        "Schema-driven ClickHouse decoder expects a JSON object payload; got: "
                                + (root == null ? "null" : root.getNodeType().name()));
            }
            Map<String, Object> row = new LinkedHashMap<>();
            for (ClickHouseColumn column : schema.columns()) {
                JsonNode value = resolveField(root, column.name());
                row.put(column.name(), toJavaValue(value));
            }
            return row;
        } catch (MaterializationException e) {
            throw e;
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                    "Failed to parse ClickHouse row payload for schema version " + version
                            + ": " + e.getMessage(),
                    e);
        }
    }

    /**
     * Resolves a column value from the JSON root, supporting dotted column
     * names ({@code address.city}) that arise from AVRO record flattening.
     */
    private static JsonNode resolveField(JsonNode root, String columnName) {
        if (!columnName.contains(".")) {
            return root.get(columnName);
        }
        JsonNode cursor = root;
        for (String segment : columnName.split("\\.")) {
            if (cursor == null || !cursor.isObject()) {
                return null;
            }
            cursor = cursor.get(segment);
        }
        return cursor;
    }

    private Map<String, Object> decodeRowJsonFallback(String json) {
        try {
            JsonNode root = JSON.readTree(json);
            if (root == null || !root.isObject()) {
                // Non-object payloads (raw bytes, primitives) are not interpretable as rows in
                // the placeholder decoder. Surface as an explicit failure so the orchestrator's
                // error-mode policy can route it (skip/DLQ/halt).
                throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                        "Placeholder ClickHouse decoder expects a JSON object payload; got: "
                                + (root == null ? "null" : root.getNodeType().name())
                                + ". Set sourceSchemaVersion + register a schema to use the "
                                + "schema-service-driven decoder.");
            }
            Map<String, Object> row = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                row.put(field.getKey(), toJavaValue(field.getValue()));
            }
            return row;
        } catch (MaterializationException e) {
            throw e;
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.MESSAGE_PARSE_FAILED,
                    "Failed to parse ClickHouse row payload as JSON: " + e.getMessage(), e);
        }
    }

    private static Object toJavaValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isInt()) {
            return node.intValue();
        }
        if (node.isLong()) {
            return node.longValue();
        }
        if (node.isFloat() || node.isDouble()) {
            return node.doubleValue();
        }
        if (node.isShort()) {
            return node.shortValue();
        }
        if (node.isBigInteger()) {
            return node.bigIntegerValue();
        }
        if (node.isBigDecimal()) {
            return node.decimalValue();
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        // Fallback: object/array -> JSON string. T12 will replace this with proper structural
        // mapping (ClickHouse Array/Tuple/Map types) once the schema service is wired in.
        return node.toString();
    }

    private static long estimateBytes(GenericEntry entry) {
        ByteBuf payload = entry.entry().payload();
        return payload == null ? 0L : payload.readableBytes();
    }

    private static void release(GenericEntry record) {
        if (record.entry() != null && record.entry().payload() != null) {
            record.entry().payload().release();
        }
    }
}
