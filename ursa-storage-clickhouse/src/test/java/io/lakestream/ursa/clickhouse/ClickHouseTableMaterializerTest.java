/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.CommitResult;
import io.lakestream.ursa.materialization.MaterializationContext;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ClickHouseTableMaterializerTest {

    @Mock
    private Connection connection;
    @Mock
    private PreparedStatement preparedStatement;

    private TableIdentifier tableIdentifier;
    private MaterializationContext context;

    @BeforeEach
    void setUp() throws Exception {
        tableIdentifier = new TableIdentifier("analytics", "events");
        context = new MaterializationContext(
                StreamIdentifier.of("public/default", "events"),
                1L,
                10L,
                Optional.empty(),
                Map.of());
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    }

    @Test
    void writeBuffersRowsUpToBatchSize() throws Exception {
        ClickHouseTableMaterializer materializer = newMaterializer(3);

        materializer.write(jsonEntry("{\"id\":1}"), context);
        materializer.write(jsonEntry("{\"id\":2}"), context);
        // Below threshold — no flush yet.
        verify(connection, never()).prepareStatement(anyString());

        materializer.write(jsonEntry("{\"id\":3}"), context);
        // Threshold hit — exactly one prepareStatement + executeBatch invocation.
        verify(connection, times(1)).prepareStatement(anyString());
        verify(preparedStatement, times(1)).executeBatch();
    }

    @Test
    void commitFlushesRemainingRows() throws Exception {
        ClickHouseTableMaterializer materializer = newMaterializer(100);

        materializer.write(jsonEntry("{\"id\":1,\"name\":\"a\"}"), context);
        materializer.write(jsonEntry("{\"id\":2,\"name\":\"b\"}"), context);

        verify(connection, never()).prepareStatement(anyString());

        CommitResult result = materializer.commit();
        verify(connection, times(1)).prepareStatement(anyString());
        verify(preparedStatement, times(1)).executeBatch();
        assertThat(result.recordsCommitted()).isEqualTo(2L);
        assertThat(result.bytesCommitted()).isGreaterThan(0L);
    }

    @Test
    void commitReturnsCommitResultWithCounts() throws Exception {
        ClickHouseTableMaterializer materializer = newMaterializer(10);

        materializer.write(jsonEntry("{\"id\":1}"), context);
        materializer.write(jsonEntry("{\"id\":2}"), context);
        materializer.write(jsonEntry("{\"id\":3}"), context);

        CommitResult result = materializer.commit();

        assertThat(result.recordsCommitted()).isEqualTo(3L);
        assertThat(result.sinkMetadata()).containsEntry("clickhouse.rows-inserted", "3");
        assertThat(result.sinkMetadata())
                .containsEntry("clickhouse.engine", ClickHouseTableEngine.MERGE_TREE.name());
        assertThat(result.sinkMetadata()).containsEntry("clickhouse.table", "analytics.events");
    }

    @Test
    void schemaAwareEncoderPathBuffersOneRowPerEmittedRecord() throws Exception {
        // When a row encoder is wired, write() routes the entry through it and buffers each
        // emitted row — verifying the R3/R4 schema-aware path independently of a real broker.
        EntryEncoder<Map<String, Object>> stubEncoder = new EntryEncoder<>() {
            @Override
            public void encode(String topic, GenericEntry entry,
                               ResultConsumer<MaterializationRecord<Map<String, Object>>> consumer,
                               TableSchemaService tableSchemaService, EntryEncoderContext ctx) {
                consumer.onResult(new MaterializationRecord<>(Map.of("id", 1), Optional.empty()));
                consumer.onResult(new MaterializationRecord<>(Map.of("id", 2), Optional.empty()));
            }
        };
        ClickHouseTableMaterializer materializer = new ClickHouseTableMaterializer(
                connection, tableIdentifier, ClickHouseTableEngine.MERGE_TREE, List.of(), 100,
                null, stubEncoder, "events-0");

        materializer.write(MemoryRecordsEntries.of("ignored-by-stub"), context);
        CommitResult result = materializer.commit();

        assertThat(result.recordsCommitted()).isEqualTo(2L);
        verify(preparedStatement, times(2)).addBatch();
        verify(preparedStatement, times(1)).executeBatch();
    }

    @Test
    void entriesDecodeToOneRowPerKafkaRecord() throws Exception {
        ClickHouseTableMaterializer materializer = newMaterializer(100);

        materializer.write(MemoryRecordsEntries.of("{\"id\":1}"), context);
        materializer.write(MemoryRecordsEntries.of("{\"id\":2}"), context);
        materializer.write(MemoryRecordsEntries.of("{\"id\":3}"), context);

        CommitResult result = materializer.commit();
        assertThat(result.recordsCommitted()).isEqualTo(3L);
        verify(preparedStatement, times(3)).addBatch();
        verify(preparedStatement, times(1)).executeBatch();
    }

    @Test
    void insertSqlIsScopedToTableAndColumns() throws Exception {
        ClickHouseTableMaterializer materializer = newMaterializer(10);

        materializer.write(jsonEntry("{\"id\":1,\"name\":\"a\"}"), context);
        materializer.commit();

        ArgumentCaptor<String> sqlCaptor = forClass(String.class);
        verify(connection).prepareStatement(sqlCaptor.capture());
        assertThat(sqlCaptor.getValue())
                .startsWith("INSERT INTO `analytics`.`events`")
                .contains("`id`")
                .contains("`name`")
                .contains("VALUES (?, ?)");
    }

    @Test
    void closeIsIdempotent() throws Exception {
        ClickHouseTableMaterializer materializer = newMaterializer(10);

        materializer.close();
        materializer.close();

        verify(connection, times(1)).close();
    }

    @Test
    void writeAfterCloseThrowsMaterializationException() {
        ClickHouseTableMaterializer materializer = newMaterializer(10);
        materializer.close();
        GenericEntry transferred = jsonEntry("{\"id\":1}");

        assertThatThrownBy(() -> materializer.write(transferred, context))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.INTERNAL_ERROR);
                });
        assertThat(transferred.entry().payload().refCnt()).isZero();
    }

    @Test
    void writeAfterCommitThrowsMaterializationException() {
        ClickHouseTableMaterializer materializer = newMaterializer(10);
        materializer.commit();
        GenericEntry transferred = jsonEntry("{\"id\":1}");

        assertThatThrownBy(() -> materializer.write(transferred, context))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.INTERNAL_ERROR);
                });
        assertThat(transferred.entry().payload().refCnt()).isZero();
    }

    @Test
    void nullContextReleasesTransferredEntry() {
        ClickHouseTableMaterializer materializer = newMaterializer(10);
        GenericEntry transferred = jsonEntry("{\"id\":1}");

        assertThatThrownBy(() -> materializer.write(transferred, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("context");

        assertThat(transferred.entry().payload().refCnt()).isZero();
    }

    @Test
    void secondCommitIsIdempotent() throws Exception {
        ClickHouseTableMaterializer materializer = newMaterializer(10);
        materializer.write(jsonEntry("{\"id\":1}"), context);
        materializer.commit();
        // Second commit must not re-issue an INSERT.
        materializer.commit();
        verify(connection, times(1)).prepareStatement(anyString());
        verify(preparedStatement, times(1)).executeBatch();
    }

    @Test
    void engineIsExposedForCommitMetadata() {
        ClickHouseTableMaterializer materializer = new ClickHouseTableMaterializer(
                connection, tableIdentifier, ClickHouseTableEngine.REPLACING_MERGE_TREE,
                List.of("id"), 10);
        assertThat(materializer.engine()).isEqualTo(ClickHouseTableEngine.REPLACING_MERGE_TREE);
    }

    @Test
    void supportedEvolutionsAreClickHouseStrict() {
        ClickHouseTableMaterializer materializer = newMaterializer(10);
        // ClickHouse defaults: addColumn + addNullableColumn permitted; everything else denied.
        assertThat(materializer.supportedEvolutions().addColumn()).contains(Boolean.TRUE);
        assertThat(materializer.supportedEvolutions().addNullableColumn()).contains(Boolean.TRUE);
        assertThat(materializer.supportedEvolutions().widenType()).contains(Boolean.FALSE);
        assertThat(materializer.supportedEvolutions().dropColumn()).contains(Boolean.FALSE);
    }

    @Test
    void nonJsonObjectPayloadSurfacesParseError() {
        ClickHouseTableMaterializer materializer = newMaterializer(10);

        assertThatThrownBy(() -> materializer.write(jsonEntry("\"just a string\""), context))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.MESSAGE_PARSE_FAILED);
                });
    }

    private ClickHouseTableMaterializer newMaterializer(int batchSize) {
        return new ClickHouseTableMaterializer(
                connection, tableIdentifier, ClickHouseTableEngine.MERGE_TREE, List.of(), batchSize);
    }

    private static GenericEntry jsonEntry(String json) {
        // Build a framed single-message WAL entry so write() decodes through the batch path.
        return MemoryRecordsEntries.of(json);
    }
}
