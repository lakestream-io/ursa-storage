/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.CommitResult;
import io.lakestream.ursa.materialization.MaterializationContext;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.GenericEntry;
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

/**
 * Decode-path tests for {@link ClickHouseTableMaterializer} that verify the
 * schema-service-driven row decoder (T12) and the JSON fallback decoder
 * (T11 placeholder, retained for unversioned streams).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ClickHouseTableMaterializerDecodeTest {

    @Mock
    private Connection connection;
    @Mock
    private PreparedStatement preparedStatement;

    private TableIdentifier tableIdentifier;
    private MaterializationContext unversionedContext;
    private MaterializationContext versionedContext;

    @BeforeEach
    void setUp() throws Exception {
        tableIdentifier = new TableIdentifier("analytics", "events");
        unversionedContext = new MaterializationContext(
                StreamIdentifier.of("public/default", "events"),
                1L,
                10L,
                Optional.empty(),
                Map.of());
        versionedContext = new MaterializationContext(
                StreamIdentifier.of("public/default", "events"),
                2L,
                20L,
                Optional.of(7L),
                Map.of());
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    }

    @Test
    void schemaServiceDrivenDecodePopulatesColumnsInSchemaOrder() throws Exception {
        ClickHouseTableSchemaService schemaService = mock(ClickHouseTableSchemaService.class);
        ClickHouseSchema schema = new ClickHouseSchema(
                List.of(
                        new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("name", "Nullable(String)", true),
                        new ClickHouseColumn("amount", "Float64", false)),
                List.of("id"),
                ClickHouseTableEngine.MERGE_TREE);
        when(schemaService.getTableSchema(7L)).thenReturn(schema);

        ClickHouseTableMaterializer materializer = newMaterializer(schemaService);
        // Note: JSON payload deliberately uses a different key order — the column order in the
        // INSERT statement must follow the schema, not the payload.
        materializer.write(
                jsonEntry("{\"amount\":12.5,\"id\":42,\"name\":\"alice\"}"),
                versionedContext);
        materializer.commit();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(connection).prepareStatement(sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        // Schema-driven order: id, name, amount.
        int idIdx = sql.indexOf("`id`");
        int nameIdx = sql.indexOf("`name`");
        int amountIdx = sql.indexOf("`amount`");
        assertThat(idIdx).isPositive();
        assertThat(nameIdx).isGreaterThan(idIdx);
        assertThat(amountIdx).isGreaterThan(nameIdx);

        // Jackson decodes "42" as Integer; ClickHouse JDBC will coerce it to Int64 on the wire.
        verify(preparedStatement).setObject(eq(1), eq(42));
        verify(preparedStatement).setObject(eq(2), eq("alice"));
        verify(preparedStatement).setObject(eq(3), eq(12.5));
    }

    @Test
    void schemaServiceDrivenDecodeFillsMissingFieldsWithNull() throws Exception {
        ClickHouseTableSchemaService schemaService = mock(ClickHouseTableSchemaService.class);
        ClickHouseSchema schema = new ClickHouseSchema(
                List.of(
                        new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("optional", "Nullable(String)", true)),
                List.of("id"),
                ClickHouseTableEngine.MERGE_TREE);
        when(schemaService.getTableSchema(7L)).thenReturn(schema);

        ClickHouseTableMaterializer materializer = newMaterializer(schemaService);
        materializer.write(jsonEntry("{\"id\":1}"), versionedContext);
        materializer.commit();

        verify(preparedStatement).setObject(eq(1), eq(1));
        // Missing field -> null binding.
        verify(preparedStatement).setObject(eq(2), eq(null));
    }

    @Test
    void schemaServiceDrivenDecodeResolvesDottedColumnNamesFromNestedJson() throws Exception {
        ClickHouseTableSchemaService schemaService = mock(ClickHouseTableSchemaService.class);
        ClickHouseSchema schema = new ClickHouseSchema(
                List.of(
                        new ClickHouseColumn("id", "Int64", false),
                        new ClickHouseColumn("address.city", "String", false),
                        new ClickHouseColumn("address.zip", "String", false)),
                List.of("id"),
                ClickHouseTableEngine.MERGE_TREE);
        when(schemaService.getTableSchema(7L)).thenReturn(schema);

        ClickHouseTableMaterializer materializer = newMaterializer(schemaService);
        materializer.write(
                jsonEntry("{\"id\":1,\"address\":{\"city\":\"Munich\",\"zip\":\"80331\"}}"),
                versionedContext);
        materializer.commit();

        verify(preparedStatement).setObject(eq(2), eq("Munich"));
        verify(preparedStatement).setObject(eq(3), eq("80331"));
    }

    @Test
    void schemaServiceMissReturnsNullSchemaAndFallsBackToJson() throws Exception {
        ClickHouseTableSchemaService schemaService = mock(ClickHouseTableSchemaService.class);
        when(schemaService.getTableSchema(7L)).thenReturn(null);

        ClickHouseTableMaterializer materializer = newMaterializer(schemaService);
        materializer.write(jsonEntry("{\"id\":42}"), versionedContext);
        materializer.commit();

        // Fallback path took the JSON keys as columns.
        verify(connection, times(1)).prepareStatement(anyString());
        verify(preparedStatement).setObject(eq(1), eq(42));
    }

    @Test
    void schemaServiceFailureSurfacesAsParseError() throws Exception {
        ClickHouseTableSchemaService schemaService = mock(ClickHouseTableSchemaService.class);
        when(schemaService.getTableSchema(7L)).thenThrow(new RuntimeException("boom"));

        ClickHouseTableMaterializer materializer = newMaterializer(schemaService);
        assertThatThrownBy(() -> materializer.write(jsonEntry("{\"id\":42}"), versionedContext))
                .isInstanceOf(MaterializationException.class)
                .satisfies(e -> {
                    MaterializationException me = (MaterializationException) e;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.MESSAGE_PARSE_FAILED);
                    assertThat(me.getMessage()).contains("schema version 7");
                });
    }

    @Test
    void jsonFallbackUsedWhenSourceSchemaVersionAbsent() throws Exception {
        ClickHouseTableSchemaService schemaService = mock(ClickHouseTableSchemaService.class);
        // sourceSchemaVersion is empty -> never consult schema service.
        ClickHouseTableMaterializer materializer = newMaterializer(schemaService);
        materializer.write(jsonEntry("{\"id\":42,\"name\":\"a\"}"), unversionedContext);
        materializer.commit();

        verify(schemaService, times(0)).getTableSchema(org.mockito.ArgumentMatchers.any());
        // The fallback path inserts whatever columns appear in the JSON.
        verify(connection, times(1)).prepareStatement(anyString());
    }

    @Test
    void jsonFallbackWhenSchemaServiceIsNullEvenWithVersion() throws Exception {
        // No schema service supplied: even if the context carries a version, the JSON path runs.
        ClickHouseTableMaterializer materializer = newMaterializer(null);
        materializer.write(jsonEntry("{\"id\":42}"), versionedContext);
        materializer.commit();

        verify(connection, times(1)).prepareStatement(anyString());
    }

    @Test
    void commitReportsPayloadBytesMeasuredBeforeDecode() throws Exception {
        // Regression: decodeRows() consumes the entry's ByteBuf, so measuring bytes after the decode
        // loop always read 0 readable bytes and zeroed out CommitResult.bytesCommitted. The size must
        // be captured before decoding.
        ClickHouseTableMaterializer materializer = newMaterializer(null);
        GenericEntry entry = jsonEntry("{\"id\":42,\"name\":\"alice\"}");
        long payloadBytes = entry.entry().payload().readableBytes();
        assertThat(payloadBytes).isPositive();

        materializer.write(entry, versionedContext);
        CommitResult result = materializer.commit();

        assertThat(entry.entry().payload().refCnt()).isZero();
        assertThat(result.recordsCommitted()).isEqualTo(1L);
        assertThat(result.bytesCommitted()).isEqualTo(payloadBytes);
    }

    @Test
    void ursaJsonFallbackDecodesEveryBrokerBatchRecordAndConsumesTheEntry() throws Exception {
        ClickHouseTableMaterializer materializer = new ClickHouseTableMaterializer(
                connection,
                tableIdentifier,
                ClickHouseTableEngine.MERGE_TREE,
                List.of(),
                10,
                null,
                null,
                EntryFormat.URSA,
                null);
        GenericEntry entry = FramedEntries.rawUrsaBatch(
                "{\"id\":1}", "{\"id\":2}", "{\"id\":3}");

        materializer.write(entry, unversionedContext);
        CommitResult result = materializer.commit();

        assertThat(entry.entry().payload().refCnt()).isZero();
        assertThat(result.recordsCommitted()).isEqualTo(3L);
        verify(preparedStatement, times(3)).addBatch();
    }

    private ClickHouseTableMaterializer newMaterializer(ClickHouseTableSchemaService schemaSvc) {
        return new ClickHouseTableMaterializer(
                connection,
                tableIdentifier,
                ClickHouseTableEngine.MERGE_TREE,
                List.of(),
                10,
                schemaSvc);
    }

    private static GenericEntry jsonEntry(String json) {
        // Build the direct Kafka entry framing so write() exercises the production decode path.
        return FramedEntries.of(json);
    }
}
