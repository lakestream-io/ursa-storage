/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.SchemaEvolutionManager;
import io.lakestream.ursa.materialization.serde.SchemaService;
import java.util.Map;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MaterializationRuntimeTest {

    private static final SchemaService<Object> SCHEMA_SERVICE = new SchemaService<>() {
        @Override
        public Map<Long, Object> getSchemaWithVersions(String topic, long schemaVersion) {
            return Map.of();
        }

        @Override
        public void close() {
        }
    };
    private static final SchemaEvolutionManager EVOLUTION = new SchemaEvolutionManager();
    private static final Executor EXECUTOR = Runnable::run;
    private static final Logger LOG = LoggerFactory.getLogger(MaterializationRuntimeTest.class);
    private static final MaterializationMetrics METRICS = MaterializationMetrics.noop();
    private static final FailureMessageHandler HANDLER = FailureMessageHandler.noop();

    @Test
    void rejectsNullSchemaService() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationRuntime(
                        null, EVOLUTION, EXECUTOR, LOG, METRICS, HANDLER))
                .withMessageContaining("schemaService");
    }

    @Test
    void rejectsNullSchemaEvolutionManager() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationRuntime(
                        SCHEMA_SERVICE, null, EXECUTOR, LOG, METRICS, HANDLER))
                .withMessageContaining("schemaEvolutionManager");
    }

    @Test
    void rejectsNullExecutor() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationRuntime(
                        SCHEMA_SERVICE, EVOLUTION, null, LOG, METRICS, HANDLER))
                .withMessageContaining("materializationExecutor");
    }

    @Test
    void rejectsNullLogger() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationRuntime(
                        SCHEMA_SERVICE, EVOLUTION, EXECUTOR, null, METRICS, HANDLER))
                .withMessageContaining("logger");
    }

    @Test
    void rejectsNullMetrics() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationRuntime(
                        SCHEMA_SERVICE, EVOLUTION, EXECUTOR, LOG, null, HANDLER))
                .withMessageContaining("metrics");
    }

    @Test
    void rejectsNullFailureMessageHandler() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationRuntime(
                        SCHEMA_SERVICE, EVOLUTION, EXECUTOR, LOG, METRICS, null))
                .withMessageContaining("failureMessageHandler");
    }

    @Test
    void roundTripsAllFields() {
        MaterializationRuntime runtime = new MaterializationRuntime(
                SCHEMA_SERVICE, EVOLUTION, EXECUTOR, LOG, METRICS, HANDLER);

        assertThat(runtime.schemaService()).isSameAs(SCHEMA_SERVICE);
        assertThat(runtime.schemaEvolutionManager()).isSameAs(EVOLUTION);
        assertThat(runtime.materializationExecutor()).isSameAs(EXECUTOR);
        assertThat(runtime.logger()).isSameAs(LOG);
        assertThat(runtime.metrics()).isSameAs(METRICS);
        assertThat(runtime.failureMessageHandler()).isSameAs(HANDLER);
        // entryFormat defaults to null on the back-compat constructors.
        assertThat(runtime.entryFormat()).isNull();
    }

    @Test
    void withEntryFormatCopiesAllFieldsAndSetsFormat() {
        MaterializationRuntime base = new MaterializationRuntime(
                SCHEMA_SERVICE, EVOLUTION, EXECUTOR, LOG, METRICS, HANDLER);

        MaterializationRuntime withKafka = base.withEntryFormat(EntryFormat.KAFKA);

        assertThat(withKafka.entryFormat()).isEqualTo(EntryFormat.KAFKA);
        // All other fields are preserved.
        assertThat(withKafka.schemaService()).isSameAs(SCHEMA_SERVICE);
        assertThat(withKafka.schemaEvolutionManager()).isSameAs(EVOLUTION);
        assertThat(withKafka.materializationExecutor()).isSameAs(EXECUTOR);
        assertThat(withKafka.logger()).isSameAs(LOG);
        assertThat(withKafka.metrics()).isSameAs(METRICS);
        assertThat(withKafka.failureMessageHandler()).isSameAs(HANDLER);
        // The original is unchanged (record copy semantics).
        assertThat(base.entryFormat()).isNull();
        // withEntryFormat preserves the (null) storageApi.
        assertThat(withKafka.storageApi()).isNull();
    }

    @Test
    void withEntryFormatAndStorageApiCompose() {
        MaterializationRuntime base = new MaterializationRuntime(
                SCHEMA_SERVICE, EVOLUTION, EXECUTOR, LOG, METRICS, HANDLER);

        MaterializationRuntime composed = base.withEntryFormat(EntryFormat.URSA).withStorageApi(null);

        assertThat(composed.entryFormat()).isEqualTo(EntryFormat.URSA);
        assertThat(composed.storageApi()).isNull();
        // withStorageApi must preserve the previously-set entryFormat.
        assertThat(base.withStorageApi(null).withEntryFormat(EntryFormat.KAFKA).entryFormat())
                .isEqualTo(EntryFormat.KAFKA);
    }
}
