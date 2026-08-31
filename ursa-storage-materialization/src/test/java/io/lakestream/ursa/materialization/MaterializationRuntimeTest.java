/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

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
        assertThat(runtime.storageApi()).isNull();
        assertThat(runtime.taskProperties()).isEmpty();
    }

    @Test
    void withTaskPropertiesCopiesAllFieldsAndSetsProperties() {
        MaterializationRuntime base = new MaterializationRuntime(
                SCHEMA_SERVICE, EVOLUTION, EXECUTOR, LOG, METRICS, HANDLER);

        MaterializationRuntime withProperties = base.withTaskProperties(Map.of("catalog", "lakehouse"));

        assertThat(withProperties.taskProperties()).containsEntry("catalog", "lakehouse");
        // All other fields are preserved.
        assertThat(withProperties.schemaService()).isSameAs(SCHEMA_SERVICE);
        assertThat(withProperties.schemaEvolutionManager()).isSameAs(EVOLUTION);
        assertThat(withProperties.materializationExecutor()).isSameAs(EXECUTOR);
        assertThat(withProperties.logger()).isSameAs(LOG);
        assertThat(withProperties.metrics()).isSameAs(METRICS);
        assertThat(withProperties.failureMessageHandler()).isSameAs(HANDLER);
        // The original is unchanged (record copy semantics).
        assertThat(base.taskProperties()).isEmpty();
        assertThat(withProperties.storageApi()).isNull();
    }

    @Test
    void withStorageApiAndTaskPropertiesCompose() {
        MaterializationRuntime base = new MaterializationRuntime(
                SCHEMA_SERVICE, EVOLUTION, EXECUTOR, LOG, METRICS, HANDLER);

        MaterializationRuntime composed = base.withStorageApi(null)
                .withTaskProperties(Map.of("source", "storage"));

        assertThat(composed.storageApi()).isNull();
        assertThat(composed.taskProperties()).containsEntry("source", "storage");
        assertThat(base.withTaskProperties(Map.of("source", "storage"))
                .withStorageApi(null).taskProperties()).containsEntry("source", "storage");
    }
}
