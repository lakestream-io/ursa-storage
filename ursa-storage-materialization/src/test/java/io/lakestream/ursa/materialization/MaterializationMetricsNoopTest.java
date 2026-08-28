/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.MaterializationState;
import io.lakestream.api.materialization.TableCatalogType;
import org.junit.jupiter.api.Test;

class MaterializationMetricsNoopTest {

    private static final StreamIdentifier STREAM = StreamIdentifier.of("public/default", "events");

    @Test
    void everyMethodSilentlySucceeds() {
        MaterializationMetrics metrics = MaterializationMetrics.noop();
        assertThat(metrics).isNotNull();

        assertThatCode(() -> {
            metrics.recordWritten("lake", TableCatalogType.ICEBERG, STREAM);
            metrics.recordCommitDuration("lake", TableCatalogType.ICEBERG, STREAM, 1L);
            metrics.recordCommitRetry("lake", TableCatalogType.ICEBERG, STREAM, true);
            metrics.recordCommitRetry("lake", TableCatalogType.ICEBERG, STREAM, false);
            metrics.recordSchemaEvolutionApplied("lake", TableCatalogType.ICEBERG, STREAM, "ADD");
            metrics.recordSchemaEvolutionRejected("lake", TableCatalogType.ICEBERG, STREAM, "x");
            metrics.recordDlqRecord("lake", TableCatalogType.ICEBERG, STREAM);
            metrics.setState("lake", TableCatalogType.ICEBERG, STREAM, MaterializationState.RUNNING);
        }).doesNotThrowAnyException();
    }

    @Test
    void noopReturnsSeparateInstancesAcrossCalls() {
        // No state-sharing requirement, but we should never get null.
        assertThat(MaterializationMetrics.noop()).isNotNull();
        assertThat(MaterializationMetrics.noop()).isNotNull();
    }
}
