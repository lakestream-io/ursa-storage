/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.MaterializationState;
import io.lakestream.api.materialization.TableCatalogType;

/**
 * Minimal observability surface the materialization framework calls into.
 *
 * <p>Defined as a tiny interface (rather than a Micrometer / Prometheus
 * registry binding) so sinks do not have to depend on lakehouse-specific
 * observability code. Deployments wire in their own implementation; tests can
 * use {@link #noop()}.
 */
public interface MaterializationMetrics {

    /** Reports that a record was successfully written to the sink. */
    void recordWritten(String catalog, TableCatalogType catalogType, StreamIdentifier stream);

    /** Reports the duration (in nanoseconds) of a sink commit. */
    void recordCommitDuration(String catalog, TableCatalogType catalogType,
                              StreamIdentifier stream, long nanos);

    /** Reports a commit-retry attempt and its outcome. */
    void recordCommitRetry(String catalog, TableCatalogType catalogType,
                           StreamIdentifier stream, boolean success);

    /** Reports that the sink accepted a schema-evolution operation. */
    void recordSchemaEvolutionApplied(String catalog, TableCatalogType catalogType,
                                      StreamIdentifier stream, String operation);

    /** Reports that a schema-evolution operation was rejected by policy or sink. */
    void recordSchemaEvolutionRejected(String catalog, TableCatalogType catalogType,
                                       StreamIdentifier stream, String reason);

    /** Reports that a record was diverted to the dead-letter sink. */
    void recordDlqRecord(String catalog, TableCatalogType catalogType, StreamIdentifier stream);

    /** Records the current {@link MaterializationState} for the stream. */
    void setState(String catalog, TableCatalogType catalogType,
                  StreamIdentifier stream, MaterializationState state);

    /**
     * Returns a no-op metrics implementation. Convenient for unit tests and
     * stub deployments that do not need to record metrics.
     */
    static MaterializationMetrics noop() {
        return new MaterializationMetrics() {
            @Override
            public void recordWritten(String catalog, TableCatalogType catalogType,
                                      StreamIdentifier stream) {
            }

            @Override
            public void recordCommitDuration(String catalog, TableCatalogType catalogType,
                                             StreamIdentifier stream, long nanos) {
            }

            @Override
            public void recordCommitRetry(String catalog, TableCatalogType catalogType,
                                          StreamIdentifier stream, boolean success) {
            }

            @Override
            public void recordSchemaEvolutionApplied(String catalog, TableCatalogType catalogType,
                                                     StreamIdentifier stream, String operation) {
            }

            @Override
            public void recordSchemaEvolutionRejected(String catalog, TableCatalogType catalogType,
                                                      StreamIdentifier stream, String reason) {
            }

            @Override
            public void recordDlqRecord(String catalog, TableCatalogType catalogType,
                                        StreamIdentifier stream) {
            }

            @Override
            public void setState(String catalog, TableCatalogType catalogType,
                                 StreamIdentifier stream, MaterializationState state) {
            }
        };
    }
}
