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
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * API-shape tests for {@link MaterializationMetrics}: locks in the OTel-bound
 * attribute slots on each {@code record*} / {@code setState} method so adapter
 * teams cannot silently change the metric labels (which would break dashboards
 * and alerts downstream).
 *
 * <p>The contract is asserted by driving a hand-rolled capture spy through
 * every interface method and verifying:
 * <ul>
 *   <li>each invocation lands on the expected method;</li>
 *   <li>the first three positional arguments are always
 *       {@code (String catalog, TableCatalogType catalogType, StreamIdentifier stream)}
 *       — these are the OTel-bound dimensions; and</li>
 *   <li>the method-specific trailing arguments (duration, success-flag,
 *       operation-name, reason, state) flow through unchanged.</li>
 * </ul>
 *
 * <p>This is not a behavioural test: the spy stores invocations rather than
 * exercising any production code path. The point is to freeze the
 * interface so editing it requires updating the assertions here, which forces
 * the author to think about the OTel attribute contract before changing it.
 */
class MaterializationMetricsContractTest {

    private static final StreamIdentifier STREAM = StreamIdentifier.of("public/default", "events");
    private static final String CATALOG = "lake";
    private static final TableCatalogType CATALOG_TYPE = TableCatalogType.ICEBERG;

    @Test
    void recordWrittenCapturesCatalogCatalogTypeStream() {
        CapturingMetrics spy = new CapturingMetrics();
        spy.recordWritten(CATALOG, CATALOG_TYPE, STREAM);

        assertThat(spy.invocations).hasSize(1);
        Invocation inv = spy.invocations.get(0);
        assertThat(inv.method).isEqualTo("recordWritten");
        assertThat(inv.args).containsExactly(CATALOG, CATALOG_TYPE, STREAM);
    }

    @Test
    void recordCommitDurationCapturesCatalogTypeStreamThenNanos() {
        CapturingMetrics spy = new CapturingMetrics();
        spy.recordCommitDuration(CATALOG, CATALOG_TYPE, STREAM, 12_345L);

        assertThat(spy.invocations).hasSize(1);
        Invocation inv = spy.invocations.get(0);
        assertThat(inv.method).isEqualTo("recordCommitDuration");
        assertThat(inv.args).containsExactly(CATALOG, CATALOG_TYPE, STREAM, 12_345L);
    }

    @Test
    void recordCommitRetryCapturesCatalogTypeStreamThenSuccessFlag() {
        CapturingMetrics spy = new CapturingMetrics();
        spy.recordCommitRetry(CATALOG, CATALOG_TYPE, STREAM, true);
        spy.recordCommitRetry(CATALOG, CATALOG_TYPE, STREAM, false);

        assertThat(spy.invocations).hasSize(2);
        assertThat(spy.invocations.get(0).args)
                .containsExactly(CATALOG, CATALOG_TYPE, STREAM, true);
        assertThat(spy.invocations.get(1).args)
                .containsExactly(CATALOG, CATALOG_TYPE, STREAM, false);
    }

    @Test
    void recordSchemaEvolutionAppliedCapturesCatalogTypeStreamThenOperation() {
        CapturingMetrics spy = new CapturingMetrics();
        spy.recordSchemaEvolutionApplied(CATALOG, CATALOG_TYPE, STREAM, "ADD_COLUMN");

        assertThat(spy.invocations).hasSize(1);
        Invocation inv = spy.invocations.get(0);
        assertThat(inv.method).isEqualTo("recordSchemaEvolutionApplied");
        assertThat(inv.args).containsExactly(CATALOG, CATALOG_TYPE, STREAM, "ADD_COLUMN");
    }

    @Test
    void recordSchemaEvolutionRejectedCapturesCatalogTypeStreamThenReason() {
        CapturingMetrics spy = new CapturingMetrics();
        spy.recordSchemaEvolutionRejected(CATALOG, CATALOG_TYPE, STREAM, "DROP_DENIED");

        assertThat(spy.invocations).hasSize(1);
        Invocation inv = spy.invocations.get(0);
        assertThat(inv.method).isEqualTo("recordSchemaEvolutionRejected");
        assertThat(inv.args).containsExactly(CATALOG, CATALOG_TYPE, STREAM, "DROP_DENIED");
    }

    @Test
    void recordDlqRecordCapturesCatalogCatalogTypeStream() {
        CapturingMetrics spy = new CapturingMetrics();
        spy.recordDlqRecord(CATALOG, CATALOG_TYPE, STREAM);

        assertThat(spy.invocations).hasSize(1);
        Invocation inv = spy.invocations.get(0);
        assertThat(inv.method).isEqualTo("recordDlqRecord");
        assertThat(inv.args).containsExactly(CATALOG, CATALOG_TYPE, STREAM);
    }

    @Test
    void setStateCapturesCatalogTypeStreamThenMaterializationState() {
        CapturingMetrics spy = new CapturingMetrics();
        spy.setState(CATALOG, CATALOG_TYPE, STREAM, MaterializationState.RUNNING);

        assertThat(spy.invocations).hasSize(1);
        Invocation inv = spy.invocations.get(0);
        assertThat(inv.method).isEqualTo("setState");
        assertThat(inv.args)
                .containsExactly(CATALOG, CATALOG_TYPE, STREAM, MaterializationState.RUNNING);
    }

    @Test
    void firstThreePositionalArgsAreStableAcrossAllRecordMethods() {
        // Drive every method with the SAME (catalog, catalogType, stream) triple and
        // assert each invocation starts with that exact prefix — guards against an
        // accidental re-ordering of attribute slots that would break the OTel contract.
        CapturingMetrics spy = new CapturingMetrics();
        spy.recordWritten(CATALOG, CATALOG_TYPE, STREAM);
        spy.recordCommitDuration(CATALOG, CATALOG_TYPE, STREAM, 1L);
        spy.recordCommitRetry(CATALOG, CATALOG_TYPE, STREAM, true);
        spy.recordSchemaEvolutionApplied(CATALOG, CATALOG_TYPE, STREAM, "ADD_COLUMN");
        spy.recordSchemaEvolutionRejected(CATALOG, CATALOG_TYPE, STREAM, "DROP_DENIED");
        spy.recordDlqRecord(CATALOG, CATALOG_TYPE, STREAM);
        spy.setState(CATALOG, CATALOG_TYPE, STREAM, MaterializationState.RUNNING);

        assertThat(spy.invocations).hasSize(7);
        for (Invocation inv : spy.invocations) {
            assertThat(inv.args.size())
                    .as("method %s should take at least 3 attribute-slot args", inv.method)
                    .isGreaterThanOrEqualTo(3);
            assertThat(inv.args.get(0))
                    .as("method %s slot 0 must be the catalog name", inv.method)
                    .isEqualTo(CATALOG);
            assertThat(inv.args.get(1))
                    .as("method %s slot 1 must be the TableCatalogType", inv.method)
                    .isEqualTo(CATALOG_TYPE);
            assertThat(inv.args.get(2))
                    .as("method %s slot 2 must be the StreamIdentifier", inv.method)
                    .isEqualTo(STREAM);
        }
    }

    @Test
    void noopSmokeEveryMethodSucceedsAndHasNoSideEffect() {
        MaterializationMetrics noop = MaterializationMetrics.noop();
        assertThat(noop).isNotNull();
        assertThatCode(() -> {
            noop.recordWritten(CATALOG, CATALOG_TYPE, STREAM);
            noop.recordCommitDuration(CATALOG, CATALOG_TYPE, STREAM, 1L);
            noop.recordCommitRetry(CATALOG, CATALOG_TYPE, STREAM, true);
            noop.recordCommitRetry(CATALOG, CATALOG_TYPE, STREAM, false);
            noop.recordSchemaEvolutionApplied(CATALOG, CATALOG_TYPE, STREAM, "ADD_COLUMN");
            noop.recordSchemaEvolutionRejected(CATALOG, CATALOG_TYPE, STREAM, "DROP_DENIED");
            noop.recordDlqRecord(CATALOG, CATALOG_TYPE, STREAM);
            for (MaterializationState state : MaterializationState.values()) {
                noop.setState(CATALOG, CATALOG_TYPE, STREAM, state);
            }
        }).doesNotThrowAnyException();
    }

    // --- helpers ---------------------------------------------------------

    /** One captured invocation: method name + positional argument list (boxed). */
    private static final class Invocation {
        final String method;
        final List<Object> args;

        Invocation(String method, Object... args) {
            this.method = method;
            this.args = List.of(args);
        }
    }

    /**
     * Hand-rolled capture spy: records each call's method name and argument list
     * for the test to inspect. Deliberately written without Mockito to keep the
     * dependency surface small and to make the captured-invocation shape easy to
     * read in failure messages.
     */
    private static final class CapturingMetrics implements MaterializationMetrics {
        final List<Invocation> invocations = new ArrayList<>();

        @Override
        public void recordWritten(String catalog, TableCatalogType catalogType,
                                  StreamIdentifier stream) {
            invocations.add(new Invocation("recordWritten", catalog, catalogType, stream));
        }

        @Override
        public void recordCommitDuration(String catalog, TableCatalogType catalogType,
                                         StreamIdentifier stream, long nanos) {
            invocations.add(new Invocation(
                    "recordCommitDuration", catalog, catalogType, stream, nanos));
        }

        @Override
        public void recordCommitRetry(String catalog, TableCatalogType catalogType,
                                      StreamIdentifier stream, boolean success) {
            invocations.add(new Invocation(
                    "recordCommitRetry", catalog, catalogType, stream, success));
        }

        @Override
        public void recordSchemaEvolutionApplied(
                String catalog, TableCatalogType catalogType,
                StreamIdentifier stream, String operation) {
            invocations.add(new Invocation(
                    "recordSchemaEvolutionApplied", catalog, catalogType, stream, operation));
        }

        @Override
        public void recordSchemaEvolutionRejected(
                String catalog, TableCatalogType catalogType,
                StreamIdentifier stream, String reason) {
            invocations.add(new Invocation(
                    "recordSchemaEvolutionRejected", catalog, catalogType, stream, reason));
        }

        @Override
        public void recordDlqRecord(String catalog, TableCatalogType catalogType,
                                    StreamIdentifier stream) {
            invocations.add(new Invocation("recordDlqRecord", catalog, catalogType, stream));
        }

        @Override
        public void setState(String catalog, TableCatalogType catalogType,
                             StreamIdentifier stream, MaterializationState state) {
            invocations.add(new Invocation("setState", catalog, catalogType, stream, state));
        }
    }
}
