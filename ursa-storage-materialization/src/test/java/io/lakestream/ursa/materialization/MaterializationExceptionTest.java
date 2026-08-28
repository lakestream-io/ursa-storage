/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import org.junit.jupiter.api.Test;

class MaterializationExceptionTest {

    @Test
    void carriesExceptionCode() {
        MaterializationException ex = new MaterializationException(
                ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE, "schema mismatch");

        assertThat(ex.getExceptionCode())
                .isEqualTo(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE);
        assertThat(ex.getMessage()).isEqualTo("schema mismatch");
    }

    @Test
    void preservesMessageAndCause() {
        Throwable cause = new IllegalStateException("boom");
        MaterializationException ex = new MaterializationException(
                ExceptionCode.LAKEHOUSE_COMMIT_ERROR, "commit failed", cause);

        assertThat(ex.getExceptionCode()).isEqualTo(ExceptionCode.LAKEHOUSE_COMMIT_ERROR);
        assertThat(ex.getMessage()).isEqualTo("commit failed");
        // The cause chain is: MaterializationException -> ExceptionWithCode -> user cause.
        // The user-provided cause remains reachable on the chain.
        assertThat(ex.getRealException().getCause()).isSameAs(cause);
    }

    @Test
    void isUnchecked() {
        MaterializationException ex = new MaterializationException(
                ExceptionCode.INTERNAL_ERROR, "x");
        assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void rejectsNullCode() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationException(null, "msg"));
    }

    @Test
    void rejectsNullCodeWithCause() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationException(
                        null, "msg", new RuntimeException("c")));
    }

    /**
     * Integration contract: T10's CompactionWorker pattern-matches on the existing
     * {@code RuntimeExceptionWithCode} / {@code ExceptionWithCode} hierarchy. Locking
     * this in so a future refactor of the exception type does not silently break
     * code-aware retry / quarantine routing.
     */
    @Test
    void composesWithRuntimeExceptionWithCode() {
        MaterializationException ex = new MaterializationException(
                ExceptionCode.MESSAGE_BAD_SCHEMA, "bad");

        assertThat(ex).isInstanceOf(RuntimeExceptionWithCode.class);
        assertThat(ex.getRealException())
                .isNotNull()
                .isInstanceOf(ExceptionWithCode.class);
        assertThat(ex.getRealException().getExceptionCode())
                .isEqualTo(ExceptionCode.MESSAGE_BAD_SCHEMA);
    }

    @Test
    void wrappedExceptionWithCodeIsReachableViaCauseChain() {
        // Confirms that orchestrator code matching e.getCause() instanceof ExceptionWithCode
        // finds the carried ExceptionWithCode and its code.
        MaterializationException ex = new MaterializationException(
                ExceptionCode.LAKEHOUSE_WRITE_ERROR, "write failed");

        assertThat(ex.getCause()).isInstanceOf(ExceptionWithCode.class);
        ExceptionWithCode inner = (ExceptionWithCode) ex.getCause();
        assertThat(inner.getExceptionCode()).isEqualTo(ExceptionCode.LAKEHOUSE_WRITE_ERROR);
    }
}
