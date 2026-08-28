/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.RuntimeExceptionWithCode;
import java.util.Objects;

/**
 * Runtime exception raised by the materialization framework.
 *
 * <p>Composes with the existing {@link RuntimeExceptionWithCode} +
 * {@link ExceptionWithCode} contract from {@code ursa-storage-common}: code-aware
 * orchestrator code that already pattern-matches on {@code RuntimeExceptionWithCode}
 * (e.g. the lakehouse encoders) keeps working without change, and the carried
 * {@link ExceptionCode} is accessible through {@link #getExceptionCode()} or
 * {@link #getRealException()}{@code .getExceptionCode()}.
 *
 * <p>The exception is unchecked because materialization runs inside an executor
 * and rethrowing checked exceptions across that boundary is awkward.
 */
public class MaterializationException extends RuntimeExceptionWithCode {

    /**
     * Builds a {@code MaterializationException} with the given code and message.
     */
    public MaterializationException(ExceptionCode code, String message) {
        super(new ExceptionWithCode(Objects.requireNonNull(code, "code"), message));
    }

    /**
     * Builds a {@code MaterializationException} with the given code, message and cause.
     *
     * <p>The {@code cause} is attached to the wrapped {@link ExceptionWithCode} so it
     * remains reachable through the standard {@link Throwable#getCause()} chain
     * ({@code MaterializationException} -&gt; {@code ExceptionWithCode} -&gt; {@code cause}).
     */
    public MaterializationException(ExceptionCode code, String message, Throwable cause) {
        super(new ExceptionWithCode(Objects.requireNonNull(code, "code"), message, cause));
    }

    /**
     * Returns the {@link ExceptionCode} carried by the underlying {@link ExceptionWithCode}.
     * Convenience accessor; equivalent to {@code getRealException().getExceptionCode()}.
     */
    public ExceptionCode getExceptionCode() {
        return getRealException().getExceptionCode();
    }
}
