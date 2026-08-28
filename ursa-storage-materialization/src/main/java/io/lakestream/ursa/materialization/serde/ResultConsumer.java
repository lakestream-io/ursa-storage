/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

public interface ResultConsumer<T> {

    void onResult(T t);

    /**
     * Receives an error and its optional source context.
     *
     * <p>When {@code ctx} is a {@link GenericEntry}, ownership of its payload reference transfers
     * to this callback as soon as the method is invoked. The implementation must release that
     * reference exactly once, including when it throws.
     */
    void onErrorWithCtx(Object ctx, Throwable throwable);
}
