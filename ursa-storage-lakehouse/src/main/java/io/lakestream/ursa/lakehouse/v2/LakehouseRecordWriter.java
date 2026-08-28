/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.compact.FailureMessageHandler;
import java.util.List;

public interface LakehouseRecordWriter<T> {

    void write(T entry) throws ExceptionWithCode;

    List<IWriteResult> close() throws ExceptionWithCode;

    default void registerFailureMessageHandler(FailureMessageHandler failureMessageHandler) {
        // Default implementation does nothing, can be overridden by specific implementations
    }
}
