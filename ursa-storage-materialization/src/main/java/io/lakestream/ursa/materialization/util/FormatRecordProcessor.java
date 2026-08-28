/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.util;

import java.io.IOException;

/**
 * Processes a decoded storage record immediately so callers do not retain direct-memory
 * payloads longer than necessary.
 *
 * @param <R>
 *           the decoded record type
 */
public interface FormatRecordProcessor<R> {

    /**
     * This method will be called once the record is generated.
     *
     * @param r
     *     the record parsed from message
     * @throws IOException
     */
    void handleRecord(R r) throws RuntimeException;

}
