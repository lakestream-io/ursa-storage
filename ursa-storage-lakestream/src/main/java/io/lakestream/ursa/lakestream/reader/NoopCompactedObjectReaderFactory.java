/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.reader;

import io.lakestream.ursa.metrics.InstrumentProvider;
import java.util.Properties;

public class NoopCompactedObjectReaderFactory implements CompactedObjectReaderFactory {

    @Override
    public void initialize(Properties properties, InstrumentProvider provider) throws Exception {

    }

    @Override
    public CompactedObjectReader open(String logName) {
        return new NoopCompactedObjectReader();
    }

    @Override
    public void close() {

    }
}
