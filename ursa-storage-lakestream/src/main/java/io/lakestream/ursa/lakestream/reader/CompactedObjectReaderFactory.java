/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.reader;

import static com.google.common.base.Preconditions.checkArgument;

import io.lakestream.ursa.metrics.InstrumentProvider;
import java.io.IOException;
import java.util.Properties;

public interface CompactedObjectReaderFactory {

    static CompactedObjectReaderFactory create(String externalReaderClass)
        throws IOException {
        Class<?> externalReaderFactory;
        try {
            externalReaderFactory = Class.forName(externalReaderClass);
            Object obj = externalReaderFactory.getDeclaredConstructor().newInstance();
            checkArgument(obj instanceof CompactedObjectReaderFactory,
                "The package storage provider has to be an instance of "
                    + CompactedObjectReaderFactory.class.getName());
            return (CompactedObjectReaderFactory) obj;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    void initialize(Properties properties, InstrumentProvider provider) throws Exception;

    CompactedObjectReader open(String logName);

    void close();

}
