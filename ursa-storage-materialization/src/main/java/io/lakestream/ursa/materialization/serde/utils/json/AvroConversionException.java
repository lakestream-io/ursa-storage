/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json;

import org.apache.avro.AvroRuntimeException;

public class AvroConversionException extends AvroRuntimeException {

    public AvroConversionException(String message) {
        super(message);
    }

    public AvroConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
