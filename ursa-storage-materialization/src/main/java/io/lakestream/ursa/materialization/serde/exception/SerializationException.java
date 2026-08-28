/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.exception;

import java.io.IOException;

public class SerializationException extends IOException {

    public SerializationException(String message) {}

    public SerializationException(Throwable e) {
        super(e);
    }

}
