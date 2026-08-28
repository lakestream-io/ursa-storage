/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.exception;

public class ConvertException extends RuntimeException {

    public ConvertException(String message, Throwable cause) {
        super(message, cause);
    }

}
