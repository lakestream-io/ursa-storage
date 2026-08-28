/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class OxiaIndexNotFoundException extends Exception {

    public OxiaIndexNotFoundException(String message) {
        super(message);
    }

    public OxiaIndexNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
