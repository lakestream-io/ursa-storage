/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class NoSuchKeyException extends Exception {
    public NoSuchKeyException(String message) {
        super(message);
    }
}
