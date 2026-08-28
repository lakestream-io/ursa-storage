/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class NoSuchOffsetException extends RuntimeException {

    public NoSuchOffsetException(Long stream, Long offset) {
        super("No such offset " + stream + ":" + offset);
    }
}
