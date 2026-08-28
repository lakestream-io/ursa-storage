/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class NoSuchEntriesException extends RuntimeException {

    public NoSuchEntriesException(Long stream, Long startOffset, Long endOffset) {
        super("No such entries during the range " + stream + ":[" + startOffset + "-" + endOffset + "]");
    }

}
