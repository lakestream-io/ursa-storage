/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.exception;

public class WalFileAlreadyCompactedException extends RuntimeException {

    public WalFileAlreadyCompactedException() {
        super("wal file already compacted");
    }
}