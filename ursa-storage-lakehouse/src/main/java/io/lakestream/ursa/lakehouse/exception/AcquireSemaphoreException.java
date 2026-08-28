/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class AcquireSemaphoreException extends LakehouseException {

        public AcquireSemaphoreException(String message) {
            super(message);
        }

        public AcquireSemaphoreException(String message, Throwable cause) {
            super(message, cause);
        }
}
