/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.exception;

public class TopicNotFoundException extends LakehouseException {

    public TopicNotFoundException(String message) {
        super(message);
    }
}
