/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

public enum Unit {
    Bytes,

    Messages,

    Seconds,

    Connections,

    Sessions,

    Request,

    None,

    ;

    public String toString() {
        switch (this) {
            case Bytes:
                return "By";

            case Messages:
                return "{message}";

            case Seconds:
                return "s";

            case Connections:
                return "{connection}";

            case Sessions:
                return "{session}";

            case Request:
                return "{request}";

            case None:
            default:
                return "1";
        }
    }
}
