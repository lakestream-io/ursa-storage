/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json;

import static java.util.stream.Collectors.joining;

import java.util.Deque;

public class PathsPrinter {

    public static String print(Deque<String> path) {
        return path.stream().collect(joining("."));
    }

    public static String print(Deque<String> path, String additionalSegment) {
        if (path.isEmpty()) {
            return additionalSegment;
        }
        return print(path) + "." + additionalSegment;
    }

}
