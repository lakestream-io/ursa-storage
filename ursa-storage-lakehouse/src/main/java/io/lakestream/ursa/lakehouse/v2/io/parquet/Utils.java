/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import java.net.URI;

public class Utils {

    public static URI ensureIsDirectory(URI uri) {
        String path = uri.getPath();
        if (!path.endsWith("/")) {
            return URI.create(uri.toString() + "/");
        }
        return uri;
    }
}
