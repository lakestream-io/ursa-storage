/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import org.apache.commons.lang3.tuple.Pair;

public class Utils {

    public static Pair<String, String> validateOxiaUrl(String metadataURL) {
        if (metadataURL == null || !metadataURL.startsWith("oxia://")) {
            throw new IllegalArgumentException("Invalid metadata URL. The oxia metadata format should be "
                + "'oxia://host:port/[namespace]'.");
        }
        final var addressWithNamespace = metadataURL.substring("oxia://".length());
        final var split = addressWithNamespace.split("/");
        if (split.length != 2 && split.length != 1) {
            throw new IllegalArgumentException("Invalid metadata URL. The oxia metadata format should be "
                + "'oxia://host:port/[namespace]'.");
        }

        return Pair.of(split[0], (split.length > 1) ? split[1] : "default");
    }
}
