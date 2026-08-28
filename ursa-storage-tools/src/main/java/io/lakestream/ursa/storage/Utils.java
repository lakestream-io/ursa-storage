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

    /**
     * Generates a key range for list/rangeScan operations to retrieve child nodes at a specific recursive layer
     * under the given base path.
     *
     * <p>The recursive layer determines the depth of child nodes to retrieve:
     * <ul>
     *   <li>Layer 1: immediate children of basePath (e.g., /root/a, /root/b)</li>
     *   <li>Layer 2: children of immediate children (e.g., /root/a/0, /root/b/0)</li>
     *   <li>Layer N: children at depth N from basePath</li>
     * </ul>
     *
     * <p>Examples:
     * <pre>{@code
     * // Given keys: /root/a, /root/a/0, /root/a/1, /root/b, /root/b/0
     *
     * // basePath="/root", recursiveLayer=1 returns range that matches:
     * // - /root/a
     * // - /root/b
     *
     * // basePath="/root", recursiveLayer=2 returns range that matches:
     * // - /root/a/0
     * // - /root/a/1
     * // - /root/b/0
     * }</pre>
     *
     * @param basePath the base path to search under (trailing slash is optional)
     * @param recursiveLayer the depth of child nodes to retrieve (must be >= 1)
     * @return a Pair containing (startKey, endKey) for range scan operations
     * @throws IllegalArgumentException if recursiveLayer is less than 1
     */
    public static Pair<String, String> generateKeyRange(String basePath, int recursiveLayer) {
        if (recursiveLayer < 1) {
            throw new IllegalArgumentException("recursiveLayer should be greater than or equal to 1");
        }
        final StringBuilder startKey;
        if (basePath.endsWith("/")) {
            startKey = new StringBuilder(basePath);
        } else {
            startKey = new StringBuilder(basePath + "/");
        }
        if (recursiveLayer == 1) {
            return Pair.of(startKey.toString(), startKey + "/");
        }
        final var endKey = new StringBuilder(startKey.toString());
        for (int i = 0; i < recursiveLayer - 1; i++) {
            startKey.append("\0/");
            endKey.append("~/");
        }
        return Pair.of(startKey.toString(), endKey.toString());
    }

}
