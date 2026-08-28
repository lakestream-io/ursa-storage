/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class UnityCatalogSasTokenProvider implements SASTokenProvider {

    public static final Map<String, String> TOKEN_MAP = new ConcurrentHashMap<>();

    public static void updateToken(String url, String token) {
        TOKEN_MAP.put(url, token);
    }

    //todo: remove the token delay some time.
    public static void removeToken(String url) {
        TOKEN_MAP.remove(url);
    }

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {

    }

    @Override
    public String getSASToken(String account, String fileSystem, String path, String operation)
        throws IOException {
        String[] pathParts = path.split("/");
        if (pathParts.length < 4) {
            throw new IllegalStateException("Invalid path format: " + path);
        }
        String targetContainerPath = "/" + pathParts[1] + "/" + pathParts[2] + "/" + pathParts[3];

        if (TOKEN_MAP.containsKey(targetContainerPath)) {
            return TOKEN_MAP.get(targetContainerPath);
        } else {
            throw new IllegalStateException(
                "Unity catalog token not found for " + account + "/" + fileSystem + "/" + path);
        }
    }

    @VisibleForTesting
    public static void clearTokenMap() {
        TOKEN_MAP.clear();
    }
}
