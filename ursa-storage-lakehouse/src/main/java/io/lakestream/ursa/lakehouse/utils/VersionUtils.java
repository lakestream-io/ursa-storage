/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VersionUtils {

    private VersionUtils() {
    }

    public static final String PROJECT_VERSION = parseProjectVersion();

    public static final String VERSION_PLACEHOLDER = "${project.version}";

    private static final String VERSION_FILE = "ursa-version.txt";

    public static String parseProjectVersion() {
        String projectVersion = "unknown";
        try (InputStream in = VersionUtils.class.getClassLoader().getResourceAsStream(VERSION_FILE)) {
            Properties props = new Properties();
            props.load(in);
            String val = props.getProperty("version");
            if (val != null && !VERSION_PLACEHOLDER.equals(val)) {
                projectVersion = val;
            }
            return projectVersion;
        } catch (IOException e) {
            log.warn("Failed to parse project version", e);
            return projectVersion;
        }
    }
}