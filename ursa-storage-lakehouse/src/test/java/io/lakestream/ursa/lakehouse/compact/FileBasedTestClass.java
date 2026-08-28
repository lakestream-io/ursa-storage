/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

@Slf4j
public class FileBasedTestClass {

    protected Path path;

    @BeforeEach
    public void setup() throws Exception {
        this.path = Files.createTempDirectory("ursa-storage");
        log.info("Using the directory {} for testing", this.path.toAbsolutePath());
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (this.path != null) {
            deleteDirectory(path.toFile());
        }
    }

    public static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}
