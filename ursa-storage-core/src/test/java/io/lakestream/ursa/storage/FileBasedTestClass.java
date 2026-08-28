/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

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
            Files.walk(path)
                .map(Path::toFile)
                .forEach(File::delete);
            try {
                Files.deleteIfExists(path);
            } catch (Exception e) {
                log.error("Failed to delete the directory {}", this.path.toAbsolutePath());
            }
        }
    }
}
