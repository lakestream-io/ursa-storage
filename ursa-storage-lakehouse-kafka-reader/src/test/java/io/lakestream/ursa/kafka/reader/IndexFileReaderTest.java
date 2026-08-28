/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexFileReaderTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void failsFastOnEmptyMetadataBlockInsteadOfLooping() throws Exception {
        Path parquetFile = temporaryDirectory.resolve("data.parquet");
        Files.createFile(parquetFile);
        Path indexFile = temporaryDirectory.resolve("data.index");
        byte[] emptyMetadata = "[]".getBytes(StandardCharsets.UTF_8);
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
                new GZIPOutputStream(Files.newOutputStream(indexFile))))) {
            output.writeInt(emptyMetadata.length);
            output.write(emptyMetadata);
            output.writeInt(Integer.MAX_VALUE);
            byte[] secondaryIndex = "{\"0\":0}".getBytes(StandardCharsets.UTF_8);
            output.writeInt(secondaryIndex.length);
            output.write(secondaryIndex);
        }

        Properties properties = new Properties();
        properties.setProperty("storagePath", temporaryDirectory.toString());
        try (IndexFileReader reader = new IndexFileReader(
                parquetFile.toUri(), new ReaderConfiguration(properties))) {
            assertThatThrownBy(() -> reader.read(0))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("empty metadata block");
        }
    }
}
