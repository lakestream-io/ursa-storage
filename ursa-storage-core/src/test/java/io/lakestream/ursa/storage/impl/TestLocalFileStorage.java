/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.FileBasedTestClass;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32C;
import lombok.Cleanup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLocalFileStorage extends FileBasedTestClass {

    @Test
    public void testPutAsyncDuplicateFileReturnsEarlyWithoutRedundantIO() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);

        String dataPath = UUID.randomUUID().toString();
        byte[] data = "hello".getBytes();
        ByteBuf content = Unpooled.wrappedBuffer(data);
        // First write should succeed
        storage.putAsync(content, dataPath).get();
        content.release();

        // Second write to the same location should fail with FileAlreadyExistsException
        ByteBuf content2 = Unpooled.wrappedBuffer("world".getBytes());
        ExecutionException ex = Assertions.assertThrows(ExecutionException.class, () -> {
            storage.putAsync(content2, dataPath).get();
        });
        Assertions.assertInstanceOf(FileAlreadyExistsException.class, ex.getCause());
        content2.release();

        // Verify the original data is unchanged (not corrupted by the second write attempt)
        ByteBuf readData = storage.get(dataPath);
        byte[] readBytes = new byte[readData.readableBytes()];
        readData.readBytes(readBytes);
        readData.release();
        Assertions.assertArrayEquals(data, readBytes);
    }

    @Test
    public void testReadWrite() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);
        byte[] data = new byte[1024];
        String dataPath = UUID.randomUUID().toString();
        ByteBuf content = Unpooled.wrappedBuffer(data);
        storage.put(content, dataPath);
        Assertions.assertEquals(1, content.refCnt());
        content.release();

        ByteBuf readData = storage.get(dataPath);
        byte[] readBytes = new byte[readData.readableBytes()];
        readData.readBytes(readBytes);
        readData.release();
        Assertions.assertArrayEquals(data, readBytes);
    }

    @Test
    public void testChecksumSidecarFileIsCreated() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);

        String dataPath = UUID.randomUUID().toString();
        ByteBuf content = Unpooled.wrappedBuffer("test data".getBytes());
        storage.put(content, dataPath);
        content.release();

        Path checksumPath = Paths.get(path.toAbsolutePath().toString(), dataPath + ".crc32c");
        Assertions.assertTrue(Files.exists(checksumPath),
            "Checksum sidecar file should be created alongside the data file");
        String checksumContent = Files.readString(checksumPath);
        Assertions.assertFalse(checksumContent.isBlank(),
            "Checksum file should contain a non-empty checksum value");
    }

    @Test
    public void testPutAsyncChecksumSidecarFileIsCreated() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);

        String dataPath = UUID.randomUUID().toString();
        ByteBuf content = Unpooled.wrappedBuffer("async test data".getBytes());
        storage.putAsync(content, dataPath).get();
        content.release();

        Path checksumPath = Paths.get(path.toAbsolutePath().toString(), dataPath + ".crc32c");
        Assertions.assertTrue(Files.exists(checksumPath),
            "Checksum sidecar file should be created by putAsync()");
        String checksumContent = Files.readString(checksumPath);
        Assertions.assertFalse(checksumContent.isBlank(),
            "Checksum file should contain a non-empty checksum value");
    }

    @Test
    public void testRoundTripWithChecksumVerification() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);

        byte[] data = "round-trip checksum test data".getBytes();
        String dataPath = UUID.randomUUID().toString();
        ByteBuf content = Unpooled.wrappedBuffer(data);
        storage.put(content, dataPath);
        content.release();

        // Verify sidecar exists (checksum verification is active)
        Path checksumPath = Paths.get(path.toAbsolutePath().toString(), dataPath + ".crc32c");
        Assertions.assertTrue(Files.exists(checksumPath));

        // Read back and verify byte-for-byte equality
        ByteBuf readData = storage.get(dataPath);
        byte[] readBytes = new byte[readData.readableBytes()];
        readData.readBytes(readBytes);
        readData.release();
        Assertions.assertArrayEquals(data, readBytes);
    }

    @Test
    public void testChecksumValueMatchesKnownGood() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);

        byte[] data = "test data".getBytes();
        String dataPath = UUID.randomUUID().toString();
        ByteBuf content = Unpooled.wrappedBuffer(data);
        storage.put(content, dataPath);
        content.release();

        // Compute expected CRC32C independently
        CRC32C crc32c = new CRC32C();
        crc32c.update(data);
        String expectedChecksum = Long.toString(crc32c.getValue());

        // Read the stored checksum and compare
        Path checksumPath = Paths.get(path.toAbsolutePath().toString(), dataPath + ".crc32c");
        String storedChecksum = Files.readString(checksumPath, StandardCharsets.UTF_8).trim();
        Assertions.assertEquals(expectedChecksum, storedChecksum,
            "Stored checksum should match independently computed CRC32C value");
    }

    @Test
    public void testCorruptedDataIsDetected() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);

        String dataPath = UUID.randomUUID().toString();
        ByteBuf content = Unpooled.wrappedBuffer("original data".getBytes());
        storage.put(content, dataPath);
        content.release();

        // Corrupt the data file on disk
        Path dataFilePath = Paths.get(path.toAbsolutePath().toString(), dataPath);
        Files.writeString(dataFilePath, "corrupted data");

        // Reading should detect the corruption and throw
        IOException ex = Assertions.assertThrows(IOException.class, () -> {
            storage.get(dataPath);
        });
        Assertions.assertTrue(ex.getMessage().contains("Checksum"),
            "Exception message should mention checksum verification failure");
    }

    @Test
    public void testBackwardCompatWithoutChecksumFile() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);

        // Write data file directly, bypassing storage (no sidecar checksum)
        String dataPath = UUID.randomUUID().toString();
        byte[] data = "legacy data".getBytes();
        Path dataFilePath = Paths.get(path.toAbsolutePath().toString(), dataPath);
        Files.write(dataFilePath, data);

        // Reading should succeed without checksum file
        ByteBuf readData = storage.get(dataPath);
        byte[] readBytes = new byte[readData.readableBytes()];
        readData.readBytes(readBytes);
        readData.release();
        Assertions.assertArrayEquals(data, readBytes);
    }

    @Test
    public void testDeleteRemovesChecksumFile() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);

        String dataPath = UUID.randomUUID().toString();
        ByteBuf content = Unpooled.wrappedBuffer("delete me".getBytes());
        storage.put(content, dataPath);
        content.release();

        Path dataFilePath = Paths.get(path.toAbsolutePath().toString(), dataPath);
        Path checksumPath = Paths.get(path.toAbsolutePath().toString(), dataPath + ".crc32c");

        // Both files should exist before delete
        Assertions.assertTrue(Files.exists(dataFilePath));
        Assertions.assertTrue(Files.exists(checksumPath));

        storage.delete(dataPath);

        // Both files should be gone after delete
        Assertions.assertFalse(Files.exists(dataFilePath));
        Assertions.assertFalse(Files.exists(checksumPath));
    }
}
