/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.metrics.Counter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.metrics.LatencyHistogram;
import io.lakestream.ursa.metrics.Unit;
import io.lakestream.ursa.storage.FileStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.zip.CRC32C;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class LocalFileStorage implements FileStorage {

    private final String basePath;

    // otel metrics
    private final LatencyHistogram writeStorageLatency;
    private final LatencyHistogram readStorageLatency;
    private final LatencyHistogram readMetadataStorageLatency;
    private final LatencyHistogram deleteStorageLatency;
    private final Counter writeBytesCount;
    private final Counter readBytesCount;

    private static final String CHECKSUM_EXTENSION = ".crc32c";

    private final Map<String, Map<String, String>> putMetadata = new HashMap<>();

    public LocalFileStorage(StorageConfig configuration, InstrumentProvider instrumentProvider) {
        this.basePath = configuration.getStoragePath();
        if (basePath == null) {
            throw new IllegalArgumentException("Need config: fileStoragePath");
        }

        // otel metrics
        writeStorageLatency = instrumentProvider.newLatencyHistogram("ursa.storage.backend.write.duration",
            "FileBackendStorage write latency", Attributes.of(AttributeKey.stringKey("type"), "LOCAL"));
        readStorageLatency = instrumentProvider.newLatencyHistogram("ursa.storage.backend.read.duration",
            "FileBackendStorage read latency", Attributes.of(AttributeKey.stringKey("type"), "LOCAL"));
        readMetadataStorageLatency =
                instrumentProvider.newLatencyHistogram("ursa.storage.backend.metadata.read.duration",
                        "FileBackendStorage read metadata latency",
                        Attributes.of(AttributeKey.stringKey("type"), "LOCAL"));
        deleteStorageLatency = instrumentProvider.newLatencyHistogram("ursa.storage.backend.delete.duration",
            "FileBackendStorage delete latency", Attributes.of(AttributeKey.stringKey("type"), "LOCAL"));
        writeBytesCount = instrumentProvider.newCounter("ursa.storage.backend.write.bytes.count",
            Unit.Bytes, "FileBackendStorage write bytes count", Attributes.of(AttributeKey.stringKey("type"), "LOCAL"));
        readBytesCount = instrumentProvider.newCounter("ursa.storage.backend.read.bytes.count",
            Unit.Bytes, "FileBackendStorage read bytes count", Attributes.of(AttributeKey.stringKey("type"), "LOCAL"));
    }

    @Override
    public void put(ByteBuf data, String location) throws IOException {
        long start = System.nanoTime();
        Path writePath = Paths.get(basePath, location);
        boolean exists = Files.exists(writePath);
        if (exists) {
            writeStorageLatency.recordFailure(System.nanoTime() - start);
            throw new FileAlreadyExistsException(writePath.toString());
        }
        FileChannel fc = null;
        try {
            // todo: put the extra metadata into the file as well.
            fc = FileChannel.open(writePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            fc.write(data.nioBuffer());
            fc.force(true);
            try {
                writeChecksumSidecar(writePath, data);
            } catch (IOException e) {
                log.warn("Failed to write checksum sidecar for {}, data written successfully", writePath, e);
            }
            writeStorageLatency.recordSuccess(System.nanoTime() - start);
            writeBytesCount.add(data.readableBytes());
        } catch (IOException e) {
            writeStorageLatency.recordFailure(System.nanoTime() - start);
            throw e;
        } finally {
            if (fc != null) {
                fc.close();
            }
        }
    }

    @Override
    public CompletableFuture<Void> putAsync(ByteBuf data, String location) {
        CompletableFuture<Void> res = new CompletableFuture<>();
        long start = System.nanoTime();
        Path writePath = Paths.get(basePath, location);
        boolean exists = Files.exists(writePath);
        if (exists) {
            writeStorageLatency.recordFailure(System.nanoTime() - start);
            res.completeExceptionally(new FileAlreadyExistsException(writePath.toString()));
            return res;
        }
        FileChannel fc = null;
        try {
            Files.createDirectories(writePath.getParent());
            // todo: put the extra metadata into the file as well.
            fc = FileChannel.open(writePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            fc.write(data.nioBuffer());
            fc.force(true);
            try {
                writeChecksumSidecar(writePath, data);
            } catch (IOException e) {
                log.warn("Failed to write checksum sidecar for {}, data written successfully", writePath, e);
            }
            writeStorageLatency.recordSuccess(System.nanoTime() - start);
            writeBytesCount.add(data.readableBytes());
            res.complete(null);
        } catch (IOException e) {
            writeStorageLatency.recordFailure(System.nanoTime() - start);
            res.completeExceptionally(e);
        } finally {
            if (fc != null) {
                try {
                    fc.close();
                } catch (IOException e) {
                    log.error("Failed to put data to the file {}", location);
                }
            }
        }
        return res;
    }

    @Override
    public ByteBuf get(String location) throws IOException {
        return getWithMetadata(location).getLeft();
    }

    @Override
    public CompletableFuture<ByteBuf> getAsync(String location) {
        CompletableFuture<ByteBuf> res = new CompletableFuture<>();
        try {
            res.complete(getWithMetadata(location).getLeft());
        } catch (IOException e) {
            res.completeExceptionally(e);
        }
        return res;
    }

    public Pair<ByteBuf, Map<String, String>> getWithMetadata(String location) throws IOException {
        long start = System.nanoTime();
        Path readPath = Paths.get(basePath, location);
        boolean exists = Files.exists(readPath);
        if (!exists) {
            readStorageLatency.recordFailure(System.nanoTime() - start);
            throw new FileNotFoundException(readPath.toString());
        }

        FileChannel fc = FileChannel.open(readPath, StandardOpenOption.READ);
        ByteBuffer buffer = ByteBuffer.allocate((int) fc.size());
        try {
            String creationTime = Files.readAttributes(readPath, BasicFileAttributes.class)
                .creationTime().toInstant().toString();
            Map<String, String> metadata = Collections.singletonMap(METADATA_CREATION_TIME, creationTime);
            fc.read(buffer);
            buffer.flip();
            ByteBuf payload = Unpooled.wrappedBuffer(buffer);
            verifyChecksumIfPresent(readPath, payload);
            readStorageLatency.recordSuccess(System.nanoTime() - start);
            readBytesCount.add(payload.readableBytes());
            return Pair.of(payload, metadata);
        } catch (IOException e) {
            readStorageLatency.recordFailure(System.nanoTime() - start);
            throw e;
        } finally {
            fc.close();
        }
    }

    @Override
    public void delete(String location) throws IOException {
        long start = System.nanoTime();
        Path deletePath = Paths.get(basePath, location);
        boolean exists = Files.exists(deletePath);
        if (!exists) {
            return;
        }
        Files.delete(deletePath);
        try {
            Path checksumPath = Paths.get(basePath, location + CHECKSUM_EXTENSION);
            Files.deleteIfExists(checksumPath);
        } catch (IOException e) {
            log.warn("Failed to delete checksum sidecar for {}", location, e);
        }
        deleteStorageLatency.recordSuccess(System.nanoTime() - start);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(List<String> locations) {
        for (String location : locations) {
            try {
                delete(location);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private static String calculateCRC32C(ByteBuf data) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(data.nioBuffer());
        return Long.toString(crc32c.getValue());
    }

    private static void writeChecksumSidecar(Path dataPath, ByteBuf data) throws IOException {
        Path checksumPath = dataPath.resolveSibling(dataPath.getFileName() + CHECKSUM_EXTENSION);
        Path tmpPath = dataPath.resolveSibling(dataPath.getFileName() + CHECKSUM_EXTENSION + ".tmp");
        String checksum = calculateCRC32C(data);
        Files.writeString(tmpPath, checksum, StandardCharsets.UTF_8);
        try (FileChannel fc = FileChannel.open(tmpPath, StandardOpenOption.WRITE)) {
            fc.force(true);
        }
        Files.move(tmpPath, checksumPath, StandardCopyOption.ATOMIC_MOVE);
    }

    private void verifyChecksumIfPresent(Path dataPath, ByteBuf data) throws IOException {
        Path checksumPath = dataPath.resolveSibling(dataPath.getFileName() + CHECKSUM_EXTENSION);
        if (!Files.exists(checksumPath)) {
            log.debug("No checksum sidecar found for {}, skipping verification", dataPath.getFileName());
            return;
        }
        String storedChecksum = Files.readString(checksumPath, StandardCharsets.UTF_8).trim();
        String computedChecksum = calculateCRC32C(data);
        if (!storedChecksum.equals(computedChecksum)) {
            throw new IOException("Checksum verification failed for " + dataPath.getFileName()
                + ": expected " + storedChecksum + " but computed " + computedChecksum);
        }
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
