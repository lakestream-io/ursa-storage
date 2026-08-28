/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGCSFileStorageWithMock {

    private Storage mockStorage;
    private WriteChannel mockWriteChannel;
    private GCSFileStorage gcsFileStorage;
    private StorageConfig config;
    private Blob mockBlob;
    private ReadChannel mockReadChannel;

    @BeforeEach
    void setup() throws Exception {
        mockStorage = mock(Storage.class);
        mockWriteChannel = mock(WriteChannel.class);
        mockBlob = mock(Blob.class);
        mockReadChannel = mock(ReadChannel.class);

        config = new StorageConfig();
        config.setBucket("test-bucket");
        config.setPrefix("test-prefix");
        Properties properties = new Properties();
        properties.setProperty("disableCredential", "true");
        config.setProperties(properties);

        // Mock storage client behavior
        when(mockStorage.writer(any(BlobInfo.class))).thenReturn(mockWriteChannel);
    }

    @Test
    void testPartialWrite() throws Exception {
        // Create test data
        byte[] testData = new byte[1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        ByteBuf data = Unpooled.wrappedBuffer(testData);

        // Configure mock to write partial data
        AtomicInteger bytesWrittenPerCall = new AtomicInteger(0);
        when(mockWriteChannel.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            int remainingBytes = buffer.remaining();
            // Write only 100 bytes at a time
            int bytesToWrite = Math.min(100, remainingBytes);
            buffer.position(buffer.position() + bytesToWrite);
            bytesWrittenPerCall.addAndGet(bytesToWrite);
            return bytesToWrite;
        });

        // Create GCSFileStorage with mocked storage
        gcsFileStorage = new GCSFileStorage(config, InstrumentProvider.NOOP) {
            @Override
            protected Storage buildStorageClient(StorageConfig config) {
                return mockStorage;
            }
        };

        // Perform write operation
        gcsFileStorage.putAsync(data, "test-location").get();

        // Verify all data was written
        Assertions.assertEquals(testData.length, bytesWrittenPerCall.get(),
            "All bytes should be written even with partial writes");

        // Cleanup
        data.release();
        gcsFileStorage.close();
    }

    @Test
    void testWriteWithZeroBytes() throws Exception {
        // Create test data
        byte[] testData = new byte[512];
        ByteBuf data = Unpooled.wrappedBuffer(testData);

        // Configure mock to alternate between writing zero bytes and actual bytes
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger totalBytesWritten = new AtomicInteger(0);

        when(mockWriteChannel.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            int remainingBytes = buffer.remaining();

            // Alternate between writing zero bytes and 50 bytes
            int bytesToWrite = callCount.incrementAndGet() % 2 == 0 ? Math.min(50, remainingBytes) : 0;

            if (bytesToWrite > 0) {
                buffer.position(buffer.position() + bytesToWrite);
                totalBytesWritten.addAndGet(bytesToWrite);
            }
            return bytesToWrite;
        });

        // Create GCSFileStorage with mocked storage
        gcsFileStorage = new GCSFileStorage(config, InstrumentProvider.NOOP) {
            @Override
            protected Storage buildStorageClient(StorageConfig config) {
                return mockStorage;
            }
        };

        // Perform write operation
        gcsFileStorage.putAsync(data, "test-location").get();

        // Verify all data was written despite zero-byte writes
        Assertions.assertEquals(testData.length, totalBytesWritten.get(),
            "All bytes should be written even with zero-byte write attempts");
        Assertions.assertTrue(callCount.get() > testData.length / 50,
            "Should have more write attempts than theoretical minimum due to zero-byte writes");

        // Cleanup
        data.release();
        gcsFileStorage.close();
    }

    @Test
    void testPartialRead() throws Exception {
        // Create test data
        byte[] testData = new byte[1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }

        // Configure mock to read partial data
        AtomicInteger readOffset = new AtomicInteger(0);
        when(mockStorage.get(any(BlobId.class))).thenReturn(mockBlob);
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockBlob.getSize()).thenReturn((long) testData.length);

        when(mockReadChannel.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            int currentOffset = readOffset.get();
            if (currentOffset >= testData.length) {
                return -1; // EOF
            }

            // Read only 100 bytes at a time
            int bytesToRead = Math.min(100, testData.length - currentOffset);
            buffer.put(testData, currentOffset, bytesToRead);
            readOffset.addAndGet(bytesToRead);
            return bytesToRead;
        });

        // Create GCSFileStorage with mocked storage
        gcsFileStorage = new GCSFileStorage(config, InstrumentProvider.NOOP) {
            @Override
            protected Storage buildStorageClient(StorageConfig config) {
                return mockStorage;
            }
        };

        // Perform read operation
        ByteBuf readData = gcsFileStorage.get("test-location");

        // Verify all data was read correctly
        byte[] readBytes = new byte[readData.readableBytes()];
        readData.readBytes(readBytes);
        Assertions.assertArrayEquals(testData, readBytes,
            "All bytes should be read correctly even with partial reads");
        Assertions.assertTrue(readOffset.get() > testData.length / 100,
            "Should have multiple read attempts due to partial reads");

        // Cleanup
        readData.release();
        gcsFileStorage.close();
    }
}
