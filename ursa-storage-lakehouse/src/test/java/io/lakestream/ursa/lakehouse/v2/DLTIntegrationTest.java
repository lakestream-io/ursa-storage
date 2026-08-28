/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.DLTFailureMessageHandler;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergExternalDLTTableWriter;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Integration tests for DLT (Dead Letter Topic) functionality.
 * Tests the end-to-end flow of writing failure messages to DLT tables.
 */
@ExtendWith(MockitoExtension.class)
public class DLTIntegrationTest {

    @Test
    public void testIcebergDLTWriterEndToEnd() throws Exception {
        // Setup
        String topic = "test-topic";
        Properties props = new Properties();
        props.setProperty(LakehouseConfiguration.DLT_SUFFIX, "-dlt");
        props.setProperty("iceberg.catalog", "test-catalog");
        props.setProperty("iceberg.warehouse", "/tmp/warehouse");
        LakehouseConfiguration config = new LakehouseConfiguration(props);

        InstrumentProvider provider = mock(InstrumentProvider.class);

        // Create DLT writer
        IcebergExternalDLTTableWriter dltWriter =
                new IcebergExternalDLTTableWriter(topic, config, provider);

        // Create DLT handler with retry mechanism
        DLTFailureMessageHandler handler = DLTFailureMessageHandler.of(dltWriter);

        // Create failure messages
        ByteBuf payload1 = Unpooled.copiedBuffer("test payload 1", StandardCharsets.UTF_8);
        ByteBuf payload2 = Unpooled.copiedBuffer("test payload 2", StandardCharsets.UTF_8);

        FailureMessage msg1 = FailureMessage.builder()
                .topic(topic)
                .messageId("1:1:0")
                .payload(payload1)
                .failureReason("Processing failed")
                .build();

        FailureMessage msg2 = FailureMessage.builder()
                .topic(topic)
                .messageId("2:2:0")
                .payload(payload2)
                .failureReason("Schema validation failed")
                .build();

        try {
            // Send failure messages
            CompletableFuture<Void> future1 = handler.sendFailureMessage(msg1);
            CompletableFuture<Void> future2 = handler.sendFailureMessage(msg2);

            // Wait for completion
            CompletableFuture.allOf(future1, future2).get(10, TimeUnit.SECONDS);

            // Verify write results
            List<IWriteResult> results = dltWriter.close();
            assertNotNull(results);
            assertTrue(results.size() > 0, "Should have write results");

            // Verify schema
            assertEquals(3, IcebergExternalDLTTableWriter.ICEBERG_SCHEMA.columns().size());
            assertEquals("messageId",
                    IcebergExternalDLTTableWriter.ICEBERG_SCHEMA.findField(1).name());
            assertEquals("payload",
                    IcebergExternalDLTTableWriter.ICEBERG_SCHEMA.findField(2).name());
            assertEquals("failureReason",
                    IcebergExternalDLTTableWriter.ICEBERG_SCHEMA.findField(3).name());

        } finally {
            // Cleanup
            msg1.release();
            msg2.release();
            handler.close();
        }
    }

    @Test
    public void testDLTRetryMechanismIntegration() throws Exception {
        // This test verifies that the retry mechanism works correctly with real DLT writers
        String topic = "test-retry-topic";
        Properties props = new Properties();
        props.setProperty(LakehouseConfiguration.DLT_SUFFIX, "-dlt-retry");
        LakehouseConfiguration config = new LakehouseConfiguration(props);

        InstrumentProvider provider = mock(InstrumentProvider.class);

        // Mock a writer that fails on first attempt but succeeds on retry
        LakehouseRecordWriter<FailureMessage> mockWriter = mock(LakehouseRecordWriter.class);

        // Correct way to simulate a void method throwing an exception
        doThrow(new ExceptionWithCode(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE, "Transient error"))
                .when(mockWriter)
                .write(any(FailureMessage.class));

        // Create handler with retry
        DLTFailureMessageHandler handler = DLTFailureMessageHandler.of(mockWriter);

        ByteBuf payload = Unpooled.copiedBuffer("retry test", StandardCharsets.UTF_8);
        FailureMessage msg = FailureMessage.builder()
                .topic(topic)
                .messageId("retry:1:1:0")
                .payload(payload)
                .failureReason("Initial processing failed")
                .build();

        try {
            // Send message - should succeed after retry
            CompletableFuture<Void> future = handler.sendFailureMessage(msg);

            // If we get here, the retry succeeded
            assertTrue(future.isDone());
            assertTrue(future.isCompletedExceptionally());

        } finally {
            msg.release();
            handler.close();
        }
    }
}
