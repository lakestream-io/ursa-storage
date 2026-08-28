/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;

public class FailureMessageTest {

    @Test
    void testFailureMessageBuilderWithPayload() {
        ByteBuf payload = Unpooled.copiedBuffer("test payload", StandardCharsets.UTF_8);
        String topic = "test-topic";
        String messageId = "123:456:0";
        String failureReason = "Test failure reason";

        FailureMessage message = FailureMessage.builder()
                .topic(topic)
                .messageId(messageId)
                .payload(payload)
                .failureReason(failureReason)
                .build();

        assertEquals(topic, message.getTopic());
        assertEquals(messageId, message.getMessageId());
        assertEquals(failureReason, message.getFailureReason());
        assertNotNull(message.getEncodedPayload());

        String expectedEncoded = Base64.getEncoder()
                .encodeToString("test payload".getBytes(StandardCharsets.UTF_8));
        assertEquals(expectedEncoded, message.getEncodedPayload());

        message.release();
    }

    @Test
    void testFailureMessageBuilderWithNullPayload() {
        String topic = "test-topic";
        String messageId = "123:456:0";
        String failureReason = "Test failure reason";

        FailureMessage message = FailureMessage.builder()
                .topic(topic)
                .messageId(messageId)
                .payload(null)
                .failureReason(failureReason)
                .build();

        assertEquals(topic, message.getTopic());
        assertEquals(messageId, message.getMessageId());
        assertEquals(failureReason, message.getFailureReason());
        assertNull(message.getEncodedPayload());
    }

    @Test
    void testFailureMessageWithEmptyPayload() {
        ByteBuf payload = Unpooled.EMPTY_BUFFER;

        FailureMessage message = FailureMessage.builder()
                .topic("topic")
                .messageId("1:1:0")
                .payload(payload)
                .failureReason("reason")
                .build();

        assertNull(message.getEncodedPayload());
    }

    @Test
    void testFailureMessageRelease() {
        ByteBuf payload = Unpooled.copiedBuffer("test", StandardCharsets.UTF_8);
        int refCnt = payload.refCnt();

        FailureMessage message = FailureMessage.builder()
                .topic("topic")
                .messageId("1:1:0")
                .payload(payload)
                .failureReason("reason")
                .build();

        message.release();
        assertTrue(refCnt > payload.refCnt());

        message.release(); // safe to call again
    }

    @Test
    void testFailureMessageNoArgsConstructor() {
        FailureMessage message = new FailureMessage();

        assertNull(message.getTopic());
        assertNull(message.getMessageId());
        assertNull(message.getEncodedPayload());
        assertNull(message.getFailureReason());
    }

    @Test
    void testPayloadEncodingWithSpecialCharacters() {
        String specialContent = "Special chars: 你好世界 🌍 \n\t\r";
        ByteBuf payload = Unpooled.copiedBuffer(specialContent, StandardCharsets.UTF_8);

        FailureMessage message = FailureMessage.builder()
                .topic("topic")
                .messageId("1:1:0")
                .payload(payload)
                .failureReason("reason")
                .build();

        assertNotNull(message.getEncodedPayload());
        String expectedEncoded = Base64.getEncoder()
                .encodeToString(specialContent.getBytes(StandardCharsets.UTF_8));
        assertEquals(expectedEncoded, message.getEncodedPayload());

        message.release();
    }

    @Test
    void testPayloadEncodingWithBinaryData() {
        byte[] binaryData = new byte[]{0, 1, 2, 3, -1, -2, -3, 127, -128};
        ByteBuf payload = Unpooled.wrappedBuffer(binaryData);

        FailureMessage message = FailureMessage.builder()
                .topic("topic")
                .messageId("1:1:0")
                .payload(payload)
                .failureReason("reason")
                .build();

        assertNotNull(message.getEncodedPayload());
        String expectedEncoded = Base64.getEncoder().encodeToString(binaryData);
        assertEquals(expectedEncoded, message.getEncodedPayload());

        message.release();
    }

    @Test
    void testPayloadReadIndexPreservation() {
        ByteBuf payload = Unpooled.copiedBuffer("test payload", StandardCharsets.UTF_8);
        payload.readByte();
        int readerIndexBefore = payload.readerIndex();

        FailureMessage message = FailureMessage.builder()
                .topic("topic")
                .messageId("1:1:0")
                .payload(payload)
                .failureReason("reason")
                .build();

        assertNotNull(message.getEncodedPayload());
        assertEquals(readerIndexBefore, payload.readerIndex());

        message.release();
    }
}