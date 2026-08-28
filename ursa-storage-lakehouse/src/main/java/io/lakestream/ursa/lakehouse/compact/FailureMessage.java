/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.buffer.ByteBuf;
import java.util.Base64;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class FailureMessage {
    private String topic;
    private String messageId;

    @Getter(AccessLevel.NONE)
    @JsonIgnore
    private ByteBuf payload;

    private String encodedPayload;
    private String failureReason;

    @Builder
    public FailureMessage(String topic, String messageId, ByteBuf payload, String failureReason) {
        this.topic = topic;
        this.messageId = messageId;
        this.payload = payload;
        this.failureReason = failureReason;
        this.encodedPayload = encodePayload(payload);
    }

    private static String encodePayload(ByteBuf buf) {
        if (buf == null || !buf.isReadable()) {
            return null;
        }
        int readerIndex = buf.readerIndex();
        try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        } finally {
            buf.readerIndex(readerIndex); // Reset reader index
        }
    }

    public void release() {
        if (payload != null) {
            payload.release();
            payload = null;
        }
    }
}