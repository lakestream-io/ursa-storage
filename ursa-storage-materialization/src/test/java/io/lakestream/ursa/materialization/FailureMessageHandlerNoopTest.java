/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableCatalogType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class FailureMessageHandlerNoopTest {

    @Test
    void sendReturnsCompletedFuture() throws Exception {
        FailureMessageHandler handler = FailureMessageHandler.noop();

        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{1});
        try {
            FailureRecord record = new FailureRecord(
                    StreamIdentifier.of("public/default", "events"),
                    TableCatalogType.DELTA,
                    Optional.empty(),
                    "test",
                    payload);

            CompletableFuture<Void> future = handler.sendFailureMessage(record);
            assertThat(future).isCompleted();
            assertThat(future.get()).isNull();
        } finally {
            payload.release();
        }
    }

    @Test
    void closeIsNoOp() {
        FailureMessageHandler handler = FailureMessageHandler.noop();
        assertThatCode(handler::close).doesNotThrowAnyException();
    }
}
