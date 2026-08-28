/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.lakehouse.compact.FailureMessageHandler;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryEncoderContext;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class AbstractLakehouseWriterOwnershipTest {

    @Test
    @SuppressWarnings("unchecked")
    void releasesInputWhenContextConstructionFailsBeforeEncoderInvocation() {
        LakehouseConfiguration configuration = mock(LakehouseConfiguration.class);
        when(configuration.isVariantTypeEnabled()).thenThrow(new IllegalStateException("bad configuration"));
        EntryEncoder<Object> encoder = mock(EntryEncoder.class);
        TestWriter writer = new TestWriter(configuration, encoder);
        GenericEntry entry = ownedEntry();
        ByteBuf payload = entry.entry().payload();

        assertThatThrownBy(() -> writer.write(entry))
                .isInstanceOf(LakehouseOptException.class)
                .hasMessageContaining("Error during write");

        assertThat(payload.refCnt()).isZero();
        verifyNoInteractions(encoder);
    }

    @Test
    @SuppressWarnings("unchecked")
    void releasesInputWhenSchemaServiceLookupFailsBeforeEncoderInvocation() {
        LakehouseConfiguration configuration = mock(LakehouseConfiguration.class);
        when(configuration.getProperties()).thenReturn(new Properties());
        EntryEncoder<Object> encoder = mock(EntryEncoder.class);
        TestWriter writer = new TestWriter(configuration, encoder, true);
        GenericEntry entry = ownedEntry();
        ByteBuf payload = entry.entry().payload();

        assertThatThrownBy(() -> writer.write(entry))
                .isInstanceOf(LakehouseOptException.class)
                .hasMessageContaining("Error during write");

        assertThat(payload.refCnt()).isZero();
        verifyNoInteractions(encoder);
    }

    @Test
    void releasesRetainedDltPayloadWhenHandlerThrowsSynchronously() {
        LakehouseConfiguration configuration = mock(LakehouseConfiguration.class);
        when(configuration.getProperties()).thenReturn(new Properties());
        EntryEncoder<Object> failingEncoder = new EntryEncoder<>() {
            @Override
            public void encode(
                    String topic,
                    GenericEntry entry,
                    ResultConsumer<MaterializationRecord<Object>> consumer,
                    TableSchemaService schemaService,
                    EntryEncoderContext context) {
                consumer.onErrorWithCtx(entry, new MessageSerDeException(
                        ExceptionCode.MESSAGE_DESERIALIZE_FROM_SOURCE_ERROR, "bad record"));
            }
        };
        TestWriter writer = new TestWriter(configuration, failingEncoder);
        writer.registerFailureMessageHandler(new FailureMessageHandler() {
            @Override
            public CompletableFuture<Void> sendFailureMessage(FailureMessage failureMessage) {
                throw new IllegalStateException("DLT unavailable");
            }

            @Override
            public void close() {
            }
        });
        GenericEntry entry = ownedEntry();
        ByteBuf payload = entry.entry().payload();

        assertThatThrownBy(() -> writer.write(entry))
                .isInstanceOf(LakehouseOptException.class)
                .hasMessageContaining("Error during write");

        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void usesLogicalSchemaTopicWithoutChangingDestinationIdentity() throws Exception {
        LakehouseConfiguration configuration = mock(LakehouseConfiguration.class);
        when(configuration.getProperties()).thenReturn(new Properties());
        AtomicReference<String> encodedTopic = new AtomicReference<>();
        EntryEncoder<Object> encoder = new EntryEncoder<>() {
            @Override
            public void encode(String topic, GenericEntry entry,
                               ResultConsumer<MaterializationRecord<Object>> consumer,
                               TableSchemaService schemaService, EntryEncoderContext context) {
                encodedTopic.set(topic);
                entry.entry().payload().release();
            }
        };
        TestWriter writer = new TestWriter(
                "default/orders-partition-3-topic-id", "orders", configuration, encoder);

        writer.write(ownedEntry());

        assertThat(writer.topic).isEqualTo("default/orders-partition-3-topic-id");
        assertThat(encodedTopic).hasValue("orders");
    }

    private static GenericEntry ownedEntry() {
        return new GenericEntry(new Entry(
                new EntryHeader(1L, 1, 2L, 1, 1L), Unpooled.buffer().writeByte(1)));
    }

    private static final class TestWriter extends AbstractLakehouseWriter {
        private final boolean failSchemaServiceLookup;

        private TestWriter(LakehouseConfiguration configuration, EntryEncoder<Object> encoder) {
            this(configuration, encoder, false);
        }

        private TestWriter(
                LakehouseConfiguration configuration,
                EntryEncoder<Object> encoder,
                boolean failSchemaServiceLookup) {
            this("default/test-partition-0", "default/test-partition-0",
                    configuration, encoder, failSchemaServiceLookup);
        }

        private TestWriter(String topic, String schemaTopic,
                           LakehouseConfiguration configuration, EntryEncoder<Object> encoder) {
            this(topic, schemaTopic, configuration, encoder, false);
        }

        private TestWriter(String topic, String schemaTopic,
                           LakehouseConfiguration configuration, EntryEncoder<Object> encoder,
                           boolean failSchemaServiceLookup) {
            super(topic, schemaTopic, mock(EntrySerdeFactory.class), configuration, InstrumentProvider.NOOP);
            this.encoder = encoder;
            this.serializeType = EntrySerdeFactory.SerdeType.KAFKA_PARQUET;
            this.failSchemaServiceLookup = failSchemaServiceLookup;
        }

        @Override
        protected void beforeWrite(MaterializationRecord<Object> entry) {
        }

        @Override
        public TableSchemaService getLakehouseTableSchemaService() {
            if (failSchemaServiceLookup) {
                throw new IllegalStateException("schema service unavailable");
            }
            return null;
        }
    }
}
