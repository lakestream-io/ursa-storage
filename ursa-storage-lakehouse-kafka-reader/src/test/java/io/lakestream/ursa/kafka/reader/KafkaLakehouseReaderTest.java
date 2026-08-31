/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import io.lakestream.ursa.compaction.common.ManagedTableFileIndex;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.v2.LakehouseWriter;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetFileWriter;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetWriteResult;
import io.lakestream.ursa.lakehouse.v2.serde.kafka.parquet.KafkaEntryBatchedRawDataToParquetEncoder;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.ResultConsumer;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaLakehouseReaderTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void conversionFailureDoesNotDoubleReleaseTransferredEntries() {
        ByteBuf firstPayload = Unpooled.buffer().writeByte(1);
        ByteBuf trailingPayload = Unpooled.buffer().writeByte(2);
        List<Entry> entries = Arrays.asList(
                new Entry(EntryHeader.NOT_FOUND, firstPayload),
                null,
                new Entry(EntryHeader.NOT_FOUND, trailingPayload));

        assertThatThrownBy(() -> KafkaLakehouseReader.transferEntries(entries))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("convert Kafka V2 lakehouse entries")
                .satisfies(error -> {
                    assertThat(error.getCause()).isInstanceOf(IllegalArgumentException.class);
                    assertThat(error.getCause().getSuppressed()).isEmpty();
                });

        assertThat(firstPayload.refCnt()).isZero();
        assertThat(trailingPayload.refCnt()).isZero();
    }

    @Test
    void readsFromInsideBatchUsingFileIndexProducedByV2Writer() throws Exception {
        byte[] expectedPayload = {1, 2, 3};
        EntryHeader header = new EntryHeader(10, 2, 1234, 3, 3);
        ParquetWriteResult writeResult = writeWithLakehouseWriter(
                temporaryDirectory, header, expectedPayload);
        long inclusiveEndOffset = (long) writeResult.getExtraMetadata().get(
                AbstractLakehouseWriter.LAST_ENTRY_ID_IN_FILE);
        assertThat(inclusiveEndOffset).isEqualTo(11);

        ManagedTableFileIndex fileIndex = new ManagedTableFileIndex();
        fileIndex.append(inclusiveEndOffset, writeResult.getDataFile());
        EntryIndex entryIndex = new EntryIndex(
                header, new Position("ignored"), 1, EntryIndex.IndexType.COMPACT,
                Optional.empty(), Optional.of(Map.of(
                        ManagedTableFileIndex.NAME, fileIndex.serializeToString())));

        KafkaLakehouseReaderFactory factory = new KafkaLakehouseReaderFactory();
        Properties properties = new Properties();
        properties.setProperty("storagePath", temporaryDirectory.toString());
        factory.initialize(properties, InstrumentProvider.NOOP);
        CompactedObjectReader reader = factory.open("default/orders-partition-0");

        CompactedObjectReader.ReadResult result = reader.readMessagesWithEntryIndexAsync(
                entryIndex, 11, 10, 10, 1024).join();
        ByteBuf returnedPayload = result.entries().get(0).payload();
        try {
            assertThat(result.isV2Result()).isTrue();
            assertThat(result.entries()).hasSize(1);
            assertThat(result.entries().get(0).offset()).isEqualTo(10);
            assertThat(result.entries().get(0).numberOfRecords()).isEqualTo(2);
            assertThat(ByteBufUtil.getBytes(returnedPayload)).containsExactly(expectedPayload);
        } finally {
            result.entries().forEach(entry -> entry.close());
            assertThat(returnedPayload.refCnt()).isZero();
            reader.close();
            factory.close();
        }
    }

    private static ParquetWriteResult writeWithLakehouseWriter(
            Path storageRoot, EntryHeader header, byte[] payloadBytes) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storagePath", storageRoot.toString());
        properties.setProperty("entrySerDeType", "KAFKA_BATCHED_RAW_PARQUET");
        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        EntrySerdeFactory serdeFactory = new EntrySerdeFactory(null);
        LakehouseWriter writer = new LakehouseWriter(
                "default/orders-partition-0", serdeFactory, configuration);
        io.lakestream.ursa.storage.Entry source = new io.lakestream.ursa.storage.Entry(
                header, Unpooled.wrappedBuffer(payloadBytes));
        try {
            writer.write(new GenericEntry(source));
            List<IWriteResult> writeResults = writer.close();
            assertThat(writeResults).hasSize(1);
            assertThat(writeResults.get(0)).isInstanceOf(ParquetWriteResult.class);
            return (ParquetWriteResult) writeResults.get(0);
        } finally {
            serdeFactory.close();
            if (source.payload().refCnt() > 0) {
                source.payload().release();
            }
        }
    }

    @Test
    void rejectsEntryIndexesWithoutTheKafkaV2ManagedTableFileIndex() throws Exception {
        KafkaLakehouseReaderFactory factory = new KafkaLakehouseReaderFactory();
        Properties properties = new Properties();
        properties.setProperty("storagePath", temporaryDirectory.toString());
        factory.initialize(properties, InstrumentProvider.NOOP);
        CompactedObjectReader reader = factory.open("default/orders");
        EntryHeader header = new EntryHeader(0, 1, 0, 1, 1);
        EntryIndex legacyEntryIndex = new EntryIndex(
                header, new Position("legacy.parquet"), 1, EntryIndex.IndexType.COMPACT,
                Optional.empty(), Optional.empty());

        assertThatThrownBy(() -> reader.readMessagesWithEntryIndexAsync(
                legacyEntryIndex, 0, 0, 1, 1024).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("V2 KAFKA_BATCHED_RAW_PARQUET");

        reader.close();
        factory.close();
    }

    @Test
    void rejectsMalformedLegacyMetadataWithoutLeakingTheReader() throws Exception {
        Path topicDirectory = temporaryDirectory.resolve("default/orders");
        Files.createDirectories(topicDirectory);
        EntryHeader header = new EntryHeader(10, 1, 1234, 3, 3);
        String dataFile = writeWithLegacyLakehouseWriter(topicDirectory, header, new byte[] {1, 2, 3}, false);
        ManagedTableFileIndex fileIndex = new ManagedTableFileIndex();
        fileIndex.append(10, dataFile);
        EntryIndex entryIndex = new EntryIndex(
                header, new Position("ignored"), 1, EntryIndex.IndexType.COMPACT,
                Optional.empty(), Optional.of(Map.of(
                        ManagedTableFileIndex.NAME, fileIndex.serializeToString())));

        KafkaLakehouseReaderFactory factory = new KafkaLakehouseReaderFactory();
        Properties properties = new Properties();
        properties.setProperty("storagePath", temporaryDirectory.toString());
        factory.initialize(properties, InstrumentProvider.NOOP);
        CompactedObjectReader reader = factory.open("default/orders-partition-0");
        try {
            assertThatThrownBy(() -> reader.readMessagesWithEntryIndexAsync(
                    entryIndex, 10, 10, 1, 1024).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);
        } finally {
            reader.close();
            factory.close();
        }
    }

    @Test
    void reportsExecutorRejectionAsAFailedFutureAfterFactoryClose() throws Exception {
        KafkaLakehouseReaderFactory factory = new KafkaLakehouseReaderFactory();
        Properties properties = new Properties();
        properties.setProperty("storagePath", temporaryDirectory.toString());
        factory.initialize(properties, InstrumentProvider.NOOP);
        KafkaLakehouseReader reader = (KafkaLakehouseReader) factory.open(
                "default/orders-partition-0");
        factory.close();

        ManagedTableFileIndex fileIndex = new ManagedTableFileIndex();
        fileIndex.append(0, "unused.parquet");
        EntryHeader header = new EntryHeader(0, 1, 0, 1, 1);
        EntryIndex entryIndex = new EntryIndex(
                header, new Position("ignored"), 1, EntryIndex.IndexType.COMPACT,
                Optional.empty(), Optional.of(Map.of(
                        ManagedTableFileIndex.NAME, fileIndex.serializeToString())));

        assertThatThrownBy(() -> reader.readMessagesWithEntryIndexAsync(
                entryIndex, 0, 0, 1, 1).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(RejectedExecutionException.class);
        reader.close();
    }

    private static String writeWithLegacyLakehouseWriter(
            Path directory, EntryHeader header, byte[] payloadBytes, boolean validMetadata) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storagePath", directory.toString());
        LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
        ParquetFileWriter<GenericRecord> writer = new ParquetFileWriter<>(
                directory.toUri(), configuration, InstrumentProvider.NOOP);
        writer.addExtraMetadataAtFile("entrySerDeType", "KAFKA_BATCHED_RAW_PARQUET");
        writer.setSecondaryIndexKey("offset");

        KafkaEntryBatchedRawDataToParquetEncoder encoder = new KafkaEntryBatchedRawDataToParquetEncoder();
        io.lakestream.ursa.storage.Entry source = new io.lakestream.ursa.storage.Entry(
                header, Unpooled.wrappedBuffer(payloadBytes));
        encoder.encode("default/orders", new GenericEntry(source),
                new ResultConsumer<MaterializationRecord<GenericRecord>>() {
                    @Override
                    public void onResult(MaterializationRecord<GenericRecord> result) {
                        try {
                            writer.write(result.record(), Map.of(
                                    "metadata", validMetadata
                                            ? result.metadata().orElseThrow().serializeTo() : "not-base64",
                                    "offset", String.valueOf(header.offset())));
                        } catch (Exception error) {
                            throw new IllegalStateException(error);
                        }
                    }

                    @Override
                    public void onErrorWithCtx(Object context, Throwable error) {
                        throw new IllegalStateException(error);
                    }
                });
        assertThat(source.payload().refCnt()).isZero();
        IWriteResult writeResult = writer.close().get(0);
        return ((ParquetWriteResult) writeResult).getDataFile();
    }
}
