/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.api.Position;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.delta.DeltaTableUtils;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.materialization.serde.InternalFieldNames;
import io.lakestream.ursa.storage.Key;
import io.lakestream.ursa.storage.OxiaClientFactory;
import io.lakestream.ursa.storage.Value;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.options.GetOption;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

public class CompactTools {

    private final AsyncOxiaClient oxiaClient;

    private final StorageConfig config;

    public CompactTools(StorageConfig config,
                        String oxiaUrl, String namespace) throws ExecutionException, InterruptedException {
        this.config = config;
        try {
            this.oxiaClient = OxiaClientFactory.create(
                    oxiaUrl + "/" + namespace,
                    config.getOxiaStorageConfig(),
                    OpenTelemetry.noop());
        } catch (ExecutionException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create Oxia client.", e);
        }
    }

    public CompactTools(StorageConfig config, AsyncOxiaClient oxiaClient) {
        this.config = config;
        this.oxiaClient = oxiaClient;
    }

    public void verifyTheOxiaIndex(long streamId, String topic, boolean verifyParquetData)
            throws ExecutionException, InterruptedException, IOException {
        GetResult getResult = oxiaClient.get(Key.smallestKey(streamId).toString(),
                Set.of(GetOption.PartitionKey(String.valueOf(streamId)), GetOption.ComparisonCeiling)).get();
        if (getResult == null || !getResult.key().startsWith(String.format("%020d", streamId))) {
            throw new IllegalStateException("The stream didn't contain any oxia index");
        }
        Key baseKey = Key.parse(getResult.key());
        Value baseValue = Value.parse(config.getIndexSerializeFormatVersion(), getResult.value());
        if (baseValue.position().fileType() != Position.FileType.PARQUET) {
            throw new IllegalStateException("The index " + baseKey + " corresponding data file type is not parquet.");
        }
        while (true) {
            if (verifyParquetData) {
                long startOffset = baseKey.offset() - baseValue.numberOfMessages();
                long numberOfMessages = baseValue.numberOfMessages();
                String location = baseValue.position().location();
                ParquetReader<GenericRecord> parquetReader =
                        getParquetReader(config, getBasePath(config, topic) + location);
                int count = 0;
                while (true) {
                    GenericRecord record = parquetReader.read();
                    if (record == null) {
                        break;
                    }
                    Long offset = (Long) record.get(InternalFieldNames.INTERNAL_MESSAGE_OFFSET);
                    if (offset != startOffset + count) {
                        throw new IllegalStateException("The parquet file records is not continuous");
                    }
                    count++;
                }
                if (count != numberOfMessages) {
                    throw new IllegalStateException("The parquet file records number is less not enough.");
                }
            }
            getResult = oxiaClient.get(Key.largestKey(streamId, baseKey.offset()).toString(),
                    Set.of(GetOption.PartitionKey(String.valueOf(streamId)), GetOption.ComparisonHigher)).get();
            if (getResult == null || !getResult.key().startsWith(String.format("%020d", streamId))) {
                break;
            }
            Key key = Key.parse(getResult.key());
            Value value = Value.parse(config.getIndexSerializeFormatVersion(), getResult.value());
            if (value.position().fileType() != Position.FileType.PARQUET) {
                throw new IOException("The index " + key + " corresponding data file type is not parquet.");
            }
            if (baseKey.offset() + baseValue.numberOfMessages() != key.offset()) {
                throw new IllegalStateException(
                        "The oxia index is not continuous. before key " + baseKey + " before value " + baseValue
                                + " current key " + key + " current value " + value);
            }
            baseKey = key;
            baseValue = value;
        }
    }

    private String getBasePath(StorageConfig config, String topic) {
        return DeltaTableUtils.generateTableLocation(LakehouseConfiguration.getStoragePath(config.getProperties()),
                TopicName.get(topic)) + "/";
    }

    public ParquetReader<GenericRecord> getParquetReader(StorageConfig config, String path) throws IOException {
        LakehouseConfiguration lakehouseConfiguration = new LakehouseConfiguration(config.getProperties());
        return AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(new Path(path), lakehouseConfiguration.getHadoopConfiguration()))
                .withDataModel(new GenericData()).build();

    }
}
