/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;

public class IndexFileWriter implements AutoCloseable {

    private final URI directory;
    private final String indexFileName;
    private final Configuration configuration;
    private final ObjectMapper objectMapper;
    private DataOutputStream writer;
    private int writeIdx = 0;
    private String secondaryIndexKey;
    private final Map<String, Integer> secondaryIndexMap = new HashMap<>();
    private final GzipCodec gzipCodec;

    private List<Map<String, String>> bufferedMeatdata = new ArrayList<>();

    private final int writeBufferSize;
    private final int maxBufferedRecords;

    IndexFileWriter(URI directory, String indexFileName, LakehouseConfiguration conf) {
        this.directory = Utils.ensureIsDirectory(directory);
        this.indexFileName = indexFileName;
        this.configuration = conf.getHadoopConfiguration();
        this.objectMapper = new ObjectMapper();
        this.gzipCodec = new GzipCodec();
        this.gzipCodec.setConf(configuration);
        var bufferSizeStr = conf.getProperties()
            .getProperty("ursaIndexFileWriterBufferSize", String.valueOf(1024 * 1024));
        var maxBufferedRecordsStr = conf.getProperties()
            .getProperty("ursaIndexFileWriterMaxBufferedRecords", String.valueOf(10000));
        this.writeBufferSize = Integer.parseInt(bufferSizeStr);
        this.maxBufferedRecords = Integer.parseInt(maxBufferedRecordsStr);
    }

    void setSecondaryIndexKey(String secondaryIndexKey) {
        this.secondaryIndexKey = secondaryIndexKey;
    }

    public void write(Map<String, String> metadata) throws IOException {
        if (writer == null) {
            initWriter();
        }
        if (secondaryIndexKey != null) {
            var secondaryIndexValue = metadata.get(secondaryIndexKey);
            secondaryIndexMap.computeIfAbsent(secondaryIndexValue, k -> writeIdx);
        }
        bufferedMeatdata.add(metadata);
        if (bufferedMeatdata.size() == maxBufferedRecords) {
            flush();
        }
        writeIdx++;
    }

    private void flush() throws IOException {
        var data = objectMapper.writeValueAsBytes(bufferedMeatdata);
        writer.writeInt(data.length);
        writer.write(data);
        bufferedMeatdata.clear();
    }

    private void initWriter() throws IOException {
        var target = directory.resolve(indexFileName);
        var fs = FileSystem.get(target, configuration);
        var outputStream = fs.create(new Path(target), false);
        var compressionOutput = gzipCodec.createOutputStream(outputStream);
        writer = new DataOutputStream(new BufferedOutputStream(compressionOutput, writeBufferSize));
    }

    private void writeSecondaryIndex() throws IOException {
        writer.writeInt(Integer.MAX_VALUE);
        var data = objectMapper.writeValueAsBytes(secondaryIndexMap);
        writer.writeInt(data.length);
        writer.write(data);
    }

    String getIndexFileName() {
        return indexFileName;
    }

    int getWriteBufferSize() {
        return writeBufferSize;
    }

    int getMaxBufferedRecords() {
        return maxBufferedRecords;
    }

    public void close() throws IOException {
        if (writer != null) {
            flush();
            writeSecondaryIndex();
            writer.flush();
            writer.close();
        }
    }
}
