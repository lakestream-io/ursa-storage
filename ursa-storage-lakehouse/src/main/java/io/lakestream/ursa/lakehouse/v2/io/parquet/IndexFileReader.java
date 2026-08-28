/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;

@Slf4j
public class IndexFileReader implements AutoCloseable {

    private final URI directory;
    private final String indexFileName;
    private final Configuration configuration;
    private final ObjectMapper objectMapper;
    private DataInputStream reader;
    private final GzipCodec gzipCodec;
    private Map<String, Integer> secondaryIndexMap;
    private final boolean allowApproximateMatching;

    private final List<Map<String, String>> bufferedMetadata = new ArrayList<>();
    private int cacheRangeStart = 0;
    private int cacheRangeEnd = 0;

    private final int readBufferSize;

    IndexFileReader(URI directory, String indexFileName, LakehouseConfiguration conf) {
        this.directory = Utils.ensureIsDirectory(directory);
        this.indexFileName = indexFileName;
        this.configuration = conf.getHadoopConfiguration();
        this.objectMapper = new ObjectMapper();
        this.gzipCodec = new GzipCodec();
        this.gzipCodec.setConf(configuration);
        var bufferSizeStr = conf.getProperties()
            .getProperty("ursaIndexFileReaderBufferSize", String.valueOf(1024 * 1024));
        this.readBufferSize = Integer.parseInt(bufferSizeStr);
        this.allowApproximateMatching = conf.allowApproximateMatching();
    }

    public int seekBySecondaryIndex(String secondaryIndexValue) throws IOException {
        if (secondaryIndexMap == null) {
            loadSecondaryIndex();
        }

        Integer row = null;
        if (allowApproximateMatching && (secondaryIndexMap instanceof TreeMap<String, Integer>)) {
            var entry = ((TreeMap<String, Integer>) secondaryIndexMap).floorEntry(secondaryIndexValue);
            if (entry != null) {
                row = entry.getValue();
            }
        } else {
            row = secondaryIndexMap.get(secondaryIndexValue);
        }
        if (row != null) {
            return row;
        } else {
            throw new IOException("Secondary index value not found: " + secondaryIndexValue);
        }
    }

    public Map<String, String> read(int row) throws IOException {
        if (row < 0) {
            throw new IOException("Row index cannot be negative: " + row);
        }

        if (reader == null) {
            initReader();
            loadBufferedMetadata();
        }

        if (row < cacheRangeStart) {
            clearCache();
            closeReader();
            initReader();
            loadBufferedMetadata();
        }

        if (!inCache(row)) {
            seekToTheBlock(row);
        }

        return getFromCache(row);
    }

    private boolean inCache(int row) {
        return cacheRangeStart <= row && cacheRangeEnd > row;
    }

    private Map<String, String> getFromCache(int row) {
        var idx = row - cacheRangeStart;
        return bufferedMetadata.get(idx);
    }

    private void seekToTheBlock(int row) throws IOException {
        while (!inCache(row)) {
            loadBufferedMetadata();
        }
    }

    private void initReader() throws IOException {
        var target = directory.resolve(indexFileName);
        var fs = FileSystem.get(target, configuration);
        var path = new Path(target);

        if (!fs.exists(path)) {
            throw new IOException("Index file does not exist: " + path);
        }

        var inputStream = fs.open(path);
        var compressionInput = gzipCodec.createInputStream(inputStream);
        reader = new DataInputStream(new BufferedInputStream(compressionInput, readBufferSize));
    }

    // todo: async load secondary index when open the file.
    private void loadSecondaryIndex() throws IOException {
        if (secondaryIndexMap == null) {
            long start = System.currentTimeMillis();
            if (reader == null) {
                initReader();
            }
            int size = 0;
            while ((size = reader.readInt()) != Integer.MAX_VALUE) {
                reader.skipBytes(size);
            }
            size = reader.readInt();
            var data = reader.readNBytes(size);
            var values = objectMapper.readValue(data, new TypeReference<Map<String, Integer>>(){});
            if (allowApproximateMatching) {
                var treeMap = new TreeMap<String, Integer>((s1, s2) -> {
                    if (s1.length() != s2.length()) {
                        return s1.length() - s2.length();
                    }
                    return s1.compareTo(s2);
                });
                treeMap.putAll(values);
                secondaryIndexMap = treeMap;
            } else {
                secondaryIndexMap = values;
            }
            closeReader();
        }
    }

    private void loadBufferedMetadata() throws IOException {
        var size = reader.readInt();
        if (size == Integer.MAX_VALUE) {
            throw new IOException("Reached end of index file or no more metadata available.");
        }
        var data = reader.readNBytes(size);
        bufferedMetadata.clear();
        bufferedMetadata.addAll(objectMapper.readValue(data, new TypeReference<List<Map<String, String>>>(){}));
        cacheRangeStart = cacheRangeEnd;
        cacheRangeEnd += bufferedMetadata.size();
    }

    private void clearCache() {
        bufferedMetadata.clear();
        cacheRangeStart = 0;
        cacheRangeEnd = 0;
    }

    private void closeReader() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    int getReadBufferSize() {
        return readBufferSize;
    }

    @Override
    public void close() throws IOException {
        closeReader();
    }
}
