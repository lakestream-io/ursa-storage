/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;

final class IndexFileReader implements AutoCloseable {

    private final URI directory;
    private final String indexFileName;
    private final Configuration configuration;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final GzipCodec gzipCodec = new GzipCodec();
    private final boolean allowApproximateMatching;
    private final int readBufferSize;
    private final List<Map<String, String>> bufferedMetadata = new ArrayList<>();
    private DataInputStream reader;
    private Map<String, Integer> secondaryIndexMap;
    private int cacheRangeStart;
    private int cacheRangeEnd;

    IndexFileReader(URI parquetFile, ReaderConfiguration configuration) {
        Path parquetPath = new Path(parquetFile);
        this.directory = ensureDirectory(parquetPath.getParent().toUri());
        this.indexFileName = parquetPath.getName().replace(".parquet", ".index");
        this.configuration = configuration.hadoopConfiguration();
        this.gzipCodec.setConf(this.configuration);
        this.allowApproximateMatching = configuration.allowApproximateMatching();
        this.readBufferSize = Integer.parseInt(configuration.properties()
                .getProperty("ursaIndexFileReaderBufferSize", String.valueOf(1024 * 1024)));
    }

    int seekBySecondaryIndex(String secondaryIndexValue) throws IOException {
        if (secondaryIndexMap == null) {
            loadSecondaryIndex();
        }
        Integer row;
        if (allowApproximateMatching && secondaryIndexMap instanceof TreeMap<String, Integer> treeMap) {
            Map.Entry<String, Integer> entry = treeMap.floorEntry(secondaryIndexValue);
            row = entry == null ? null : entry.getValue();
        } else {
            row = secondaryIndexMap.get(secondaryIndexValue);
        }
        if (row == null) {
            throw new IOException("Secondary index value not found: " + secondaryIndexValue);
        }
        return row;
    }

    Map<String, String> read(int row) throws IOException {
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
        while (row < cacheRangeStart || row >= cacheRangeEnd) {
            loadBufferedMetadata();
        }
        return bufferedMetadata.get(row - cacheRangeStart);
    }

    private void initReader() throws IOException {
        URI target = directory.resolve(indexFileName);
        FileSystem fileSystem = FileSystem.get(target, configuration);
        Path path = new Path(target);
        if (!fileSystem.exists(path)) {
            throw new IOException("Index file does not exist: " + path);
        }
        reader = new DataInputStream(new BufferedInputStream(
                gzipCodec.createInputStream(fileSystem.open(path)), readBufferSize));
    }

    private void loadSecondaryIndex() throws IOException {
        initReader();
        try {
            int size;
            while ((size = reader.readInt()) != Integer.MAX_VALUE) {
                reader.skipNBytes(size);
            }
            size = reader.readInt();
            byte[] data = reader.readNBytes(size);
            Map<String, Integer> values = objectMapper.readValue(
                    data, new TypeReference<Map<String, Integer>>() { });
            if (allowApproximateMatching) {
                TreeMap<String, Integer> sorted = new TreeMap<>((left, right) -> {
                    if (left.length() != right.length()) {
                        return left.length() - right.length();
                    }
                    return left.compareTo(right);
                });
                sorted.putAll(values);
                secondaryIndexMap = sorted;
            } else {
                secondaryIndexMap = values;
            }
        } finally {
            closeReader();
        }
    }

    private void loadBufferedMetadata() throws IOException {
        int size = reader.readInt();
        if (size == Integer.MAX_VALUE) {
            throw new IOException("Reached end of index file before the requested row");
        }
        byte[] data = reader.readNBytes(size);
        bufferedMetadata.clear();
        bufferedMetadata.addAll(objectMapper.readValue(
                data, new TypeReference<List<Map<String, String>>>() { }));
        if (bufferedMetadata.isEmpty()) {
            throw new IOException("Index file contains an empty metadata block");
        }
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

    private static URI ensureDirectory(URI uri) {
        String value = uri.toString();
        return URI.create(value.endsWith("/") ? value : value + "/");
    }

    @Override
    public void close() throws IOException {
        closeReader();
    }
}
