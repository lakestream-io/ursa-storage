/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;

import com.github.luben.zstd.Zstd;
import com.google.common.annotations.VisibleForTesting;
import io.lakestream.api.FileInfo;
import io.lakestream.api.Position;
import io.lakestream.ursa.storage.proto.CompressionType;
import io.lakestream.ursa.storage.proto.EntryIndex;
import io.lakestream.ursa.storage.proto.ExtraData;
import io.lakestream.ursa.storage.proto.FileType;
import io.lakestream.ursa.storage.proto.IndexType;
import io.lakestream.ursa.storage.proto.Version;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import me.lemire.integercompression.differential.IntegratedIntCompressor;

/**
 * Represents a value in the storage system, containing information about messages and their position.
 * This class is immutable and thread-safe due to its record nature.
 *
 * @param numberOfMessages The number of messages associated with this value
 * @param entrySize The size of the entry in bytes
 * @param entryCount The count of the entry
 * @param indexType The index type
 * @param position The position of the entry in the storage system
 * @param position the entry offset array in this index
 */
public record Value(long numberOfMessages, long entrySize, int entryCount, IndexType indexType, Position position,
                    Optional<int[]> entryOffsets, Optional<Map<String, String>> extraData) {

    public Value(long numberOfMessages, long entrySize, int entryCount, IndexType indexType, Position position) {
        this(numberOfMessages, entrySize, entryCount, indexType, position, Optional.empty());
    }

    public Value(long numberOfMessages, long entrySize, int entryCount, IndexType indexType, Position position,
                 Optional<int[]> entryOffsets) {
        this(numberOfMessages, entrySize, entryCount, indexType, position, entryOffsets, Optional.empty());
    }



    /**
     * Returns a string representation of the Value.
     * The format is: "{numberOfMessages}-{entrySize}-{position}"
     * Each numeric field is padded to 20 digits for consistent length.
     *
     * @return A string representation of the Value
     */
    private String toString(int formatVersion) {
        return String.format("%020d-%020d-%s", numberOfMessages, entrySize, position.toString(formatVersion));
    }

    /**
     * Converts the Value to a byte array using UTF-8 encoding.
     * This is useful for serialization and storage purposes.
     *
     * @return A byte array representation of the Value
     */
    public byte[] toBytes(int formatVersion) {
        if (formatVersion >= PROTOBUF_VERSION) {
            ByteBuf buf = null;
            try {
                EntryIndex entryIndex = new EntryIndex();
                entryIndex.setVersion(Version.valueOf(formatVersion));
                entryIndex.setOffsetInFile(position.indexId());
                entryIndex.setMessageCount((int) numberOfMessages);
                entryIndex.setEntrySizeBytes(entrySize);
                entryIndex.setLocation(position.location());
                entryIndex.setFileType(FileType.valueOf(position.fileType().name()));
                entryIndex.setIndexType(IndexType.valueOf(indexType.name()));
                entryIndex.setFileSize(position.size());
                entryIndex.setEntryCount(entryCount);
                if (entryOffsets.isPresent()) {
                    var arr = entryOffsets.get();
                    var entryOffsets = entryIndex.setEntryOffsets();
                    int uncompressedSize = arr.length * 4;
                    buf = compressEntryOffsetsByFastPFOR(arr);
                    entryOffsets.setCompressionType(CompressionType.FastPFOR);
                    entryOffsets.setUncompressedSize(uncompressedSize);
                    entryOffsets.setCompressedPayload(buf);
                }
                if (extraData.isPresent() && !extraData.get().isEmpty()) {
                    for (Map.Entry<String, String> entry: extraData.get().entrySet()) {
                        entryIndex.addExtraData().setKey(entry.getKey()).setValue(entry.getValue());
                    }
                }
                return entryIndex.toByteArray();
            } finally {
                if (buf != null) {
                    buf.release();
                }
            }
        }
        return this.toString(formatVersion).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses a byte array to create a Value object.
     * The byte array should be in the format produced by the toBytes() method.
     *
     * @param formatVersion The format version used for deserialization of the Value
     * @param value         The byte array to parse
     * @return A new Value object
     * @throws NumberFormatException     if the numeric fields cannot be parsed
     * @throws IndexOutOfBoundsException if the byte array is not in the expected format
     */
    public static Value parse(int formatVersion, byte[] value) {
        if (formatVersion >= PROTOBUF_VERSION) {
            EntryIndex entryIndex = new EntryIndex();
            entryIndex.parseFrom(value);
            long numberOfMessages = entryIndex.getMessageCount();
            long entrySize = entryIndex.getEntrySizeBytes();
            Position.FileType fileType = Position.FileType.valueOf(entryIndex.getFileType().name());
            FileInfo fileInfo = new FileInfo(entryIndex.getLocation(), entryIndex.getFileSize());
            var pos = new Position(fileInfo, entryIndex.getOffsetInFile(), fileType);
            //var pos = new Position(fileInfo, -1, fileType);

            int[] entryOffsets = null;
            if (entryIndex.hasEntryOffsets()) {
                entryOffsets =
                        uncompressEntryOffsetsByFastPFOR(entryIndex.getEntryOffsets().getCompressedPayloadSlice());
            }
            Map<String, String> extraData = null;
            if (entryIndex.getExtraDatasCount() > 0) {
                var map = entryIndex.getExtraDatasList().stream()
                    .collect(Collectors.toMap(ExtraData::getKey, ExtraData::getValue));
                extraData = Collections.unmodifiableMap(map);
            }
            return new Value(numberOfMessages, entrySize, entryIndex.getEntryCount(), entryIndex.getIndexType(), pos,
                Optional.ofNullable(entryOffsets), Optional.ofNullable(extraData));
        }
        String v = new String(value, StandardCharsets.UTF_8);
        long numberOfMessages = Long.parseLong(v.substring(0, 20));
        long entrySize = Long.parseLong(v.substring(21, 41));
        String position = v.substring(42);

        if (formatVersion == 1) {
            var positionObj = Position.parseV1Format(position);
            var indexType = positionObj.fileType() == Position.FileType.PARQUET ? IndexType.COMPACT : IndexType.NORMAL;
            return new Value(numberOfMessages, entrySize, 1, indexType, positionObj,
                    Optional.empty());
        }
        if (formatVersion == 2) {
            var positionObj = Position.parseV2Format(position);
            var indexType = positionObj.fileType() == Position.FileType.PARQUET ? IndexType.COMPACT : IndexType.NORMAL;
            return new Value(numberOfMessages, entrySize, 1, indexType, positionObj,
                    Optional.empty());
        }
        throw new IllegalArgumentException("Invalid format version: " + formatVersion);
    }

    public static Value of(io.lakestream.api.EntryIndex index) {
        var header = index.header();
        var position = index.position();
        return new Value(
                header.numberOfMessages(),
                header.entrySize(),
                index.entryCount(),
                IndexType.valueOf(index.indexType().name()),
                position,
                index.entryOffsets());
    }

    private static final IntegratedIntCompressor INT_COMPRESSOR = new IntegratedIntCompressor();

    @VisibleForTesting
    static int[] uncompressEntryOffsetsByFastPFOR(ByteBuf compressed) {
        int[] compressedArr = new int[compressed.readableBytes() / 4];
        for (int i = 0; compressed.readableBytes() > 0; i++) {
            compressedArr[i] = compressed.readInt();
        }
        return INT_COMPRESSOR.uncompress(compressedArr);
    }

    @VisibleForTesting
    static ByteBuf compressEntryOffsetsByFastPFOR(int[] entryOffsets) {
        int[] compressed = INT_COMPRESSOR.compress(entryOffsets);
        var buf = PooledByteBufAllocator.DEFAULT.buffer(compressed.length * 4);
        for (int i = 0; i < compressed.length; i++) {
            buf.writeInt(compressed[i]);
        }
        return buf;
    }

    @VisibleForTesting
    static int[] uncompressEntryOffsetsByZSTD(ByteBuf compressed, int uncompressedSize) {
        try {
            byte[] compressedBytes = new byte[compressed.readableBytes()];
            compressed.getBytes(compressed.readerIndex(), compressedBytes);
            byte[] uncompressed = Zstd.decompress(compressedBytes, uncompressedSize);
            ByteBuffer buffer = ByteBuffer.wrap(uncompressed);
            int[] arr = new int[uncompressedSize / 4];
            for (int i = 0; buffer.hasRemaining(); i++) {
                arr[i] = buffer.getInt();
            }
            return arr;
        } catch (Exception e) {
            throw new RuntimeException("failed to decode", e);
        }
    }

    @VisibleForTesting
    static ByteBuf compressEntryOffsetsByZSTD(int[] arr) {
        ByteBuffer raw = ByteBuffer.allocate(arr.length * Integer.BYTES);
        for (int j : arr) {
            raw.putInt(j);
        }
        return Unpooled.wrappedBuffer(Zstd.compress(raw.array()));
    }
}
