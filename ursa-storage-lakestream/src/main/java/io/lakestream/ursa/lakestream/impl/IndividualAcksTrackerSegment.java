/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import com.github.luben.zstd.Zstd;
import io.lakestream.ursa.lakestream.proto.CompressionType;
import io.lakestream.ursa.lakestream.proto.IndividualAcksSegment;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.AsyncOxiaClient;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jpountz.lz4.LZ4Factory;
import org.roaringbitmap.RoaringBitmap;
import org.xerial.snappy.Snappy;

@Getter
@Slf4j
public class IndividualAcksTrackerSegment {

    private final IndividualAcksTracker tracker;
    private final long baseOffset;

    private final IndividualAcksSegment segment;
    private final RoaringBitmap bitmap;

    private final String path;
    @Nullable private final AsyncOxiaClient oxia;
    private boolean dirty;

    public static IndividualAcksTrackerSegment parseFromValue(IndividualAcksTracker tracker, byte[] serializedSegment) {
        IndividualAcksSegment segment = new IndividualAcksSegment();
        segment.parseFrom(serializedSegment);
        RoaringBitmap bitmap = recoverBitmap(segment);
        return new IndividualAcksTrackerSegment(tracker, segment, bitmap);
    }

    IndividualAcksTrackerSegment(IndividualAcksTracker tracker, long baseOffset) {
        this(tracker, new IndividualAcksSegment().setBaseOffset(baseOffset), new RoaringBitmap());
    }

    private IndividualAcksTrackerSegment(IndividualAcksTracker tracker, IndividualAcksSegment segment,
                                         RoaringBitmap bitmap) {
        this.tracker = tracker;
        this.segment = segment;
        this.bitmap = bitmap;
        this.baseOffset = segment.getBaseOffset();
        this.oxia = tracker.getOxia();
        this.path = String.format("individual-acks-%020d-%020d", tracker.getCursorId(), baseOffset);
    }

    public CompletableFuture<Void> remove() {
        if (oxia == null) {
            return CompletableFuture.completedFuture(null);
        }
        return oxia.delete(path, tracker.deleteOptions).thenApply(x -> null);
    }

    public void addOffset(long offset) {
        bitmap.add(toBitmapOffset(offset));
        dirty = true;
    }

    public void addFromAckSet(long ackSetBaseOffset, long[] ackSet) {
        if (ackSet == null) {
            return;
        }

        BitSet bitSet = BitSet.valueOf(ackSet);
        for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1)) {
            bitmap.add(toBitmapOffset(ackSetBaseOffset + i));
        }
        dirty = true;
    }

    public long firstNonDeletedOffset(long markDeleteOffset) {
        if (baseOffset > markDeleteOffset) {
            return markDeleteOffset;
        }

        int fromOffset = toBitmapOffset(markDeleteOffset);
        return baseOffset + bitmap.nextAbsentValue(fromOffset + 1);
    }

    public long count() {
        return bitmap.getCardinality();
    }

    public long countFromRange(long from, long to) {
        from = Math.max(from, baseOffset);
        to = Math.min(to, baseOffset + bitmap.stream().max().orElse(0));
        int fromOffset = toBitmapOffset(from);
        int toOffset = toBitmapOffset(to);
        return bitmap.rangeCardinality(fromOffset, toOffset + 1);
    }

    public long lastOffset() {
        if (bitmap.isEmpty()) {
            return baseOffset - 1;
        }
        return baseOffset + bitmap.last();
    }

    public boolean isEmpty() {
        return bitmap.isEmpty();
    }

    public void trimToOffset(long offset) {
        if (bitmap.isEmpty()) {
            return;
        }
        int bmOffset = toBitmapOffset(offset);
        if (bitmap.first() <= bmOffset) {
            bitmap.remove(0, bmOffset + 1);
            dirty = true;
        }
    }

    public CompletableFuture<Void> flush() {
        if (!dirty || oxia == null) {
            return CompletableFuture.completedFuture(null);
        }

        try {
            return oxia.put(path, serializeRecord())
                    .thenApply(x -> {
                        dirty = false;
                        return null;
                    });
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public int getSegmentSize() {
        return bitmap.getSizeInBytes();
    }

    public int span() {
        return bitmap.isEmpty() ? 0 : bitmap.last();
    }

    public byte[] serializeRecord() throws IOException {
        bitmap.runOptimize();
        int bufferSize = bitmap.serializedSizeInBytes();
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        bitmap.serialize(buffer);
        byte[] compressed = Zstd.compress(buffer.array());

        segment.clear();
        segment.setBaseOffset(baseOffset);
        segment.setCompression(CompressionType.ZSTD);
        segment.setBitmap(compressed);
        segment.setOriginalSize(bufferSize);

        int serializedSize = segment.getSerializedSize();
        byte[] serialized = new byte[serializedSize];
        segment.writeTo(Unpooled.wrappedBuffer(serialized).writerIndex(0));
        return serialized;
    }

    public static RoaringBitmap recoverBitmap(IndividualAcksSegment segment) {
        CompressionType compressionType = CompressionType.NONE;
        if (segment.hasCompression()) {
            compressionType = segment.getCompression();
        }

        ByteBuf compressedBuffer = segment.getBitmapSlice();
        RoaringBitmap bitmap = new RoaringBitmap();

        try {
            byte[] compressed = new byte[compressedBuffer.readableBytes()];
            compressedBuffer.getBytes(compressedBuffer.readerIndex(), compressed);
            byte[] uncompressed = decompress(compressionType, compressed, segment.getOriginalSize());
            bitmap.deserialize(ByteBuffer.wrap(uncompressed));
        } catch (IOException e) {
            log.warn("Failed to de-serialize individual acks", e);
        }

        return bitmap;
    }

    private static byte[] decompress(CompressionType compressionType, byte[] compressed, int originalSize)
            throws IOException {
        return switch (compressionType) {
            case NONE -> compressed;
            case SNAPPY -> Snappy.uncompress(compressed);
            case LZ4 -> LZ4Factory.fastestInstance().safeDecompressor().decompress(compressed, originalSize);
            case ZSTD -> Zstd.decompress(compressed, originalSize);
            case ZLIB -> inflate(compressed, originalSize);
        };
    }

    private static byte[] inflate(byte[] compressed, int originalSize) throws IOException {
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(compressed);
            byte[] result = new byte[originalSize];
            int size = inflater.inflate(result);
            if (size != originalSize || !inflater.finished()) {
                throw new IOException("Unexpected decompressed bitmap size: " + size + ", expected " + originalSize);
            }
            return result;
        } catch (DataFormatException e) {
            throw new IOException("Invalid compressed individual-acks bitmap", e);
        } finally {
            inflater.end();
        }
    }

    private int toBitmapOffset(long offset) {
        return (int) (offset - baseOffset);
    }

    public boolean contains(long offset) {
        return bitmap.contains(toBitmapOffset(offset));
    }

    public void clearAfterOffset(long offset) {
        if (baseOffset > offset) {
            bitmap.clear();
            return;
        }
        RoaringBitmap other = new RoaringBitmap();
        other.add(0, offset);
        this.bitmap.and(other);
    }
}
