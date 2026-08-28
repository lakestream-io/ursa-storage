/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.FileStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@Slf4j
public class ByteBufAsyncResponseTransformer implements
    AsyncResponseTransformer<GetObjectResponse, Pair<ByteBuf, Map<String, String>>> {

    private volatile CompletableFuture<Pair<ByteBuf, Map<String, String>>> cf;
    ByteBuf byteBuf;
    private Map<String, String> metadata;

    // this is used for the testing purpose to see the release operation is called multiple times
    boolean hasReleaseBufferException = false;

    @Override
    public CompletableFuture<Pair<ByteBuf, Map<String, String>>> prepare() {
        this.cf = new CompletableFuture<>();
        this.metadata = new HashMap<>();
        return cf;
    }

    @Override
    public void onResponse(GetObjectResponse response) {
        // Handle the response if needed
        metadata.clear();
        metadata.putAll(response.metadata());
        // The object creation date or the last modified date, whichever is the latest. For multipart uploads,
        // the object creation date is the date of initiation of the multipart upload.
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
        metadata.put(FileStorage.METADATA_CREATION_TIME, response.lastModified().toString());
        byteBuf = PooledByteBufAllocator.DEFAULT.buffer(Math.toIntExact(response.contentLength()));
    }


    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        publisher.subscribe(new Subscriber<ByteBuffer>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer byteBuffer) {
                byteBuf.writeBytes(byteBuffer);
            }

            @Override
            public void onError(Throwable t) {
                completeExceptionally(t);
            }

            @Override
            public void onComplete() {
                cf.complete(Pair.of(byteBuf, metadata));
            }
        });
    }

    @Override
    public void exceptionOccurred(Throwable error) {
        completeExceptionally(error);
    }

    private void completeExceptionally(Throwable error) {
        if (cf.isDone()) {
            return;
        }
        if (byteBuf != null) {
            try {
                byteBuf.release();
            } catch (Exception e) {
                hasReleaseBufferException = true;
                log.error("Failed to release ByteBuf", e);
            }
        }
        cf.completeExceptionally(error);
    }
}
