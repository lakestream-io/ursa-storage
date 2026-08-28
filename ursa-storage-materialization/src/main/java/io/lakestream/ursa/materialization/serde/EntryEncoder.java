/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.ursa.materialization.serde.exception.FatalException;
import io.lakestream.ursa.materialization.serde.exception.SerializationException;
import java.util.function.Consumer;


/**
 * Converts one storage entry into one or more materialization records.
 *
 * <p>Ownership of {@code entry} transfers to the encoder when an {@code encode} method is
 * invoked. Implementations must release that owned reference exactly once on every success and
 * failure path. Error callbacks may receive a separately retained reference, but the encoder
 * remains responsible for its original reference.
 */
public interface EntryEncoder<T> {

    ObjectMapper MAPPER = new ObjectMapper();

    default void encode(String topic, GenericEntry entry, ResultConsumer<MaterializationRecord<T>> consumer) {
        encode(topic, entry, consumer, null,
                EntryEncoderContext.builder()
                        .isPersistExtraMetadata(false)
                        .build());
    }

    default void encode(String topic, GenericEntry entry, ResultConsumer<MaterializationRecord<T>> consumer,
                        TableSchemaService schemaService, EntryEncoderContext context) {
        this.encode(topic, entry, consumer);
    }


    // this method only used to keep the current testing running without change to the new way,
    // please do not use it out of the test scope
    default void encode(String topic, GenericEntry entry, Consumer<MaterializationRecord<T>> consumer)
        throws SerializationException {

        this.encode(topic, entry, new ResultConsumer<MaterializationRecord<T>>() {
            @Override
            public void onResult(MaterializationRecord<T> lakehouseEntry) {
                consumer.accept(lakehouseEntry);
            }

            @Override
            public void onErrorWithCtx(Object ctx, Throwable throwable) {
                try {
                    throw new FatalException(throwable);
                } finally {
                    if (ctx instanceof GenericEntry genericEntry) {
                        genericEntry.entry().payload().release();
                    }
                }
            }
        });
    }

}
