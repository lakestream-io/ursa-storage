/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Registry of source/sink {@link EntryEncoder} and {@link EntryDecoder} instances keyed by
 * {@link SerdeType}. Sink-specific encoder registrations (Delta, Iceberg, Parquet, ...) are
 * contributed by integration modules through {@link #registerEncoderProvider} /
 * {@link #registerDecoderProvider}; this keeps the generic serde framework free of dependencies
 * on individual table-format modules.
 *
 * <p>Known integration-side registries are eagerly loaded the first time this class is
 * referenced (see the static initializer below), so callers never need to remember to invoke
 * a separate bootstrap method before constructing the factory. Failure to load an individual
 * registry class is silently ignored — the hosting deployment may simply not bundle that
 * integration.
 */
@Slf4j
public class EntrySerdeFactory {

    public enum SerdeType {
        KAFKA_ICEBERG,
        KAFKA_DELTA,
        KAFKA_PARQUET,
        KAFKA_BATCHED_RAW_PARQUET,
        KAFKA_CLICKHOUSE,
    }

    private static final ConcurrentMap<SerdeType, Function<SchemaService, EntryEncoder<?>>>
            ENCODER_PROVIDERS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<SerdeType, Function<SchemaService, EntryDecoder<?>>>
            DECODER_PROVIDERS = new ConcurrentHashMap<>();

    static {
        // Eagerly load known integration-side registries so callers never need to remember to
        // call a bootstrap before getEncoder/getDecoder. Each registry's static initializer
        // calls registerEncoderProvider / registerDecoderProvider. Failure to load a registry
        // is logged at debug only — the hosting deployment may not bundle that integration.
        bootstrapKnownRegistries();
    }

    private static void bootstrapKnownRegistries() {
        String[] candidates = {
            "io.lakestream.ursa.lakehouse.v2.serde.LakehouseSerdeRegistry",
            "io.lakestream.ursa.clickhouse.serde.ClickHouseSerdeRegistry"
            // Other integrations add their registry FQCN here.
        };
        for (String fqcn : candidates) {
            try {
                Class.forName(fqcn);
            } catch (ClassNotFoundException ignored) {
                // The hosting deployment doesn't bundle this registry — that's fine.
                log.debug("Serde registry {} not present on the classpath; skipping", fqcn);
            }
        }
    }

    private final Map<SerdeType, EntryEncoder> encoders = new HashMap<>();
    private final Map<SerdeType, EntryDecoder> decoders = new HashMap<>();

    public EntrySerdeFactory(SchemaService schemaService) {
        for (var entry : ENCODER_PROVIDERS.entrySet()) {
            EntryEncoder<?> encoder = entry.getValue().apply(schemaService);
            if (encoder != null) {
                encoders.put(entry.getKey(), encoder);
            }
        }
        for (var entry : DECODER_PROVIDERS.entrySet()) {
            EntryDecoder<?> decoder = entry.getValue().apply(schemaService);
            if (decoder != null) {
                decoders.put(entry.getKey(), decoder);
            }
        }

        log.info("Initialized EntrySerdeFactory with encoders {}, decoders {}", encoders, decoders);
    }

    /**
     * Register a factory that creates an encoder for the given {@link SerdeType}. The factory
     * may return {@code null} when the supplied {@link SchemaService} is not compatible with
     * the encoder, in which case no encoder is registered for that type.
     */
    public static void registerEncoderProvider(SerdeType serdeType,
                                               Function<SchemaService, EntryEncoder<?>> provider) {
        ENCODER_PROVIDERS.put(serdeType, provider);
    }

    /**
     * Register a factory that creates a decoder for the given {@link SerdeType}. The factory
     * may return {@code null} when the supplied {@link SchemaService} is not compatible with
     * the decoder, in which case no decoder is registered for that type.
     */
    public static void registerDecoderProvider(SerdeType serdeType,
                                               Function<SchemaService, EntryDecoder<?>> provider) {
        DECODER_PROVIDERS.put(serdeType, provider);
    }

    public <T> EntryEncoder<T> getEncoder(SerdeType serdeType) {
        var encoder = encoders.get(serdeType);
        if (encoder == null) {
            throw new IllegalArgumentException("Unknown serde type: " + serdeType);
        }
        return encoder;
    }

    public <T> EntryDecoder<T> getDecoder(SerdeType serdeType) {
        var decoder = decoders.get(serdeType);
        if (decoder == null) {
            throw new IllegalArgumentException("Unknown serde type: " + serdeType);
        }
        return decoder;
    }

    public void close() {
        encoders.clear();
        decoders.clear();
    }
}
