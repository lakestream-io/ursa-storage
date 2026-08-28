/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/** Provides the shared JSON mapper used by Ursa metadata and task serializers. */
public final class UrsaObjectMapperFactory {

    private static final ObjectMapper MAPPER = createObjectMapper();

    private UrsaObjectMapperFactory() {
    }

    /**
     * Returns the shared, fully configured mapper.
     *
     * <p>The mapper is safe to share once configured. Callers should create immutable readers and writers rather than
     * changing its configuration.
     */
    public static ObjectMapper getMapper() {
        return MAPPER;
    }

    private static ObjectMapper createObjectMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
                .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule());
    }
}
