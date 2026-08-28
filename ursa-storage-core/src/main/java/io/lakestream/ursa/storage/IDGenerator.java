/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.ursa.storage.impl.MemIDGenerator;
import io.lakestream.ursa.storage.impl.OxiaIDGenerator;
import io.lakestream.ursa.storage.impl.RandomIDGenerator;
import io.lakestream.ursa.storage.impl.UUIDIDGenerator;
import io.lakestream.ursa.storage.impl.UUIDWithDateIDGenerator;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Locale;

/**
 * IDGenerator interface for generating unique identifiers.
 * This interface provides a common abstraction for different ID generation strategies.
 *
 * Note: When using this interface, be aware of the following:
 * 1. The choice of ID generator type can impact performance and uniqueness guarantees.
 * 2. Some generators (e.g., OXIA) may require external dependencies or configuration.
 * 3. Ensure that the chosen generator meets the uniqueness requirements of your use case.
 *
 * ID Generation Strategies:
 * - MEMORY: Uses an in-memory counter. Fast but not suitable for distributed systems.
 *   Recommended for single-node applications or testing environments.
 *
 * - RANDOM: Generates random IDs. Good performance but has a small chance of collisions.
 *   Suitable for scenarios where occasional duplicates are acceptable.
 *
 * - OXIA: Uses an external OXIA service for ID generation. Provides strong uniqueness
 *   guarantees and works well in distributed systems. Recommended for production
 *   environments with high scalability requirements. Requires additional setup.
 *
 * - UUID: Generates universally unique identifiers. Good uniqueness guarantees and
 *   doesn't require external services. Suitable for most production use cases,
 *   but IDs are longer and may impact storage efficiency.
 *
 * Recommendations:
 * - For simple, single-node applications: Use MEMORY or RANDOM.
 * - For distributed systems with high scalability needs: Use OXIA.
 * - For a balance between uniqueness and simplicity: Use UUID.
 * - Always consider your specific requirements for uniqueness, performance,
 *   and scalability when choosing an ID generation strategy.
 */
public interface IDGenerator {

    /**
     * Enum representing the available ID generator types.
     */
    enum Type {
        MEMORY,
        RANDOM,
        OXIA,
        UUID,
        DATEUUID
    }

    /**
     * Creates an IDGenerator instance based on the specified type.
     *
     * @param type The type of ID generator to create.
     * @param keyForOxia The key to use for OXIA-based generation (only used if type is OXIA).
     * @param oxiaClient The AsyncOxiaClient instance (only used if type is OXIA).
     * @return An IDGenerator instance of the specified type.
     * @throws IDGeneratorException.NotSupportException if the specified type is not supported.
     *
     * Note: When using this method, ensure that:
     * 1. The correct type is specified based on your application's requirements.
     * 2. For OXIA type, both keyForOxia and oxiaClient are properly provided.
     * 3. The chosen type is compatible with your application's architecture and scalability needs.
     */
    static IDGenerator create(String type, String keyForOxia, AsyncOxiaClient oxiaClient)
        throws IDGeneratorException.NotSupportException {

        switch (Type.valueOf(type.toUpperCase(Locale.ROOT))) {
            case MEMORY -> {
                return new MemIDGenerator();
            }
            case RANDOM -> {
                return new RandomIDGenerator();
            }
            case OXIA -> {
                return new OxiaIDGenerator(keyForOxia, oxiaClient);
            }
            case UUID -> {
                return new UUIDIDGenerator();
            }
            case DATEUUID -> {
                return new UUIDWithDateIDGenerator();
            }
            default -> throw new IDGeneratorException.NotSupportException();
        }
    }

    /**
     * Generates a unique identifier.
     *
     * @return A String representing the generated unique identifier.
     * @throws IDGeneratorException if an error occurs during ID generation.
     *
     * Note: The format and properties of the generated ID may vary depending on the
     * specific implementation. Ensure that the chosen generator meets your uniqueness
     * and format requirements.
     */
    String generate() throws IDGeneratorException;
}
