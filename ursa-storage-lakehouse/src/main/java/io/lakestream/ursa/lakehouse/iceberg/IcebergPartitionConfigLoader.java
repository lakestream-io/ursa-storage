/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;

public class IcebergPartitionConfigLoader {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static List<IcebergPartitionConfig> loadFromJson(String jsonString) throws IOException {
        // Parse JSON array into a list of PartitionConfig objects
        return mapper.readValue(
            jsonString,
            mapper.getTypeFactory().constructCollectionType(List.class, IcebergPartitionConfig.class)
        );
    }
}
