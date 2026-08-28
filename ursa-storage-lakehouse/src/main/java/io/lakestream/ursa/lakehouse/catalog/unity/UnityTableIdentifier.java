/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.catalog.unity;

import io.lakestream.ursa.lakehouse.utils.TopicName;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
@AllArgsConstructor
public class UnityTableIdentifier {

    private String schema;
    private String table;

    public static UnityTableIdentifier parse(String topic) {
        TopicName topicName = TopicName.getPartitionedTopicName(topic);
        String table = formatTableName(topicName.getLocalName());
        return new UnityTableIdentifier(formatSchemaName(topicName.getNamespace()), table);
    }

    private static String formatSchemaName(String namespace) {
        return namespace.replace("/", "_");
    }

    private static String formatTableName(String name) {
        if (StringUtils.isBlank(name)) {
            return name;
        }

        return name.replace("/", "___")
                .replace(".", "_")
                .replace("-", "__")
                .replace(":", "____");
    }

    public String getSchemaFullName(String catalog) {
        return catalog + "." + schema;
    }

    public String getTableFullName(String catalog) {
        return catalog + "." + schema + "." + table;
    }
}
