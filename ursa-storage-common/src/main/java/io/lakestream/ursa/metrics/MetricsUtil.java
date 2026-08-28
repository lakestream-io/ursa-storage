/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.ArrayList;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class MetricsUtil {

    // By default, advice to use namespace level aggregation only
    private static final List<AttributeKey<String>> DEFAULT_AGGREGATION_LABELS = List.of(
            AttributeKey.stringKey("ursa.stream.namespace")
    );
    private static final String PARTITION_SUFFIX = "-partition-";

    static List<AttributeKey<?>> getDefaultAggregationLabels(Attributes attrs) {
        List<AttributeKey<?>> res = new ArrayList<>();
        res.addAll(DEFAULT_AGGREGATION_LABELS);
        res.addAll(attrs.asMap().keySet());
        return res;
    }

    static Attributes getStreamAttributes(String streamName, Attributes baseAttributes) {
        StreamComponents stream = StreamComponents.parse(streamName);

        AttributesBuilder ab = baseAttributes.toBuilder();
        if (stream.partitionIndex() >= 0) {
            ab.put("ursa.stream.partition", stream.partitionIndex());
        }
        ab.put("ursa.stream.name", stream.baseStreamName());
        if (!stream.namespace().isEmpty()) {
            ab.put("ursa.stream.namespace", stream.namespace());
        }
        return ab.build();
    }

    private record StreamComponents(String namespace, String baseStreamName, int partitionIndex) {

        private static StreamComponents parse(String streamName) {
            if (streamName == null || streamName.isBlank()) {
                throw new IllegalArgumentException("Stream name must not be blank");
            }

            int separator = streamName.lastIndexOf('/');
            String namespace = separator < 0 ? "" : streamName.substring(0, separator);
            String localName = separator < 0 ? streamName : streamName.substring(separator + 1);
            if (localName.isBlank()) {
                throw new IllegalArgumentException("Stream name must include a local name");
            }
            int partitionIndex = getPartitionIndex(localName);
            String baseLocalName = partitionIndex < 0
                    ? localName
                    : localName.substring(0, localName.lastIndexOf(PARTITION_SUFFIX));
            String baseStreamName = namespace.isEmpty() ? baseLocalName : namespace + "/" + baseLocalName;
            return new StreamComponents(namespace, baseStreamName, partitionIndex);
        }

        private static int getPartitionIndex(String localName) {
            int suffixIndex = localName.lastIndexOf(PARTITION_SUFFIX);
            if (suffixIndex < 0) {
                return -1;
            }

            String indexText = localName.substring(suffixIndex + PARTITION_SUFFIX.length());
            try {
                int index = Integer.parseInt(indexText);
                return index >= 0 && indexText.length() == Integer.toString(index).length() ? index : -1;
            } catch (NumberFormatException ignored) {
                return -1;
            }
        }
    }
}
