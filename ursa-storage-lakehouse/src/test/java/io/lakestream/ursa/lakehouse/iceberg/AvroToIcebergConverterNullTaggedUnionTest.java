/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class AvroToIcebergConverterNullTaggedUnionTest {

    @Test
    void testMapWithNullTaggedUnionValueConvertsSuccessfully() {
        Schema unionSchema = Schema.createUnion(List.of(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.STRING),
            Schema.create(Schema.Type.LONG),
            Schema.create(Schema.Type.DOUBLE)
        ));

        Schema avroSchema = Schema.createRecord("Event", "", "", false, List.of(
            new Schema.Field("event_data", Schema.createMap(unionSchema), "", null)
        ));

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(avroSchema);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        Map<String, Object> eventData = new LinkedHashMap<>();
        eventData.put("paid_action", null);
        eventData.put("campaign_id", 27510L);
        avroRecord.put("event_data", eventData);

        Record icebergRecord = AvroToIcebergConverter.convert(avroRecord, icebergSchema);

        @SuppressWarnings("unchecked")
        Map<String, Object> convertedEventData = (Map<String, Object>) icebergRecord.getField("event_data");
        assertNotNull(convertedEventData);
        assertNull(convertedEventData.get("paid_action"));

        Record campaignId = (Record) convertedEventData.get("campaign_id");
        assertNotNull(campaignId);
        assertEquals(1, campaignId.getField("tag"));
        assertEquals(27510L, campaignId.getField("field1"));
    }
}
