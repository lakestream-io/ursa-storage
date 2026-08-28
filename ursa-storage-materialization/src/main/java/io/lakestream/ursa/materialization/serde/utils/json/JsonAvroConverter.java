/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class JsonAvroConverter {

    public static final JsonAvroConverter INSTANCE = new JsonAvroConverter();

    private JsonGenericRecordReader recordReader;
    private ObjectMapper objectMapper = new ObjectMapper();

    public JsonAvroConverter() {
        this.recordReader = new JsonGenericRecordReader();
    }

    public GenericData.Record convertToGenericDataRecord(ObjectNode objectNode, Schema schema) throws IOException {
        var bytes = objectMapper.writeValueAsBytes(objectNode);
        return convertToGenericDataRecord(bytes, schema);
    }

    public GenericData.Record convertToGenericDataRecord(byte[] data, Schema schema) {
        return recordReader.read(data, schema);
    }
}
