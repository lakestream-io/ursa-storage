/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

public class StringObjectMapWritable implements Writable {

    private final ObjectMapper objectMapper;

    @Setter
    @Getter
    private Map<String, Object> map;

    public StringObjectMapWritable(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        var bytes = objectMapper.writeValueAsBytes(this.map);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        var size = in.readInt();
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        this.map = objectMapper.readValue(bytes, new TypeReference<Map<String, Object>>() {});
    }
}
