/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

import com.alibaba.com.caucho.hessian.io.Hessian2Input;
import com.alibaba.com.caucho.hessian.io.Hessian2Output;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.lakestream.ursa.json.UrsaObjectMapperFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactStreamTaskSerde {

    public static final CompactStreamTaskSerde INSTANCE = new CompactStreamTaskSerde();


    private final ObjectReader objectReader;

    public CompactStreamTaskSerde() {
        JavaType typeRef = TypeFactory.defaultInstance().constructType(CompactStreamTask.class);
        this.objectReader = UrsaObjectMapperFactory.getMapper().reader().forType(typeRef);
    }

    public byte[] serialize(CompactStreamTask value) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Hessian2Output out = new Hessian2Output(os);
        try {
            out.writeObject(value);
            out.flush();
            return os.toByteArray();
        } finally {
            out.close();
            os.close();
        }
    }

    public CompactStreamTask deserialize(byte[] content) throws IOException, ClassNotFoundException {
        if (content.length == 0) {
            throw new IOException("The content is empty");
        }
        try {
            ByteArrayInputStream is = new ByteArrayInputStream(content);
            Hessian2Input in = new Hessian2Input(is);
            try {
                return (CompactStreamTask) in.readObject();
            } finally {
                in.close();
                is.close();
            }
        } catch (Exception e) {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(content);
                 ObjectInputStream ois = new ObjectInputStream(bis)) {
                Object task = ois.readObject();
                if (task instanceof CompactStreamTask) {
                    return (CompactStreamTask) task;
                } else {
                    return ((io.lakestream.ursa.storage.impl.compaction.task.CompactStreamTask) task)
                            .toCompactStreamTask();
                }
            } catch (Exception e1) {
                log.error("Failed to deserialize task using java ObjectInputStream, fail back to json");
                return objectReader.readValue(content);
            }
        }
    }

}
