/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapValueImpl implements MapValue {

    private final ColumnVector keys;
    private final ColumnVector values;

    public MapValueImpl(Map<Object, Object> map, DataType keyType, DataType valueType) {
        List<Object> keyList = new ArrayList<>(map.keySet());
        List<Object> valueList = new ArrayList<>(map.values());

        this.keys = new SimpleColumnVector(keyList, keyType);
        this.values = new SimpleColumnVector(valueList, valueType);
    }

    @Override
    public int getSize() {
        return keys.getSize();
    }

    @Override
    public ColumnVector getKeys() {
        return keys;
    }

    @Override
    public ColumnVector getValues() {
        return values;
    }
}