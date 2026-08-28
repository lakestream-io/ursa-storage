/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;
import java.util.List;

public class ArrayValueImpl implements ArrayValue {

    private final int size;
    private final ColumnVector elementVector;

    public ArrayValueImpl(List<Object> arrayList, DataType elementType) {
        this.size = arrayList.size();
        this.elementVector = createColumnVector(arrayList, elementType);
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public ColumnVector getElements() {
        return elementVector;
    }

    private ColumnVector createColumnVector(List<Object> arrayList, DataType elementType) {
        return new SimpleColumnVector(arrayList, elementType);
    }
}
