/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class JsonUtilsTest {

    @Test
    public void testSerializeNaN() {
        Map<String, Object> data = new HashMap<>();
        data.put("nanValue", Double.NaN);
        data.put("infinityValue", Double.POSITIVE_INFINITY);
        data.put("negInfinityValue", Double.NEGATIVE_INFINITY);
        data.put("normalValue", 42.0);

        String json = assertDoesNotThrow(() -> JsonUtils.toJson(data));
        assertTrue(json.contains("NaN"));
        assertTrue(json.contains("Infinity"));
        assertTrue(json.contains("-Infinity"));
        assertTrue(json.contains("42.0"));
    }

    @Test
    public void testSerializeFloatSpecialValues() {
        Map<String, Object> data = new HashMap<>();
        data.put("floatNaN", Float.NaN);
        data.put("floatInfinity", Float.POSITIVE_INFINITY);
        data.put("floatNegInfinity", Float.NEGATIVE_INFINITY);

        String json = assertDoesNotThrow(() -> JsonUtils.toJson(data));
        assertTrue(json.contains("NaN"));
        assertTrue(json.contains("Infinity"));
        assertTrue(json.contains("-Infinity"));
    }
}