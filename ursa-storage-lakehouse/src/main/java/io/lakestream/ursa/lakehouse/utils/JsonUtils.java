/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonUtils {

    private static final Gson GSON = new GsonBuilder()
            .serializeSpecialFloatingPointValues()
            .create();

    public static String toJson(Object o) {
        return GSON.toJson(o);
    }
}
