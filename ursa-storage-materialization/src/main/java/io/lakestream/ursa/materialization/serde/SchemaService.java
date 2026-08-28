/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import java.util.Map;

public interface SchemaService<T> extends AutoCloseable {

    Map<Long, T> getSchemaWithVersions(String topic, long schemaVersion) throws Exception;

}
