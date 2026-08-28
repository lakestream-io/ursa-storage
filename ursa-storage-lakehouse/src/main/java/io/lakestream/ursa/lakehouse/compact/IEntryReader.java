/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.exception.DataSourceException;
import io.lakestream.ursa.materialization.serde.GenericEntry;

public interface IEntryReader extends AutoCloseable {

    GenericEntry read() throws DataSourceException;

}
