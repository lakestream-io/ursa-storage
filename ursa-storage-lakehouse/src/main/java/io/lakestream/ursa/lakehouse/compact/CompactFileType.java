/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

public enum CompactFileType {
    LOCAL,
    S3,
    GCS,
    AZUREBLOB,
    AZUREDFS,
    AZURELOCAL

}
