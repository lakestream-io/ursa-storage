/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Strategy for how a stream is divided into logs.
 */
public enum PartitioningStrategy {

    /** Fixed number of logs accessed by integer index. */
    INDEXED,

    /** Key-range segments with split/merge (future). */
    RANGE
}
