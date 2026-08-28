/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.exception;

import lombok.Getter;

@Getter
public enum ExceptionCode {
    OK(0),
    UNKNOWN(1),
    INTERNAL_ERROR(2),

    // Source read exceptions
    SOURCE_CLIENT_ERROR(101),
    SOURCE_READ_ERROR(102),
    NO_SUCH_STREAM(103),
    NO_MORE_RECORDS(104),
    SOURCE_THROTTLED(105),
    NO_SUCH_LOG(106),
    NO_SUCH_ENTRIES(107),
    NO_SUCH_OFFSET(108),

    // Message related exceptions
    MESSAGE_SERIALIZE_TO_SOURCE_ERROR(201),
    MESSAGE_DESERIALIZE_FROM_SOURCE_ERROR(202),
    MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR(203),
    MESSAGE_DESERIALIZE_FROM_LAKEHOUSE_ERROR(204),
    MESSAGE_PARSE_FAILED(205),
    MESSAGE_SCHEMA_INCOMPATIBLE(206),
    MESSAGE_BAD_SCHEMA(207),
    MESSAGE_NULL_VALUE(208),

    // Lakehouse related exceptions
    LAKEHOUSE_CREATE_TABLE_ERROR(301),
    LAKEHOUSE_CREATE_TABLE_WRITER_ERROR(302),
    LAKEHOUSE_WRITE_ERROR(303),
    LAKEHOUSE_READ_ERROR(304),
    LAKEHOUSE_COMMIT_ERROR(305),
    LAKEHOUSE_CHECK_COMMITTED_ERROR(306),
    LAKEHOUSE_TABLE_CORRUPTED_ERROR(307),

    // Compaction Service related exceptions
    COMPACTION_PUBLISH_TASK_ERROR(401),
    COMPACTION_UPDATE_TASK_ERROR(402),
    COMPACTION_UPDATE_INDEX_ERROR(403),
    COMPACTION_NO_WRITE_RESULT(404);

    private final int code;

    ExceptionCode(int code) {
        this.code = code;
    }

}
