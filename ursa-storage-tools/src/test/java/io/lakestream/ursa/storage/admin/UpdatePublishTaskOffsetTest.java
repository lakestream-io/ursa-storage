/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class UpdatePublishTaskOffsetTest {

    @Test
    void rejectsOffsetsBelowNoPublishedTaskSentinel() {
        int exitCode = new CommandLine(new UpdatePublishTaskOffset()).execute(
                "--stream", "default/test-partition-0",
                "--stream-id", "1",
                "--offset", "-2",
                "--cumulative-size", "0");

        assertEquals(1, exitCode);
    }

    @Test
    void requiresCumulativeSizeRatherThanSilentlyResettingIt() {
        int exitCode = new CommandLine(new UpdatePublishTaskOffset()).execute(
                "--stream", "default/test-partition-0",
                "--stream-id", "1",
                "--offset", "10");

        assertEquals(CommandLine.ExitCode.USAGE, exitCode);
    }

    @Test
    void rejectsNegativeCumulativeSize() {
        int exitCode = new CommandLine(new UpdatePublishTaskOffset()).execute(
                "--stream", "default/test-partition-0",
                "--stream-id", "1",
                "--offset", "10",
                "--cumulative-size", "-1");

        assertEquals(1, exitCode);
    }

    @Test
    void rejectsNonZeroCumulativeSizeForEmptyCursor() {
        int exitCode = new CommandLine(new UpdatePublishTaskOffset()).execute(
                "--stream", "default/test-partition-0",
                "--stream-id", "1",
                "--offset", "-1",
                "--cumulative-size", "1");

        assertEquals(1, exitCode);
    }
}
