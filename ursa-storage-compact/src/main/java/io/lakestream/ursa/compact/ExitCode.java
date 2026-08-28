/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

/**
 * Exit code used to exit compact server.
 */
public class ExitCode {
    // normal quit
    public static final int OK                  = 0;
    // invalid configuration
    public static final int INVALID_CONF        = 1;
    // exception running compact server
    public static final int SERVER_EXCEPTION    = 2;
}
