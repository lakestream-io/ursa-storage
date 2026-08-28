/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact.elect;

public class CompactLeader {

    private String hostname;

    public CompactLeader() {
    }

    public CompactLeader(String hostname) {
        this.hostname = hostname;
    }

    public String getHostname() {
        return hostname;
    }
}
