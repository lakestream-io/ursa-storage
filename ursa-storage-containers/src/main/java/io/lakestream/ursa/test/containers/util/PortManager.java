/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test.containers.util;

import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

public class PortManager {

        private static final Set<Integer> PORTS = new HashSet<>();

        /**
         * Return a locked available port.
         *
         * @return locked available port.
         */
        public static synchronized int nextLockedFreePort() {
            int exceptionCount = 0;
            while (true) {
                try (ServerSocket ss = new ServerSocket(0)) {
                    int port = ss.getLocalPort();
                    if (!checkPortIfLocked(port)) {
                        PORTS.add(port);
                        return port;
                    }
                } catch (Exception e) {
                    exceptionCount++;
                    if (exceptionCount > 100) {
                        throw new RuntimeException("Unable to allocate socket port", e);
                    }
                }
            }
        }

        /**
         * Returns whether the port was released successfully.
         *
         * @return whether the release is successful.
         */
        public static synchronized boolean releaseLockedPort(int lockedPort) {
            return PORTS.remove(lockedPort);
        }

        /**
         * Check port if locked.
         *
         * @return whether the port is locked.
         */
        public static synchronized boolean checkPortIfLocked(int lockedPort) {
            return PORTS.contains(lockedPort);
        }
}
