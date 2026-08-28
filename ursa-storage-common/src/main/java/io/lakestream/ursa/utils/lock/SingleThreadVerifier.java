/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.lock;

public class SingleThreadVerifier {

    private static final long INVALID_THREAD_ID = -1L;
    private final boolean enabled = "true".equals(System.getProperty("ursa.test.enabled"));
    private long threadId = INVALID_THREAD_ID;
    private String threadName = "";

    public void run(String methodName) {
        if (!enabled) {
            return;
        }
        final var currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            if (threadId == INVALID_THREAD_ID) {
                threadId = currentThreadId;
                threadName = Thread.currentThread().getName();
            } else if (threadId != currentThreadId) {
                throw new RuntimeException(methodName + " is called in another thread: " + currentThreadId + "("
                        + Thread.currentThread().getName() + "), before: " + threadName);
            }
        }
    }
}
