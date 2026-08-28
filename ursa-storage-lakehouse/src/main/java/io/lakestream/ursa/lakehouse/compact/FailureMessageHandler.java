/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import java.util.concurrent.CompletableFuture;

public interface FailureMessageHandler {

    CompletableFuture<Void> sendFailureMessage(FailureMessage failureMessage);

    void close();
}
