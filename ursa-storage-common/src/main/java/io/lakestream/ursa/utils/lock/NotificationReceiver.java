/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.lock;

import io.oxia.client.api.Notification;

public interface NotificationReceiver {

    void notifyStateChanged(Notification notification);
}
