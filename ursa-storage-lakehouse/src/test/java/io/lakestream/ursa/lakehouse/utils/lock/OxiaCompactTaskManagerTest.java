/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.OxiaCompactTaskManager;
import io.lakestream.ursa.utils.lock.AsyncLock;
import io.lakestream.ursa.utils.lock.LockManager;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

public class OxiaCompactTaskManagerTest {

    private OxiaContainer oxiaContainer;
    private AsyncOxiaClient asyncOxiaClient;
    private OxiaCompactTaskManager taskManager;


    @BeforeEach
    public void setup() throws Exception {
        // Setup and start the Oxia container
        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:latest"));
        oxiaContainer.setCommand("oxia standalone -s 32 --wal-sync-data=false");
        oxiaContainer.start();


        // Create an AsyncOxiaClient
        var oxiaClient = OxiaClientBuilder.create(oxiaContainer.getServiceAddress()).asyncClient().get();
        asyncOxiaClient = spy(oxiaClient);


        // Mock LockManager for locking tests
        LockManager lockManager = mock(LockManager.class);
        AsyncLock asyncLock = mock(AsyncLock.class);
        when(lockManager.getThreadSimpleLock(anyString())).thenReturn(asyncLock);
        when(asyncLock.tryLock()).thenReturn(CompletableFuture.completedFuture(null));
        when(asyncLock.unlock()).thenReturn(CompletableFuture.completedFuture(null));


        // Initialize the task manager
        taskManager = new OxiaCompactTaskManager(asyncOxiaClient, lockManager);
    }


    @AfterEach
    public void cleanup() throws Exception {
        if (asyncOxiaClient != null) {
            asyncOxiaClient.close();
        }
        if (oxiaContainer != null) {
            oxiaContainer.stop();
        }
    }

    @Test
    void testTryLockTask() throws Exception {
        LockManagerImpl lockManager1 = (LockManagerImpl) LockManagers.createLockManager(asyncOxiaClient);
        LockManagerImpl lockManager2 = (LockManagerImpl) LockManagers.createLockManager(asyncOxiaClient);

        // lockManager1 acquires the lock
        OxiaCompactTaskManager taskManager1 = new OxiaCompactTaskManager(asyncOxiaClient, lockManager1);
        OxiaCompactTaskManager taskManager2 = new OxiaCompactTaskManager(asyncOxiaClient, lockManager2);

        assertTrue(taskManager1.tryLockTask("lock1"));
        assertEquals(1, lockManager1.getLocks().size());

        assertFalse(taskManager1.tryLockTask("lock1"));
        assertEquals(1, lockManager1.getLocks().size());

        // lockManager2 tries to acquire the same lock, should fail
        assertFalse(taskManager2.tryLockTask("lock1"));
        assertEquals(0, lockManager2.getLocks().size());

        // lockManager2 tries to acquire the lock again, should still fail
        assertFalse(taskManager2.tryLockTask("lock1"));
        assertEquals(0, lockManager2.getLocks().size());

        // lockManager1 releases the lock
        taskManager1.unlockTaskAndRemoveLock("lock1");
        assertEquals(0, lockManager1.getLocks().size());
        // lockManager2 can now acquire the lock
        assertTrue(taskManager2.tryLockTask("lock1"));
        assertEquals(1, lockManager2.getLocks().size());

        // lockManager2 acquires the another lock
        assertTrue(taskManager2.tryLockTask("lock2"));
        assertEquals(2, lockManager2.getLocks().size());

        // lockManager1 tries to acquire the another lock, should fail
        assertFalse(taskManager1.tryLockTask("lock2"));
        assertEquals(0, lockManager1.getLocks().size());
    }
}
