/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils.lock;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.lakestream.ursa.utils.lock.AsyncLock;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

public class FaultInjectedLockManagerTest {


    @Test
    public void testSpinUnlock() throws ExecutionException, InterruptedException, IOException {
        final Path tempDir = Files.createTempDirectory("lock-manager");
        var oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:main"), 1, true)
                .withFileSystemBind(
                        tempDir.toAbsolutePath().toString(),
                        "/oxia/data",
                        BindMode.READ_WRITE
                );
        oxiaContainer.start();

        var oxiaClient = OxiaClientBuilder.create(oxiaContainer.getServiceAddress())
                .connectionKeepAliveTime(Duration.ofSeconds(1))
                .connectionKeepAliveTimeout(Duration.ofSeconds(1))
                .requestTimeout(Duration.ofSeconds(5))
                .sessionTimeout(Duration.ofMinutes(5))
                .asyncClient()
                .get();

        final LockManagerImpl lm = (LockManagerImpl) LockManagers.createLockManager(oxiaClient);

        final AsyncLock lock = lm.getThreadSimpleLock("/streams/lock");
        lock.lock().get();

        assertNotNull(oxiaClient.get("/streams/lock").get());

        // lost connection
        oxiaContainer.stop();


        final CompletableFuture<Void> unlockFuture = CompletableFuture.runAsync(() -> {
            try {
                lock.unlock().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(10_000);

        // reconnected
        oxiaContainer.start();

        try {
            unlockFuture.get();
        } catch (Throwable ignore) {
            // we don't care the result
        }

        Awaitility.await()
                .ignoreExceptions()
                .untilAsserted(() -> assertNull(oxiaClient.get("/streams/lock").get()));
    }
}
