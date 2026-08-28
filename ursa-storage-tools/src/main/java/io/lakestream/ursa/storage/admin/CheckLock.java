/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import io.oxia.client.api.OxiaClientBuilder;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to check and manage locks.
 *
 * Example:
 *    bin/ursa admin check-lock -o oxia-service:6648
 *    -ns broker  -l 7aa787ff-2294-4365-98d4-c461321bd7ee
 */
@Command(name = "check-lock", description = "Check and manage Oxia locks")
public class CheckLock implements Callable<Integer> {

    @Option(names = {"-o", "--oxia-server-addr"}, description = "Oxia server address", required = true)
    private String oxiaServerAddr;

    @Option(names = {"-ns", "--namespace"}, description = "Oxia namespace", defaultValue = "default")
    private String oxiaNamespace;

    @Option(names = {"-l", "--lock-name"}, description = "Lock name to check", required = true)
    private String lockName;

    @Override
    public Integer call() throws Exception {
        try (var oxiaClient = OxiaClientBuilder.create(oxiaServerAddr)
            .namespace(oxiaNamespace).asyncClient().get()) {

            var lockKeyName = "/task-locks/" + lockName;
            var gr = oxiaClient.get(lockKeyName).get(5, TimeUnit.SECONDS);
            if (gr != null && gr.key().equals(lockKeyName)) {
                System.out.println("Lock " + lockName + " is held, do you want to release it? (y/n)");
                if (System.in.read() == 'y') {
                    oxiaClient.delete(lockKeyName).get(5, TimeUnit.SECONDS);
                    System.out.println("Lock " + lockName + " released.");
                } else {
                    System.out.println("Lock " + lockName + " not released.");
                }
            } else {
                System.out.println("Lock " + lockName + " is not held by anyone.");
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error checking lock: " + e.getMessage());
            return 1;
        }
    }
}
