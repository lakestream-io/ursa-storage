/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import io.lakestream.ursa.compaction.OxiaCompactTaskManager;
import io.lakestream.ursa.storage.UrsaStorage;
import io.oxia.client.api.OxiaClientBuilder;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

@CommandLine.Command(name = "update-publish-task-offset", description = "Update publish task offset")
public class UpdatePublishTaskOffset implements Callable<Integer> {
    @CommandLine.ParentCommand
    private Admin parent;

    @CommandLine.Option(names = {"-o", "--oxia-server-addr"}, description = "Oxia server address")
    private String oxiaServerAddr;

    @CommandLine.Option(names = {"-ns", "--namespace"}, description = "Oxia namespace")
    private String oxiaNamespace;

    @CommandLine.Option(names = {"-s", "--stream"}, description = "Stream name", required = true)
    private String stream;

    @CommandLine.Option(names = {"--stream-id"}, description = "Stream ID", required = true)
    private long streamId;

    @CommandLine.Option(
            names = {"--offset"},
            description = "Last included stream offset; use -1 when no task has been published",
            required = true)
    private long offset;

    @CommandLine.Option(
            names = {"--cumulative-size"},
            description = "Cumulative bytes through --offset; use 0 with --offset=-1",
            required = true)
    private long cumulativeSize;

    @Override
    public Integer call() throws Exception {
        if (offset < -1) {
            System.err.println("Published offset must be -1 or a non-negative last-included offset");
            return 1;
        }
        if (cumulativeSize < 0) {
            System.err.println("Published cumulative size must be non-negative");
            return 1;
        }
        if (offset == -1 && cumulativeSize != 0) {
            System.err.println("Published cumulative size must be 0 when offset is -1");
            return 1;
        }
        if (StringUtils.isEmpty(oxiaServerAddr)) {
            var config = Admin.getStorageConfig(parent.getConfigFile());
            var oxiaUrl = UrsaStorage.validateOxiaUrl(config.getOxiaStorageUrl());
            oxiaServerAddr = oxiaUrl.getLeft();
            oxiaNamespace = oxiaUrl.getRight();
        }

        try (var oxiaClient = OxiaClientBuilder.create(oxiaServerAddr).namespace(oxiaNamespace).asyncClient().get()) {
            OxiaCompactTaskManager taskManager = new OxiaCompactTaskManager(oxiaClient);
            String name = StreamNames.normalize(stream);
            taskManager.updatePublishedOffset(name, streamId, offset, cumulativeSize);
            System.out.println("Updated publish task offset for " + stream
                    + " to (streamId=" + streamId + ", offset=" + offset
                    + ", cumulativeSize=" + cumulativeSize + ")");
        } catch (Exception e) {
            System.err.println("Error updating publish task offset: " + e.getMessage());
            return 1;
        }
        return 0;
    }
}
