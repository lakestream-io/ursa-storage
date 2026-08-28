/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test.containers.util;

import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.output.OutputFrame;

@AllArgsConstructor
@Slf4j
public class ContainerLogger implements Consumer<OutputFrame>{

    private final String hostname;

    @Override
    public void accept(OutputFrame outputFrame) {
        log.info(" >>> [{}] <<< {}", hostname,  outputFrame.getUtf8StringWithoutLineEnding());
    }
}
