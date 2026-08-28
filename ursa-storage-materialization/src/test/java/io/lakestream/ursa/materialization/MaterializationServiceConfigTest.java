/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MaterializationServiceConfigTest {

    @Test
    void defaultsProduceValidInstance() {
        MaterializationServiceConfig config = MaterializationServiceConfig.defaults();
        assertThat(config.workerPoolSize()).isEqualTo(8);
        assertThat(config.walReadRateLimitWindow()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.walReadRateLimitBytes()).isEqualTo(52_428_800L);
        assertThat(config.additionalProperties()).isEmpty();
    }

    @Test
    void rejectsZeroWorkerPoolSize() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new MaterializationServiceConfig(
                        0, Duration.ofSeconds(1), 1L, Map.of()))
                .withMessageContaining("workerPoolSize");
    }

    @Test
    void rejectsNegativeWorkerPoolSize() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new MaterializationServiceConfig(
                        -1, Duration.ofSeconds(1), 1L, Map.of()))
                .withMessageContaining("workerPoolSize");
    }

    @Test
    void rejectsNullWindow() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationServiceConfig(
                        1, null, 1L, Map.of()))
                .withMessageContaining("walReadRateLimitWindow");
    }

    @Test
    void rejectsZeroByteLimit() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new MaterializationServiceConfig(
                        1, Duration.ofSeconds(1), 0L, Map.of()))
                .withMessageContaining("walReadRateLimitBytes");
    }

    @Test
    void rejectsNullAdditionalProperties() {
        assertThatNullPointerException()
                .isThrownBy(() -> new MaterializationServiceConfig(
                        1, Duration.ofSeconds(1), 1L, null))
                .withMessageContaining("additionalProperties");
    }

    @Test
    void defensiveCopyOfAdditionalProperties() {
        Map<String, String> source = new HashMap<>();
        source.put("k", "v");

        MaterializationServiceConfig config = new MaterializationServiceConfig(
                4, Duration.ofMillis(500), 1024L, source);

        source.put("k", "MUTATED");
        assertThat(config.additionalProperties()).containsEntry("k", "v");

        assertThatThrownBy(() -> config.additionalProperties().put("a", "b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
