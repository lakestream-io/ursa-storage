/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

/**
 * Small utility that reflectively loads a {@link MaterializationService}
 * implementation by class name.
 *
 * <p>The orchestrator (T10) reads a deployment-supplied class name from config
 * (analogous to today's {@code compactionServiceClass}) and asks this provider
 * to instantiate it via the public no-arg constructor. The instance is then
 * initialized by the caller through
 * {@link MaterializationService#initialize(MaterializationRuntime, MaterializationServiceConfig)}.
 *
 * <p>Lifecycle of failures: any reflective failure (missing class, missing
 * constructor, exception during construction) is rethrown as an
 * {@link IllegalStateException} so the orchestrator can fail fast at startup.
 */
public final class MaterializationServiceProvider {

    private MaterializationServiceProvider() {
        // utility class
    }

    /**
     * Loads and instantiates the {@link MaterializationService} class named
     * {@code className} via its public no-arg constructor.
     *
     * @param className fully qualified class name of the {@link MaterializationService} impl
     * @return a fresh, uninitialised {@link MaterializationService} instance
     * @throws IllegalStateException if the class cannot be loaded or instantiated
     */
    public static MaterializationService load(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (MaterializationService) clazz.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to load MaterializationService class: " + className, e);
        }
    }
}
