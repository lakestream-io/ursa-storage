/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Architectural rules enforced mechanically via ArchUnit.
 */
@AnalyzeClasses(
        packages = "io.lakestream.ursa",
        importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule NO_SYSTEM_OUT = noClasses()
            .should().accessField(System.class, "out")
            .because("Use SLF4J (@Slf4j) for logging, never System.out");

    @ArchTest
    static final ArchRule NO_SYSTEM_ERR = noClasses()
            .should().accessField(System.class, "err")
            .because("Use SLF4J (@Slf4j) for logging, never System.err");
}
