/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Architectural rules enforced mechanically via ArchUnit.
 * Ensures Iceberg and Delta table format packages remain isolated in v2 code.
 * Legacy v1 code has known cross-references that are not enforced here.
 */
@AnalyzeClasses(
        packages = "io.lakestream.ursa.lakehouse.v2",
        importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule V2_ICEBERG_DOES_NOT_DEPEND_ON_DELTA = noClasses()
            .that().resideInAPackage("..v2.iceberg..")
            .should().dependOnClassesThat()
            .resideInAPackage("..v2.delta..")
            .because("V2 Iceberg and Delta packages must remain isolated");

    @ArchTest
    static final ArchRule V2_DELTA_DOES_NOT_DEPEND_ON_ICEBERG = noClasses()
            .that().resideInAPackage("..v2.delta..")
            .should().dependOnClassesThat()
            .resideInAPackage("..v2.iceberg..")
            .because("V2 Delta and Iceberg packages must remain isolated");
}
