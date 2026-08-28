/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * Internal helpers for persisting materialization records (table catalogs and
 * materialization policies) into the Oxia-backed catalog. The translation is
 * intentionally bespoke (rather than using Jackson's {@code Jdk8Module}) to
 * avoid an additional runtime dependency.
 */
package io.lakestream.ursa.lakestream.impl.materialization;
