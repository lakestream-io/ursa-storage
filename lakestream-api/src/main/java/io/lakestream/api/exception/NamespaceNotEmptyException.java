/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.exception;

/**
 * Thrown when attempting to drop a namespace that still contains streams.
 */
public class NamespaceNotEmptyException extends RuntimeException {

    public NamespaceNotEmptyException(String namespaceName) {
        super("Namespace is not empty: " + namespaceName);
    }

    public NamespaceNotEmptyException(String namespaceName, Throwable cause) {
        super("Namespace is not empty: " + namespaceName, cause);
    }
}
