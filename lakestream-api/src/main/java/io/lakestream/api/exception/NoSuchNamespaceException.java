/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api.exception;

/**
 * Thrown when a referenced namespace does not exist in the catalog.
 */
public class NoSuchNamespaceException extends RuntimeException {

    public NoSuchNamespaceException(String namespaceName) {
        super("Namespace not found: " + namespaceName);
    }

    public NoSuchNamespaceException(String namespaceName, Throwable cause) {
        super("Namespace not found: " + namespaceName, cause);
    }
}
