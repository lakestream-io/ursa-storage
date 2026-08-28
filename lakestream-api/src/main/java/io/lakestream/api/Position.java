/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Position represents the location of data within the storage system.
 * It is a crucial component in the Ursa storage architecture, used to identify
 * and retrieve specific pieces of data across various storage backends.
 *
 * This class is designed to work in conjunction with WalStorage and FileStorage interfaces,
 * providing a unified way to reference data regardless of the underlying storage mechanism.
 *
 * In the context of WalStorage, Position is used to identify the location of entries
 * within the Write-Ahead Log. For FileStorage, it represents the physical location
 * of data in the file system or object storage.
 *
 * The Position class is immutable, ensuring thread-safety and preventing accidental modifications.
 */
public record Position(
    FileInfo file,
    long indexId, // the index id of the data in the location
    FileType fileType
) {
    public enum FileType {
        RAW,
        PARQUET,
    }

    /**
     * Constructs a Position with only a location, setting indexId to -1.
     * This constructor is useful when the exact index within a location is not relevant or known.
     *
     * @param location The storage location of the data.
     */
    public Position(String location) {
        this(location, -1, FileType.RAW);
    }

    public Position(String location, long indexId, FileType fileType) {
        // size should be >= 0 as readcache.weight
        this(new FileInfo(location, 0), indexId, fileType);
    }

    public Position(String location, long fileSize, long indexId, FileType fileType) {
        this(new FileInfo(location, fileSize), indexId, fileType);
    }


    /**
     * Creates a Position object from a v1 formatted string representation.
     *
     * @param pos A string in the format "location-indexId"
     * @return A new Position object
     * @throws IllegalArgumentException if the formatted string is invalid
     */
    public static Position parseV1Format(String pos) {
        int splitIndex = pos.lastIndexOf("-");
        if (splitIndex == -1 || splitIndex == pos.length() - 1) {
            throw new IllegalArgumentException("Invalid position format: " + pos);
        }
        String location = pos.substring(0, splitIndex);
        long indexId = Long.parseLong(pos.substring(splitIndex + 1));
        return new Position(location, indexId, FileType.RAW);
    }

    /**
     * Creates a Position object from a v2 formatted string representation.
     *
     * @param input string in the format "location-indexId-fileType"
     * @return A new Position object
     * @throws IllegalArgumentException if the formatted string is invalid
     */
    public static Position parseV2Format(String input) {
        String formattedPos = input;
        int length = formattedPos.length();
        int splitIndex = formattedPos.lastIndexOf("-");
        if (splitIndex == -1 || splitIndex == formattedPos.length() - 1) {
            throw new IllegalArgumentException("Invalid position format: " + input);
        }
        String fileType = formattedPos.substring(splitIndex + 1, length);

        formattedPos = formattedPos.substring(0, splitIndex);
        length = formattedPos.length();
        splitIndex = formattedPos.lastIndexOf("-");
        if (splitIndex == -1 || splitIndex == formattedPos.length() - 1) {
            throw new IllegalArgumentException("Invalid position format: " + input);
        }
        long offset = Long.parseLong(formattedPos.substring(splitIndex + 1, length));

        String location = formattedPos.substring(0, splitIndex);
        return new Position(location, offset, FileType.valueOf(fileType));
    }

    /**
     * Creates a new Position object with the same location but without an indexId.
     * This method is useful when you need to reference the general location without
     * specifying a particular index within that location.
     *
     * @return A new Position object with the same location and indexId set to -1
     */
    public Position withoutIndexId() {
        return new Position(file.location());
    }

    /**
     * Returns a string representation of this Position.
     * This method is useful for serializing Position objects, especially
     * when storing metadata or passing positions between system components.
     *
     * @return A string in the format "location-indexId-fileType"
     */
    public String toString(int formatVersion) {
        if (formatVersion == 1) {
            return toV1Format();
        }
        if (formatVersion == 2) {
            return toV2Format();
        }
        throw new IllegalArgumentException("Invalid format version: " + formatVersion);
    }

    /**
     * This the v1 version of the position which does not include the file type.
     * It is used for testing the compatibility.
     *
     * Returns a string representation of this Position.
     * This method is useful for serializing Position objects, especially
     * when storing metadata or passing positions between system components.
     *
     * @return A string in the format "location-indexId"
     */
    public String toV1Format() {
        return String.format("%s-%d", file.location(), indexId);
    }

    /**
     * Returns a string representation of this Position.
     * This method is useful for serializing Position objects, especially
     * when storing metadata or passing positions between system components.
     *
     * @return A string in the format "location-indexId-fileType"
     */
    public String toV2Format() {
        return String.format("%s-%d-%s", file.location(), indexId, fileType.name());
    }

    public String location() {
        return this.file().location();
    }

    public long size() {
        return this.file.size();
    }

    /**
     * Represents a non-existent or invalid entry.
     * Use this constant when an entry is not found or to initialize empty states.
     */
    public static final Position NOT_FOUND = new Position("", -1, FileType.RAW);

    public boolean isBinary() {
        return fileType == FileType.RAW;
    }
}
