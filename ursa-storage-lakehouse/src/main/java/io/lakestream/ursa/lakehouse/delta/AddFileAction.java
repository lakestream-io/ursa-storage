/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class AddFileAction {

    private String path;
    private long size;
    private Map<String, String> partitionValues;
    private long modificationTime;
    private String stats;
    private boolean dataChange;
    private DeletionVector deletionVector;
    private Map<String, String> tags;

    public String getPath() {
        return path;
    }

    public long getSize() {
        return size;
    }

    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public String getStats() {
        return stats;
    }

    public boolean isDataChange() {
        return dataChange;
    }

    public DeletionVector getDeletionVector() {
        return deletionVector;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public static class DeletionVector {
        private String storageType;
        private String pathOrInlineDv;
        private Integer offset;
        private Integer sizeInBytes;
        private Long cardinality;

        public DeletionVector(String storageType, String pathOrInlineDv, Integer offset, Integer sizeInBytes,
                              Long cardinality) {
            this.storageType = storageType;
            this.pathOrInlineDv = pathOrInlineDv;
            this.offset = offset;
            this.sizeInBytes = sizeInBytes;
            this.cardinality = cardinality;
        }

        public String getStorageType() {
            return storageType;
        }

        public String getPathOrInlineDv() {
            return pathOrInlineDv;
        }

        public Integer getOffset() {
            return offset;
        }

        public Integer getSizeInBytes() {
            return sizeInBytes;
        }

        public Long getCardinality() {
            return cardinality;
        }
    }

}
