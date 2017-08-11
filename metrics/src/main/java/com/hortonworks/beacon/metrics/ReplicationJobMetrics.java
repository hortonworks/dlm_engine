/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.metrics;

/**
 * List of counters for replication job.
 */
public enum ReplicationJobMetrics {
    TIMETAKEN("TIMETAKEN", "time taken by the distcp job"),
    TOTALMAPTASKS("TOTALMAPTASKS", "total map tasks of job"),
    COMPLETEDMAPTASKS("COMPLETEDMAPTASKS", "number of map tasks completed"),
    BYTESCOPIED("BYTESCOPIED", "number of bytes copied"),
    COPY("COPY", "number of files copied");

    private final String name;
    private final String description;

    ReplicationJobMetrics(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public static ReplicationJobMetrics getCountersKey(String counterKey) {
        if (counterKey != null) {
            for (ReplicationJobMetrics value : ReplicationJobMetrics.values()) {
                if (counterKey.equals(value.getName())) {
                    return value;
                }
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return getName();
    }
}
