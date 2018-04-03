/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.metrics;

/**
 * List of counters for replication job.
 */
public enum ReplicationJobMetrics {
    TIMETAKEN("TIMETAKEN", "time taken by the distcp job"),
    TOTAL("TOTAL", "number of maptasks/table/event required to copy"),
    EXPORT_TOTAL("EXPORT_TOTAL", "number of maptasks/table/event required to export"),
    IMPORT_TOTAL("IMPORT_TOTAL", "number of maptasks/table/event required to import"),
    COMPLETED("COMPLETED", "number of map tasks/table/event completed"),
    EXPORT_COMPLETED("EXPORT_COMPLETED", "number of export tasks/table/event completed"),
    IMPORT_COMPLETED("IMPORT_COMPLETED", "number of import tasks/table/event completed"),
    FAILED("FAILED", "number of map tasks failed"),
    KILLED("KILLED", "number of map tasks killed"),
    BYTESCOPIED("BYTESCOPIED", "number of bytes copied"),
    PROGRESS("PROGRESS", "map progress in percentage"),
    COPY("COPY", "number of files copied"),
    DIR_COPY("DIR_COPY", "number of directories copied"),
    UNIT("UNIT", "unit of captured metrics");

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
