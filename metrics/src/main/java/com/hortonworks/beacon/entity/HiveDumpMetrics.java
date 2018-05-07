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
package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.HiveReplEventType;

/**
 * Class for representing REPL dump query log metrics.
 */
public class HiveDumpMetrics {

    private String dbName;

    private String dumpType;

    private String eventType;

    private String dumpDir;

    private long estimatedNumTables;

    private long estimatedNumEvents;

    private long actualNumTables;

    private long actualNumEvents;

    private long dumpStartTime;

    private long dumpEndTime;

    private String tableName;

    private String tableType;

    private String tablesDumpProgress;

    private String eventsDumpProgress;

    private long dumpTime;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDumpType() {
        return dumpType;
    }

    public void setDumpType(String dumpType) {
        this.dumpType = dumpType;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getDumpDir() {
        return dumpDir;
    }

    public void setDumpDir(String dumpDir) {
        this.dumpDir = dumpDir;
    }

    public long getEstimatedNumTables() {
        return estimatedNumTables;
    }

    public void setEstimatedNumTables(long estimatedNumTables) {
        this.estimatedNumTables = estimatedNumTables;
    }

    public long getEstimatedNumEvents() {
        return estimatedNumEvents;
    }

    public void setEstimatedNumEvents(long estimatedNumEvents) {
        this.estimatedNumEvents = estimatedNumEvents;
    }

    public long getActualNumTables() {
        return actualNumTables;
    }

    public void setActualNumTables(long actualNumTables) {
        this.actualNumTables = actualNumTables;
    }

    public long getActualNumEvents() {
        return actualNumEvents;
    }

    public void setActualNumEvents(long actualNumEvents) {
        this.actualNumEvents = actualNumEvents;
    }

    public long getDumpStartTime() {
        return dumpStartTime;
    }

    public void setDumpStartTime(long dumpStartTime) {
        this.dumpStartTime = dumpStartTime;
    }

    public long getDumpEndTime() {
        return dumpEndTime;
    }

    public void setDumpEndTime(long dumpEndTime) {
        this.dumpEndTime = dumpEndTime;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getTablesDumpProgress() {
        return tablesDumpProgress;
    }

    public void setTablesDumpProgress(String tablesDumpProgress) {
        this.tablesDumpProgress = tablesDumpProgress;
    }

    public String getEventsDumpProgress() {
        return eventsDumpProgress;
    }

    public void setEventsDumpProgress(String eventsDumpProgress) {
        this.eventsDumpProgress = eventsDumpProgress;
    }

    public long getDumpTime() {
        return dumpTime;
    }

    public void setDumpTime(long dumpTime) {
        this.dumpTime = dumpTime;
    }

    public long getTotalDumpTable(HiveReplEventType hiveReplEventType) {
        String value;
        if (hiveReplEventType == HiveReplEventType.TABLE_DUMP) {
            value = tablesDumpProgress;
        } else {
            value = eventsDumpProgress;
        }
        return value != null ? Long.parseLong(value.split("/")[1]) : 0;
    }

    public long getCompletedDumpTable(HiveReplEventType hiveReplEventType) {
        String value;
        if (hiveReplEventType == HiveReplEventType.TABLE_DUMP) {
            value = tablesDumpProgress;
        } else {
            value = eventsDumpProgress;
        }
        return value != null ? Long.parseLong(value.split("/")[0]) : 0;
    }

}
