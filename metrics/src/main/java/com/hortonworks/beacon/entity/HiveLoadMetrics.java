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
 * Class for representing REPL load query log metrics.
 */
public class HiveLoadMetrics {

    private String dbName;

    private String loadType;

    private String tableName;

    private String dumpDir;

    private long numTables;

    private long numEvents;

    private String tablesLoadProgress;

    private String eventsLoadProgress;

    private long loadTime;

    private long loadEndTime;

    private long lastReplId;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getLoadType() {
        return loadType;
    }

    public void setLoadType(String loadType) {
        this.loadType = loadType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDumpDir() {
        return dumpDir;
    }

    public void setDumpDir(String dumpDir) {
        this.dumpDir = dumpDir;
    }

    public long getNumTables() {
        return numTables;
    }

    public void setNumTables(long numTables) {
        this.numTables = numTables;
    }

    public long getNumEvents() {
        return numEvents;
    }

    public void setNumEvents(long numEvents) {
        this.numEvents = numEvents;
    }

    public String getTablesLoadProgress() {
        return tablesLoadProgress;
    }

    public void setTablesLoadProgress(String tablesLoadProgress) {
        this.tablesLoadProgress = tablesLoadProgress;
    }

    public String getEventsLoadProgress() {
        return eventsLoadProgress;
    }

    public void setEventsLoadProgress(String eventsLoadProgress) {
        this.eventsLoadProgress = eventsLoadProgress;
    }

    public long getLoadTime() {
        return loadTime;
    }

    public void setLoadTime(long loadTime) {
        this.loadTime = loadTime;
    }

    public long getLoadEndTime() {
        return loadEndTime;
    }

    public void setLoadEndTime(long loadEndTime) {
        this.loadEndTime = loadEndTime;
    }

    public long getLastReplId() {
        return lastReplId;
    }

    public void setLastReplId(long lastReplId) {
        this.lastReplId = lastReplId;
    }

    public long getTotalLoadTable(HiveReplEventType hiveReplEventType) {
        String value;
        if (hiveReplEventType == HiveReplEventType.TABLE_LOAD) {
            value = tablesLoadProgress;
        } else {
            value = eventsLoadProgress;
        }
        return value != null ? Long.parseLong(value.split("/")[1]) : 0;
    }

    public long getCompletedLoadTable(HiveReplEventType hiveReplEventType) {
        String value;
        if (hiveReplEventType == HiveReplEventType.TABLE_LOAD) {
            value = tablesLoadProgress;
        } else {
            value = eventsLoadProgress;
        }
        return value != null ? Long.parseLong(value.split("/")[0]) : 0;
    }
}
