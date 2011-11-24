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

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.util.HiveActionType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for Hive Replication metrics.
 */
public class HiveReplicationMetricsTest {
    private List<String> bootstrapDump;
    private List<String> bootstrapLoad;
    private List<String> incrementalDump;
    private List<String> incrementalLoad;

    @BeforeClass
    public void setup() {
        bootstrapDump = getBootStrapDump();
        bootstrapLoad = getBootStrapLoad();
        incrementalDump = getIncrementalDump();
        incrementalLoad = getIncrementalLoad();
    }

    @Test
    public void testBootStrapmetrics() throws BeaconException {
        JobContext jobContext = new JobContext();
        jobContext.setJobInstanceId("/source/source/dummyRepl/0/1495688249800/00001@1");
        Map<String, String> jobContextMap = new HashMap<>();
        jobContextMap.put(BeaconConstants.START_TIME, String.valueOf(System.currentTimeMillis()));
        jobContext.setJobContextMap(jobContextMap);

        HiveReplicationMetrics hiveReplicationMetrics = new HiveReplicationMetrics();
        hiveReplicationMetrics.obtainJobMetrics(jobContext, bootstrapDump,
                HiveActionType.EXPORT);
        Progress progress = hiveReplicationMetrics.getJobProgress();

        Assert.assertEquals(progress.getExportTotal(), 4);
        Assert.assertEquals(progress.getExportCompleted(), 4);
        Assert.assertEquals(progress.getImportTotal(), 4);
        Assert.assertEquals(progress.getImportCompleted(), 0);

        hiveReplicationMetrics.obtainJobMetrics(jobContext, bootstrapLoad,
                HiveActionType.IMPORT);

        progress = hiveReplicationMetrics.getJobProgress();

        Assert.assertEquals(progress.getExportTotal(), 4);
        Assert.assertEquals(progress.getExportCompleted(), 4);
        Assert.assertEquals(progress.getImportTotal(), 4);
        Assert.assertEquals(progress.getImportCompleted(), 4);
    }

    @Test
    public void testIncrementalmetrics() throws BeaconException {
        JobContext jobContext = new JobContext();
        jobContext.setJobInstanceId("/source/source/dummyRepl/0/1495688249800/00001@2");

        Map<String, String> jobContextMap = new HashMap<>();
        jobContextMap.put(BeaconConstants.START_TIME, String.valueOf(System.currentTimeMillis()));
        jobContext.setJobContextMap(jobContextMap);

        HiveReplicationMetrics hiveReplicationMetrics = new HiveReplicationMetrics();
        hiveReplicationMetrics.obtainJobMetrics(jobContext, incrementalDump,
                HiveActionType.EXPORT);
        Progress progress = hiveReplicationMetrics.getJobProgress();

        Assert.assertEquals(progress.getExportTotal(), 10);
        Assert.assertEquals(progress.getExportCompleted(), 10);
        Assert.assertEquals(progress.getImportTotal(), 10);
        Assert.assertEquals(progress.getImportCompleted(), 0);


        hiveReplicationMetrics.obtainJobMetrics(jobContext, incrementalLoad,
                HiveActionType.IMPORT);

        progress = hiveReplicationMetrics.getJobProgress();

        Assert.assertEquals(progress.getImportTotal(), 10);
        Assert.assertEquals(progress.getImportCompleted(), 10);
    }


    private List<String> getBootStrapDump() {
        List<String> dump = new ArrayList<>();
        dump.add("INFO  : REPL::START: {\"dbName\":\"default\",\"dumpType\":\"BOOTSTRAP\",\"estimatedNumTables\":4,"
                + "\"estimatedNumFunctions\":2,\"dumpStartTime\":1504271329}");
        dump.add("INFO  : REPL::FUNCTION_DUMP: {\"dbName\":\"default\",\"functionName\":\"f1\","
                + "\"functionsDumpProgress\":\"1/2\",\"dumpTime\":1504271329}");
        dump.add("INFO  : REPL::FUNCTION_DUMP: {\"dbName\":\"default\",\"functionName\":\"f2\","
                + "\"functionsDumpProgress\":\"2/2\",\"dumpTime\":1504271330}");
        dump.add("INFO  : REPL::TABLE_DUMP: {\"dbName\":\"default\",\"tableName\":\"t1\","
                + "\"tableType\":\"MANAGED_TABLE\",\"tablesDumpProgress\":\"1/4\",\"dumpTime\":1504271331}");
        dump.add("INFO  : REPL::TABLE_DUMP: {\"dbName\":\"default\",\"tableName\":\"t2\","
                + "\"tableType\":\"MANAGED_TABLE\",\"tablesDumpProgress\":\"2/4\",\"dumpTime\":1504271332}");
        dump.add("INFO  : REPL::TABLE_DUMP: {\"dbName\":\"default\",\"tableName\":\"v1\","
                + "\"tableType\":\"VIRTUAL_VIEW\",\"tablesDumpProgress\":\"3/4\",\"dumpTime\":1504271332}");
        dump.add("INFO  : REPL::TABLE_DUMP: {\"dbName\":\"default\",\"tableName\":\"v2\","
                + "\"tableType\":\"MATERIALIZED_VIEW\",\"tablesDumpProgress\":\"4/4\",\"dumpTime\":1504271333}");
        dump.add("INFO  : REPL::END: {\"dbName\":\"default\",\"dumpType\":\"BOOTSTRAP\",\"actualNumTables\":4,"
                + "\"actualNumFunctions\":2,\"dumpEndTime\":1504271333,\"dumpDir\":\"/tmp/dump/next\",\"lastReplId\":"
                + "\"13\"}");

        return dump;
    }

    private List<String> getBootStrapLoad() {
        List<String> load = new ArrayList<>();
        load.add("INFO  : REPL::START: {\"dbName\":\"repl\",\"dumpDir\":\"/tmp/dump/next\",\"loadType\":\"BOOTSTRAP\","
                + "\"numTables\":4,\"numFunctions\":2,\"loadStartTime\":1504271416}");
        load.add("INFO  : REPL::FUNCTION_LOAD: {\"dbName\":\"repl\",\"functionName\":\"f1\","
                + "\"functionsLoadProgress\":\"1/2\",\"loadTime\":1504271417}");
        load.add("INFO  : REPL::FUNCTION_LOAD: {\"dbName\":\"repl\",\"functionName\":\"f2\","
                + "\functionsLoadProgress\":\"2/2\",\"loadTime\":1504271417}");
        load.add("INFO  : REPL::TABLE_LOAD: {\"dbName\":\"repl\",\"tableName\":\"v1\",\"tableType\":\"VIRTUAL_VIEW\","
                + "\"tablesLoadProgress\":\"1/4\",\"loadTime\":1504271418}");
        load.add("INFO  : REPL::TABLE_LOAD: {\"dbName\":\"repl\",\"tableName\":\"t1\",\"tableType\":\"MANAGED_TABLE\","
                + "\"tablesLoadProgress\":\"2/4\",\"loadTime\":1504271419}");
        load.add("INFO  : REPL::TABLE_LOAD: {\"dbName\":\"repl\",\"tableName\":\"v2\","
                + "\"tableType\":\"MATERIALIZED_VIEW\",\"tablesLoadProgress\":\"3/4\",\"loadTime\":1504271419}");
        load.add("INFO  : REPL::TABLE_LOAD: {\"dbName\":\"repl\",\"tableName\":\"t2\",\"tableType\":\"MANAGED_TABLE\","
                + "\"tablesLoadProgress\":\"4/4\",\"loadTime\":1504271419}");
        load.add("INFO  : REPL::END: {\"dbName\":\"repl\",\"loadType\":\"BOOTSTRAP\",\"numTables\":4,"
                + "\"numFunctions\":2,\"loadEndTime\":1504271419,\"dumpDir\":\"/tmp/dump/next\","
                + "\"lastReplId\":\"13\"}");

        return load;
    }

    private List<String> getIncrementalDump() {
        List<String> dump = new ArrayList<>();
        dump.add("INFO  : REPL::START: {\"dbName\":\"default\",\"dumpType\":\"INCREMENTAL\",\"estimatedNumEvents\":10,"
                + "\"dumpStartTime\":1504271603}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"31\","
                + "\"eventType\":\"EVENT_ADD_PARTITION\",\"eventsDumpProgress\":\"1/10\",\"dumpTime\":1504271604}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"32\","
                + "\"eventType\":\"EVENT_ALTER_PARTITION\",\"eventsDumpProgress\":\"2/10\",\"dumpTime\":1504271605}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"33\","
                + "\"eventType\":\"EVENT_ALTER_TABLE\",\"eventsDumpProgress\":\"3/10\",\"dumpTime\":1504271606}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"34\",\"eventType\":\"EVENT_INSERT\","
                + "\"eventsDumpProgress\":\"4/10\",\"dumpTime\":1504271607}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"35\","
                + "\"eventType\":\"EVENT_ALTER_TABLE\",\"eventsDumpProgress\":\"5/10\",\"dumpTime\":1504271608}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"36\",\"eventType\":"
                + "\"EVENT_TRUNCATE_PARTITION\",\"eventsDumpProgress\":\"6/10\",\"dumpTime\":1504271609}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"37\","
                + "\"eventType\":\"EVENT_ALTER_PARTITION\",\"eventsDumpProgress\":\"7/10\",\"dumpTime\":1504271609}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"38\","
                + "\"eventType\":\"EVENT_CREATE_TABLE\",\"eventsDumpProgress\":\"8/10\",\"dumpTime\":1504271611}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"39\","
                + "\"eventType\":\"EVENT_ALTER_TABLE\",\"eventsDumpProgress\":\"9/10\",\"dumpTime\":1504271611}");
        dump.add("INFO  : REPL::EVENT_DUMP: {\"dbName\":\"default\",\"eventId\":\"40\","
                + "\"eventType\":\"EVENT_DROP_FUNCTION\",\"eventsDumpProgress\":\"10/10\",\"dumpTime\":1504271612}");
        dump.add("INFO  : REPL::END: {\"dbName\":\"default\",\"dumpType\":\"INCREMENTAL\",\"actualNumEvents\":10,"
                + "\"dumpEndTime\":1504271612,\"dumpDir\":\"/tmp/dump/next\",\"lastReplId\":\"40\"}");

        return dump;
    }

    private List<String> getIncrementalLoad() {
        List<String> load = new ArrayList<>();
        load.add("INFO  : REPL::START: {\"dbName\":\"repl\",\"dumpDir\":\"/tmp/dump/next\","
                + "\"loadType\":\"INCREMENTAL\",\"numEvents\":10,\"loadStartTime\":1504271667}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"31\","
                + "\"eventType\":\"EVENT_ADD_PARTITION\",\"eventsLoadProgress\":\"1/10\",\"loadTime\":1504271667}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"32\","
                + "\"eventType\":\"EVENT_ALTER_PARTITION\",\"eventsLoadProgress\":\"2/10\",\"dumpTime\":1504271667}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"33\","
                + "\"eventType\":\"EVENT_ALTER_TABLE\",\"eventsLoadProgress\":\"3/10\",\"loadTime\":1504271667}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"34\",\"eventType\":\"EVENT_INSERT\","
                + "\"eventsLoadProgress\":\"4/10\",\"loadTime\":1504271668}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"35\","
                + "\"eventType\":\"EVENT_ALTER_TABLE\",\"eventsLoadProgress\":\"5/10\",\"loadTime\":1504271668}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"36\","
                + "\"eventType\":\"EVENT_TRUNCATE_PARTITION\",\"eventsLoadProgress\":\"6/10\","
                + "\"loadTime\":1504271668}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"37\","
                + "\"eventType\":\"EVENT_ALTER_PARTITION\",\"eventsLoadProgress\":\"7/10\",\"loadTime\":1504271668}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"38\","
                + "\"eventType\":\"EVENT_CREATE_TABLE\",\"eventsLoadProgress\":\"8/10\",\"loadTime\":1504271669}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"39\","
                + "\"eventType\":\"EVENT_ALTER_TABLE\",\"eventsLoadProgress\":\"9/10\",\"loadTime\":1504271669}");
        load.add("INFO  : REPL::EVENT_LOAD: {\"dbName\":\"repl\",\"eventId\":\"40\","
                + "\"eventType\":\"EVENT_DROP_FUNCTION\",\"eventsLoadProgress\":\"10/10\",\"loadTime\":1504271669}");
        load.add("INFO  : REPL::END: {\"dbName\":\"repl\",\"loadType\":\"INCREMENTAL\",\"numEvents\":10,"
                + "\"loadEndTime\":1504271669,\"dumpDir\":\" / tmp / dump / next\",\"lastReplId\":\"40\"}");

        return load;
    }
}
