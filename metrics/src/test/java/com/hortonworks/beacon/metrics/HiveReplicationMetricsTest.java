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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.util.HiveActionType;
import com.hortonworks.beacon.util.ReplicationType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
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

        JobMetrics hiveReplicationMetrics = JobMetricsHandler.getMetricsType(ReplicationType.HIVE);
        ((HiveReplicationMetrics) hiveReplicationMetrics).obtainJobMetrics(jobContext, bootstrapDump,
                HiveActionType.EXPORT);
        Map<String, Long> metrics = ((HiveReplicationMetrics) hiveReplicationMetrics).getMetricsMap();

        Assert.assertEquals(metrics.get("TOTAL").longValue(), 4);
        Assert.assertEquals(metrics.get("COMPLETED").longValue(), 0);

        ((HiveReplicationMetrics) hiveReplicationMetrics).obtainJobMetrics(jobContext, bootstrapLoad,
                HiveActionType.IMPORT);

        metrics = ((HiveReplicationMetrics) hiveReplicationMetrics).getMetricsMap();

        Assert.assertEquals(metrics.get("TOTAL").longValue(), 4);
        Assert.assertEquals(metrics.get("COMPLETED").longValue(), 4);

        ReplicationMetrics replicationMetrics = new ReplicationMetrics();
        replicationMetrics.updateReplicationMetricsDetails(metrics, ProgressUnit.TABLE);
        Assert.assertEquals(replicationMetrics.getProgress().getTotal(), 4);
        Assert.assertEquals(replicationMetrics.getProgress().getTotal(), 4);
        Assert.assertEquals(replicationMetrics.getProgress().getUnit(), ProgressUnit.TABLE.getName());
    }

    @Test
    public void testIncrementalmetrics() throws BeaconException {
        JobContext jobContext = new JobContext();
        jobContext.setJobInstanceId("/source/source/dummyRepl/0/1495688249800/00001@2");

        JobMetrics hiveReplicationMetrics = JobMetricsHandler.getMetricsType(ReplicationType.HIVE);
        ((HiveReplicationMetrics) hiveReplicationMetrics).obtainJobMetrics(jobContext, incrementalDump,
                HiveActionType.EXPORT);
        Map<String, Long> metrics = ((HiveReplicationMetrics) hiveReplicationMetrics).getMetricsMap();

        Assert.assertEquals(metrics.get("TOTAL").longValue(), 10);
        Assert.assertEquals(metrics.get("COMPLETED").longValue(), 0);

        ((HiveReplicationMetrics) hiveReplicationMetrics).obtainJobMetrics(jobContext, incrementalLoad,
                HiveActionType.IMPORT);

        metrics = ((HiveReplicationMetrics) hiveReplicationMetrics).getMetricsMap();

        Assert.assertEquals(metrics.get("TOTAL").longValue(), 10);
        Assert.assertEquals(metrics.get("COMPLETED").longValue(), 10);

        ReplicationMetrics replicationMetrics = new ReplicationMetrics();
        replicationMetrics.updateReplicationMetricsDetails(metrics, ProgressUnit.EVENTS);
        Assert.assertEquals(replicationMetrics.getProgress().getTotal(), 10);
        Assert.assertEquals(replicationMetrics.getProgress().getTotal(), 10);
        Assert.assertEquals(replicationMetrics.getProgress().getUnit(), ProgressUnit.EVENTS.getName());
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
