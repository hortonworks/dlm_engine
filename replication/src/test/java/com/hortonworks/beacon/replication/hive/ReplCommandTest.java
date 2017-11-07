/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */


package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.replication.ReplicationJobDetails;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Replication Command Test.
 */

public class ReplCommandTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReplCommandTest.class);
    private static final String FS_ENDPOINT = "hdfs://localhost:54136";
    private static final String HS_ENDPOINT = "hive2://localhost:10556";
    private static final String DUMP_DIRECTORY = FS_ENDPOINT+"/tmp/hive/repl/121";
    private ReplicationJobDetails hiveJobDetails;

    @BeforeClass
    public void setup() throws Exception {
        Properties hiveReplProps = new Properties();
        String[][] hiveReplAttrs = {
                {HiveDRProperties.JOB_NAME.getName(), "testHiveDR"},
                {HiveDRProperties.SOURCE_HS2_URI.getName(), HS_ENDPOINT, },
                {HiveDRProperties.SOURCE_DATASET.getName(), "testDB", },
                {HiveDRProperties.JOB_TYPE.getName(), "HIVE"},
                {HiveDRProperties.JOB_FREQUENCY.getName(), "3600"},
                {HiveDRProperties.QUEUE_NAME.getName(), "default"},
        };

        for (int i = 0; i < hiveReplAttrs.length; i++) {
            hiveReplProps.setProperty(hiveReplAttrs[i][0], hiveReplAttrs[i][1]);
        }
        hiveJobDetails = new ReplicationJobDetails(
                hiveReplProps.getProperty(HiveDRProperties.JOB_TYPE.getName()),
                hiveReplProps.getProperty(HiveDRProperties.JOB_NAME.getName()),
                hiveReplProps.getProperty(HiveDRProperties.JOB_TYPE.getName()),
                hiveReplProps);
    }


    @Test
    public void testReplDump() {
        LOG.info("Executing Replication Dump");
        String database = hiveJobDetails.getProperties().getProperty(HiveDRProperties.SOURCE_DATASET.getName());
        ReplCommand replDump = new ReplCommand(database);
        Assert.assertEquals(replDump.getReplDump(0L, 0L, 0),
                "REPL DUMP testDB");
        Assert.assertEquals(replDump.getReplDump(25, 0L, 0),
                "REPL DUMP testDB FROM 25");
        Assert.assertEquals(replDump.getReplDump(25, 0L, 10),
                "REPL DUMP testDB FROM 25 LIMIT 10");
    }

    @Test
    public void testReplLoad() {
        LOG.info("Executing Replication Load");
        String database = hiveJobDetails.getProperties().getProperty(HiveDRProperties.SOURCE_DATASET.getName());
        ReplCommand replLoad = new ReplCommand(database);
        Assert.assertEquals(replLoad.getReplLoad(DUMP_DIRECTORY),
                "REPL LOAD testDB FROM '" +DUMP_DIRECTORY+"'");
    }

    @Test
    public void testReplLoadWithProperties() {
        LOG.info("Executing Replication Load");
        String database = hiveJobDetails.getProperties().getProperty(HiveDRProperties.SOURCE_DATASET.getName());
        ReplCommand replLoad = new ReplCommand(database);
        Assert.assertEquals(replLoad.getReplLoad(DUMP_DIRECTORY),
                "REPL LOAD testDB FROM '" +DUMP_DIRECTORY+"'");
        String configParams = HiveDRUtils.setConfigParameters(hiveJobDetails.getProperties());
        Assert.assertEquals(replLoad.getReplLoad(DUMP_DIRECTORY) + " WITH ("+configParams+")",
                "REPL LOAD testDB FROM '" +DUMP_DIRECTORY+"' WITH ("
                        + "'mapreduce.job.queuename'='default','hive.exec.parallel'='true')");

    }

    @Test
    public void testReplStatus() {
        LOG.info("Executing Replication Status");
        String database = hiveJobDetails.getProperties().getProperty(HiveDRProperties.SOURCE_DATASET.getName());
        ReplCommand replStatus = new ReplCommand(database);
        Assert.assertEquals(replStatus.getReplStatus(),
                "REPL STATUS testDB");
    }
}
