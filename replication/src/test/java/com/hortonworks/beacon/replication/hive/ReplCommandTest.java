/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Replication Command Test.
 */

public class ReplCommandTest {

    private static final BeaconLog LOG = BeaconLog.getLog(ReplCommandTest.class);
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
    public void testReplStatus() {
        LOG.info("Executing Replication Status");
        String database = hiveJobDetails.getProperties().getProperty(HiveDRProperties.SOURCE_DATASET.getName());
        ReplCommand replStatus = new ReplCommand(database);
        Assert.assertEquals(replStatus.getReplStatus(),
                "REPL STATUS testDB");
    }
}
