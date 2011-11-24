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


package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.apache.hadoop.security.UserGroupInformation;
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
                {HiveDRProperties.TARGET_DATASET.getName(), "testDB1", },
                {HiveDRProperties.JOB_TYPE.getName(), "HIVE"},
                {HiveDRProperties.JOB_FREQUENCY.getName(), "3600"},
                {HiveDRProperties.QUEUE_NAME.getName(), "default"},
                {HiveDRProperties.SOURCE_HIVE_SERVER_AUTHENTICATION.getName(), "default"},
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
                "REPL DUMP `testDB`");
        Assert.assertEquals(replDump.getReplDump(25, 0L, 0),
                "REPL DUMP `testDB` FROM 25");
        Assert.assertEquals(replDump.getReplDump(25, 0L, 10),
                "REPL DUMP `testDB` FROM 25 LIMIT 10");
    }

    @Test
    public void testReplLoad() {
        LOG.info("Executing Replication Load");
        String database = hiveJobDetails.getProperties().getProperty(HiveDRProperties.SOURCE_DATASET.getName());
        ReplCommand replLoad = new ReplCommand(database);
        Assert.assertEquals(replLoad.getReplLoad(DUMP_DIRECTORY),
                "REPL LOAD `testDB` FROM '" +DUMP_DIRECTORY+"'");
    }

    @Test
    public void testReplLoadWithProperties() throws Exception {
        LOG.info("Executing Replication Load");
        String database = hiveJobDetails.getProperties().getProperty(HiveDRProperties.SOURCE_DATASET.getName());
        ReplCommand replLoad = new ReplCommand(database);
        Assert.assertEquals(replLoad.getReplLoad(DUMP_DIRECTORY),
                "REPL LOAD `testDB` FROM '" +DUMP_DIRECTORY+"'");
        String configParams = HiveDRUtils.setConfigParameters(hiveJobDetails.getProperties());
        String user = UserGroupInformation.getLoginUser().getShortUserName();
        Assert.assertEquals(replLoad.getReplLoad(DUMP_DIRECTORY) + " WITH ("+configParams+")",
                "REPL LOAD `testDB` FROM '" +DUMP_DIRECTORY+"' WITH ("
                        + "'mapreduce.job.queuename'='default','hive.exec.parallel'='true',"
                        + "'hive.distcp.privileged.doAs'='" + user + "')");
    }

    @Test
    public void testReplStatus(){
        LOG.info("Executing Replication Status");
        String database = hiveJobDetails.getProperties().getProperty(HiveDRProperties.TARGET_DATASET.getName());
        ReplCommand replStatus = new ReplCommand(database);
        Assert.assertEquals(replStatus.getReplStatus(hiveJobDetails.getProperties()),
                "REPL STATUS `testDB1`");

        hiveJobDetails.getProperties().setProperty(Cluster.ClusterFields.CLOUDDATALAKE.getName(), "true");
        hiveJobDetails.getProperties().setProperty(HiveDRProperties
                .TARGET_HIVE_SERVER_AUTHENTICATION.getName(), "NONE");
        hiveJobDetails.getProperties().setProperty(Cluster.ClusterFields.HMSENDPOINT.getName(),
                "thrift://localhost:9083");
        hiveJobDetails.getProperties().setProperty(HiveDRProperties
                .TARGET_HMS_KERBEROS_PRINCIPAL.getName(), "hive/_HOST@EXAMPLE.COM");
        Assert.assertEquals(replStatus.getReplStatus(hiveJobDetails.getProperties()),
                "REPL STATUS `testDB1` WITH ('hive.metastore.uris'='thrift://localhost:9083')");

        hiveJobDetails.getProperties().setProperty(Cluster.ClusterFields.CLOUDDATALAKE.getName(), "true");
        hiveJobDetails.getProperties().setProperty(HiveDRProperties
                .TARGET_HIVE_SERVER_AUTHENTICATION.getName(), "KERBEROS");
        hiveJobDetails.getProperties().setProperty(Cluster.ClusterFields.HMSENDPOINT.getName(),
                "thrift://localhost:9083");
        hiveJobDetails.getProperties().setProperty(HiveDRProperties
                .TARGET_HMS_KERBEROS_PRINCIPAL.getName(), "hive/_HOST@EXAMPLE.COM");

        Assert.assertEquals(replStatus.getReplStatus(hiveJobDetails.getProperties()),
                "REPL STATUS `testDB1` WITH ('hive.metastore.uris'='thrift://localhost:9083',"
                        + "'hive.metastore.sasl.enabled'='true',"
                        + "'hive.metastore.kerberos.principal'='hive/_HOST@EXAMPLE.COM')");

        hiveJobDetails.getProperties().setProperty(HiveDRProperties
                .TARGET_HIVE_SERVER_AUTHENTICATION.getName(), "NOT_VALID");
        Assert.assertEquals(replStatus.getReplStatus(hiveJobDetails.getProperties()),
                "REPL STATUS `testDB1` WITH ('hive.metastore.uris'='thrift://localhost:9083')");


    }
}
