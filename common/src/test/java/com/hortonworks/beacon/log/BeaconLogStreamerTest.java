/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.log;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

/**
 * Test class for Beacon logs.
 */
public class BeaconLogStreamerTest{
    private static final Logger LOG = LoggerFactory.getLogger(BeaconLogStreamerTest.class);
    private static final String BEACON_LOG_HOME = "samplelogs";
    private static final String HOST_NAME = "localhost";
    private static final File BEACON_LOG_DIR = new File("target", BEACON_LOG_HOME);
    private LogRetrieval logRetrieval = new LogRetrieval();

    @BeforeClass
    public void setup() throws IOException, BeaconException {
        System.setProperty("beacon.log.dir", BEACON_LOG_DIR.getPath());
        if (BEACON_LOG_DIR.exists()) {
            LOG.info("Delete Beacon log {} directory", BEACON_LOG_DIR);
            FileUtils.deleteDirectory(BEACON_LOG_DIR);
        }
        if (!BEACON_LOG_DIR.mkdirs()) {
            throw new IOException("Directory creation failed: " +BEACON_LOG_DIR.toString());
        }
    }

    @Test(enabled = false)
    public void testFetchLogs() throws BeaconException {
        generateBeaconLogData();
        String startStr = "2017-04-24T00:00:00";
        String endStr = DateUtil.getDateFormat().format(new Date());
        String filterBy = "user:ambari-qa";
        String logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 2);
        Assert.assertNotNull(logString);
        Assert.assertTrue(logString.contains("USER[ambari-qa]"));
        Assert.assertEquals(countLogStringLines(logString), 2);
    }

    @Test(enabled = false)
    public void testFetchLogsTailing() throws BeaconException {
        generateBeaconLogData();
        String startStr = "2017-04-24T00:00:00";
        String endStr = DateUtil.getDateFormat().format(new Date());
        String filterBy = "user:ambari-qa";
        String logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 1);
        Assert.assertNotNull(logString);
        Assert.assertTrue(logString.contains("USER[ambari-qa]"));
        String[] logMessages = getLogMessages();
        Assert.assertEquals(logMessages[logMessages.length - 2], logString);
        Assert.assertEquals(countLogStringLines(logString), 1);
    }

    @Test(enabled = false)
    public void testLogStartEndTime() throws BeaconException {
        String startStr = DateUtil.getDateFormat().format(new Date().getTime()-6000);
        String endStr = DateUtil.getDateFormat().format(new Date().getTime()-3000);
        String filterBy = "user:ambari-qa";
        BeaconLogFilter logFilter = new BeaconLogFilter();
        String log = logFilter.getFormatDate(new Date())+ ",300 INFO  - [main:[main-1 USER[ambari-qa]"
                + " CLUSTER[sourceCluster]] ~  Executing Replication Dump (ReplCommandTest:181)";

        logFilter.setStartDate(DateUtil.parseDate(startStr));
        logFilter.setEndDate(DateUtil.parseDate(endStr));
        logFilter.setFilterMap(logRetrieval.parseFilters(filterBy));
        logFilter.constructFilterPattern();
        logFilter.splitLogMessage(log);

        Assert.assertFalse(logFilter.matches(logFilter.splitLogMessage(log)));
        endStr = DateUtil.getDateFormat().format(new Date().getTime()+3000);
        logFilter.setEndDate(DateUtil.parseDate(endStr));
        Assert.assertTrue(logFilter.matches(logFilter.splitLogMessage(log)));
    }

    private void generateBeaconLogData() throws BeaconException {
        LOG.info("Generating Beacon log Data for test");
        String[] fileDates = {"2017-04-24-05", "2017-04-24-06", "2017-04-24-07", "2017-04-24-08"};
        BufferedWriter output;
        String []logMessages = getLogMessages();
        int i = 0;
        for (String fileDate : fileDates) {
            File file = new File(BEACON_LOG_DIR +File.separator+ LogRetrieval.BEACON_LOG_PREFIX
                    +"-"+ HOST_NAME +".log."+fileDate);

            String logMsg = logMessages[i++];
            if (file.exists()) {
                if (!file.delete()) {
                    LOG.info("File : {} deletion did not happen", file);
                }
            }
            try {
                if (file.createNewFile()) {
                    output = new BufferedWriter(new FileWriter(file));
                    output.write(logMsg);
                    output.close();
                }
            } catch (IOException e) {
                String errMsg = "Exception occurred while generating log data : " +e.getMessage();
                LOG.error(errMsg);
                throw new BeaconException(errMsg);
            }
        }
    }

    private String[] getLogMessages() {

        return new String[] {
            "2017-04-24 05:36:28,339 ERROR - [Thread-0:[USER[ambari-qa] CLUSTER[sourceCluster] "
                        + "POLICYNAME[fsRepl] POLICYID[/DC/sourceCluster/fsRepl/001] "
                        + "INSTANCEID[/DC/sourceCluster/fsRepl/001@1] Failed to destroy]] ~ "
                        + "service com.hortonworks.beacon.service.BeaconStoreService (ServiceManager:103)\n "
                        + "<openjpa-2.4.1-r422266:1730418 nonfatal user error> "
                        + "org.apache.openjpa.persistence.InvalidStateException: This operation failed for some "
                        + "instances.  See the nested exceptions array for details.\n"
                        + "    at org.apache.openjpa.kernel.AbstractBrokerFactory.assertNoActiveTransaction"
                        + "(AbstractBrokerFactory.java:713)\n"
                        + " at org.apache.openjpa.kernel.AbstractBrokerFactory.close("
                        + "AbstractBrokerFactory.java:377)\n"
                        + " at org.apache.openjpa.kernel.DelegatingBrokerFactory.close("
                        + "DelegatingBrokerFactory.java:195)\n"
                        + "2017-04-24 09:26:18,148 INFO  - [main:] ~ JobStoreTX initialized. (JobStoreTX:59)",
            "2017-04-24 06:25:54,451 INFO  - [Thread-0:] ~ calling shutdown hook (Main:96)\n",
            "2017-04-24 07:28:57,269 INFO  - [main:] ~ USER[ambari-qa] CLUSTER[sourceCluster] Quartz "
                        + "scheduler 'QuartzScheduler' initialized from an externally provided properties instance. "
                        + "(StdSchedulerFactory:1327)\n",
            "2017-04-24 08:53:20,070 INFO  - [main:] ~ main-1 USER[ambari-qa] CLUSTER[sourceCluster] "
                        + "Executing Replication Dump (ReplCommandTest:181)\n",
            "2017-04-24 09:53:20,072 INFO  - [main:] ~ main-1 USER[hive] CLUSTER[sourceCluster] "
                        + "Repl Load : REPL LOAD testDB FROM 'hdfs://localhost:54136/tmp/hive/repl/121' "
                        + "(ReplCommand:181)\n"
                        + "2017-04-24 09:28:57,268 INFO  - [main:] ~ USER[ambari-qa] Scheduler meta-data: Quartz "
                        + "Scheduler (v2.2.3) 'QuartzScheduler' with instanceId 'beaconScheduler'\n"
                        + "  Scheduler class: 'org.quartz.core.QuartzScheduler' - running locally.\n"
                        + "  NOT STARTED.\n"
                        + "  Currently in standby mode.\n"
                        + "  Number of jobs executed: 0\n"
                        + "  Using thread pool 'org.quartz.simpl.SimpleThreadPool' - with 10 threads.\n"
                        + "  Using job-store 'org.quartz.impl.jdbcjobstore.JobStoreTX' - which supports persistence. "
                        + "and is not clustered.\n"
                        + " (QuartzScheduler:305)\n"
                        + "2017-04-24 09:27:09,421 INFO  - [main:] ~ Connected to Apache Derby version 10.10 using "
                        + "JDBC driver Apache                         Derby Embedded JDBC Driver version 10.10.1.1 "
                        + "- (1458268).  (JDBC:81)\n",
        };
    }

    private int countLogStringLines(String logString) {
        String []logSplit = logString.split("\n");
        int count=0;
        BeaconLogFilter bFilter = new BeaconLogFilter();
        for (String logLine : logSplit) {
            ArrayList<String> logParts = bFilter.splitLogMessage(logLine);
            if (logParts != null) {
                if (logParts.get(0) != null) {
                    count++;
                }
            }
        }
        return count;
    }

    @AfterClass
    public void tearDown() {
        if (!BEACON_LOG_DIR.delete()) {
            LOG.info("Deleted : {} directory:", BEACON_LOG_DIR);
        }
    }
}


