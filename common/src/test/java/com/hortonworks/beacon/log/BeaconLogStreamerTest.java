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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;

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
    public void setup() throws Exception {
        System.setProperty("beacon.log.dir", BEACON_LOG_DIR.getPath());
        if (BEACON_LOG_DIR.exists()) {
            LOG.info("Delete Beacon log {} directory", BEACON_LOG_DIR);
            FileUtils.deleteDirectory(BEACON_LOG_DIR);
        }
        if (!BEACON_LOG_DIR.mkdirs()) {
            throw new IOException("Directory creation failed: " +BEACON_LOG_DIR.toString());
        }
        generateBeaconLogData();
    }

    @Test(enabled = false)
    public void testFetchLogs() throws BeaconException {
        String startStr = "2017-04-24T00:00:00";
        String endStr = DateUtil.getDateFormat().format(new Date());
        String filterBy = "user:ambari-qa";
        String logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 2);
        assertNotNull(logString);
        assertTrue(logString.contains("USER[ambari-qa]"));
        List<String> logLines = getLogLines(logString);
        assertEquals(logLines.size(), 2);

        //assert on last n logs ordering
        String[][] logMessages = getLogMessages();
        assertEquals(logLines.get(0), logMessages[4][1]);
        assertEquals(logLines.get(1), logMessages[3][0]);
    }

    @Test(enabled = false)
    public void testLogStartEndTime() throws BeaconException {
        String startStr = "2017-04-24T07:30:57";
        String endStr = "2017-04-24T08:54:20";
        String filterBy = "user:ambari-qa";
        String logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 2);
        assertNotNull(logString);
        assertTrue(logString.contains("USER[ambari-qa]"));
        List<String> logLines = getLogLines(logString);

        //Even though 2 logs are requested, returns 1 log based on start time filtering
        assertEquals(logLines.size(), 1);
        String[][] logMessages = getLogMessages();
        assertEquals(logLines.get(0), logMessages[3][0]);

        //end time filtering
        startStr = "2017-04-24T00:00:00";
        endStr = "2017-04-24T08:52:20";
        logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 4);
        assertNotNull(logString);
        assertTrue(logString.contains("USER[ambari-qa]"));
        logLines = getLogLines(logString);

        assertEquals(logLines.size(), 2);
        assertEquals(logLines.get(0), logMessages[2][0]);
        assertEquals(logLines.get(1), logMessages[0][0]);
    }

    private void generateBeaconLogData() throws Exception {
        LOG.info("Generating Beacon log Data for test");
        String[] fileDates = {"2017-04-24-05", "2017-04-24-06", "2017-04-24-07", "2017-04-24-08", null};
        BufferedWriter output;
        String[][] logMessages = getLogMessages();
        int i = 0;
        for (String fileDate : fileDates) {
            File file = new File(BEACON_LOG_DIR +File.separator+BeaconLogStreamer.BEACON_LOG_PREFIX
                    +"-"+ HOST_NAME +".log"
                    +(fileDate == null ? "" : "." + fileDate));

            String[] lines = logMessages[i++];
            if (file.exists()) {
                if (!file.delete()) {
                    LOG.info("File : {} deletion did not happen", file);
                }
            }
            if (file.createNewFile()) {
                output = new BufferedWriter(new FileWriter(file));
                for (String line : lines) {
                    output.write(line);
                }
                output.close();
            }
        }
    }

    private String[][] getLogMessages() {
        return new String[][] {
                //file beacon-application.log.2017-04-24-05
                {"2017-04-24 05:36:28,339 ERROR - [Thread-0:[ USER[ambari-qa] CLUSTER[sourceCluster] "
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
                        + "DelegatingBrokerFactory.java:195)\n",
                    "2017-04-24 05:46:18,148 INFO  - [main:] ~ JobStoreTX initialized. (JobStoreTX:59)\n", },
                //file beacon-application.log.2017-04-24-06
                {"2017-04-24 06:25:54,451 INFO  - [Thread-0:] ~ calling shutdown hook (Main:96)\n", },
                //file beacon-application.log.2017-04-24-07
                {"2017-04-24 07:28:57,269 INFO  - [main:] ~ USER[ambari-qa] CLUSTER[sourceCluster] Quartz "
                        + "scheduler 'QuartzScheduler' initialized from an externally provided properties instance. "
                        + "(StdSchedulerFactory:1327)\n", },
                //file beacon-application.log.2017-04-24-08
                {"2017-04-24 08:53:20,070 INFO  - [main:] ~ main-1 USER[ambari-qa] CLUSTER[sourceCluster] "
                        + "Executing Replication Dump (ReplCommandTest:181)\n", },
                //file beacon-application.log
                {"2017-04-24 09:53:20,072 INFO  - [main:] ~ main-1 USER[hive] CLUSTER[sourceCluster] "
                        + "Repl Load : REPL LOAD testDB FROM 'hdfs://localhost:54136/tmp/hive/repl/121' "
                        + "(ReplCommand:181)\n",
                    "2017-04-24 09:54:57,268 INFO  - [main:] ~ USER[ambari-qa] Scheduler meta-data: Quartz "
                        + "Scheduler (v2.2.3) 'QuartzScheduler' with instanceId 'beaconScheduler'\n"
                        + "  Scheduler class: 'org.quartz.core.QuartzScheduler' - running locally.\n"
                        + "  NOT STARTED.\n"
                        + "  Currently in standby mode.\n"
                        + "  Number of jobs executed: 0\n"
                        + "  Using thread pool 'org.quartz.simpl.SimpleThreadPool' - with 10 threads.\n"
                        + "  Using job-store 'org.quartz.impl.jdbcjobstore.JobStoreTX' - which supports persistence. "
                        + "and is not clustered.\n"
                        + " (QuartzScheduler:305)\n",
                    "2017-04-24 09:55:09,421 INFO  - [main:] ~ Connected to Apache Derby version 10.10 using "
                        + "JDBC driver Apache                         Derby Embedded JDBC Driver version 10.10.1.1 "
                        + "- (1458268).  (JDBC:81)\n", }, };
    }

    private List<String> getLogLines(String logString) {
        String[] logSplit = logString.split("\n");
        BeaconLogFilter bFilter = new BeaconLogFilter();
        List<String> logLines = new ArrayList<>();
        StringBuilder logTillNow = new StringBuilder();

        for (String logLine : logSplit) {
            ArrayList<String> logParts = bFilter.splitLogMessage(logLine);
            if (logParts != null && logParts.get(0) != null) {
                if (!logTillNow.toString().isEmpty()) {
                    logLines.add(logTillNow.toString());
                    logTillNow = new StringBuilder();
                }
            }
            logTillNow.append(logLine).append("\n");
        }
        if (!logTillNow.toString().isEmpty()) {
            logLines.add(logTillNow.toString());
        }
        return logLines;
    }

    @AfterClass
    public void tearDown() {
        if (!BEACON_LOG_DIR.delete()) {
            LOG.info("Deleted : {} directory:", BEACON_LOG_DIR);
        }
    }

    @Test
    public void testGetFiles() throws Exception {
        String[] randomList = {"beacon-application.log.2017-04-11-08", "beacon-application.log.2017-04-11-10",
            "beacon-application.log.2017-04-12-08", "beacon-application.log.2017-05-11-08",
            "beacon-application.log.2016-04-11-08", "beacon-application.log.2017-04-11-07.gz",
            "beacon-application.log", };
        String[] orderedList = {"beacon-application.log", "beacon-application.log.2017-05-11-08",
            "beacon-application.log.2017-04-12-08", "beacon-application.log.2017-04-11-10",
            "beacon-application.log.2017-04-11-08", "beacon-application.log.2017-04-11-07.gz",
            "beacon-application.log.2016-04-11-08", };

        File[] files = new File[randomList.length];
        int i = 0;
        for (String fileName: randomList) {
            files[i++] = new File("/var/log/beacon/" + fileName);
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        BeaconLogStreamer logStreamer = new BeaconLogStreamer(null, null);

        List<File> resultFiles = logStreamer.getFileList(files, dateFormat.parse("2016-04-11 08:01"), new Date());
        assertEquals(resultFiles.size(), orderedList.length);

        //assert on sorting in decreased timestamp
        i = 0;
        for (File file : resultFiles) {
            assertEquals(file.getName(), orderedList[i++]);
        }

        resultFiles = logStreamer.getFileList(files, dateFormat.parse("2017-05-11 08:01"), new Date());
        assertEquals(resultFiles.size(), 2);

        resultFiles = logStreamer.getFileList(files, DateUtils.addHours(new Date(), -1), new Date());
        assertEquals(resultFiles.size(), 1);

        resultFiles = logStreamer.getFileList(files, dateFormat.parse("2017-05-11 09:01"),
                dateFormat.parse("2017-05-11 10:01"));
        assertEquals(resultFiles.size(), 0);

    }
}


