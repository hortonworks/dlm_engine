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
import org.apache.commons.lang.StringUtils;
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
    private static final String HOST_NAME = "localhost.me";
    private static final File BEACON_LOG_DIR = new File("target", BEACON_LOG_HOME);
    private LogRetrieval logRetrieval;
    private String[][] logMessages;

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
        logRetrieval = new LogRetrieval();
    }

    @Test
    public void testFetchLogs() throws BeaconException {
        String startStr = "2017-04-24T00:00:00";
        String endStr = DateUtil.getDateFormat().format(new Date());
        String filterBy = "policyname:fspolicy";
        String logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 2);
        assertNotNull(logString);
        assertTrue(logString.contains("POLICYNAME[fspolicy]"));
        List<String> logLines = getLogLines(logString);
        assertEquals(logLines.size(), 2);

        //assert on last n logs ordering
        assertEquals(logLines.get(0), logMessages[4][2]);
        assertEquals(logLines.get(1), logMessages[4][0]);

        //assert on multiple filter by keys
        filterBy = "policyname:fspolicy,policyid:id1";
        logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 2);
        assertNotNull(logString);
        assertTrue(logString.contains("POLICYNAME[fspolicy]"));
        assertTrue(logString.contains("POLICYID[id1]"));
        logLines = getLogLines(logString);
        assertEquals(logLines.size(), 2);
        assertEquals(logLines.get(0), logMessages[4][2]);
        assertEquals(logLines.get(1), logMessages[4][1]);
    }

    @Test
    public void testLogStartEndTime() throws BeaconException {
        String startStr = "2017-04-24T07:30:57";
        String endStr = "2017-04-24T08:54:20";
        String filterBy = "policyname:fspolicy";
        String logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 5);
        assertNotNull(logString);
        assertTrue(logString.contains("POLICYNAME[fspolicy]"));
        List<String> logLines = getLogLines(logString);

        //Even though 5 logs are requested, returns 1 log based on start time filtering
        assertEquals(logLines.size(), 1);
        assertEquals(logLines.get(0), logMessages[3][0]);

        //end time filtering
        startStr = "2017-04-24T00:00:00";
        endStr = "2017-04-24T08:52:20";
        logString = logRetrieval.getPolicyLogs(filterBy, startStr, endStr, 10, 4);
        assertNotNull(logString);
        assertTrue(logString.contains("POLICYNAME[fspolicy]"));
        logLines = getLogLines(logString);

        assertEquals(logLines.size(), 3);
        assertEquals(logLines.get(0), logMessages[2][0]);
        assertEquals(logLines.get(1), logMessages[1][0]);
        assertEquals(logLines.get(2), logMessages[0][0]);
    }

    private void generateBeaconLogData() throws Exception {
        LOG.info("Generating Beacon log Data for test");
        String[] fileDates = {"2017-04-24-05", "2017-04-24-06", "2017-04-24-07", "2017-04-24-08", null};
        BufferedWriter output;
        logMessages = getLogMessages();
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
                    output.write("\n");
                }
                output.close();
            }
        }
    }

    private String[][] getLogMessages() {
        return new String[][]
        {
            //file beacon-application.log.2017-04-24-05
            {"2017-04-24 05:36:28,339 DEBUG - [1445222369@qtp-1726780304-0:d181d6fc-e868-4695-a470-b8876b019624 "
                    + "POLICYNAME[fspolicy]] ~ Checking for HCFS path: /data/repl1 (FSUtils:50)", },
            //file beacon-application.log.2017-04-24-06
            {"2017-04-24 06:25:54,451 DEBUG - [1445222369@qtp-1726780304-0:d181d6fc-e868-4695-a470-b8876b019624 "
                    + "POLICYNAME[fspolicy]] ~ Creating Distributed FS for the login user beacon, impersonation not "
                    + "required (FileSystemClientFactory:197)", },
            //file beacon-application.log.2017-04-24-07
            {"2017-04-24 07:28:57,269 ERROR - [1445222369@qtp-1726780304-0:d181d6fc-e868-4695-a470-b8876b019624 "
                 + "POLICYNAME[fspolicy]] ~ Throwing web exception with status code: 400, message: Target dataset "
                 + "directory /data/repl1 is not empty. (BeaconWebException:60)\n"
                 + "com.hortonworks.beacon.api.exception.BeaconWebException: com.hortonworks.beacon.entity"
                 + ".exceptions.ValidationException: Target dataset directory /data/repl1 is not empty.\n"
                 + "at com.hortonworks.beacon.api.exception.BeaconWebException.newAPIException(BeaconWebException:56)\n"
                 + "at com.hortonworks.beacon.api.exception.BeaconWebException.newAPIException(BeaconWebException:36)\n"
                 + "at com.hortonworks.beacon.api.PolicyResource.submitAndSchedulePolicy(PolicyResource.java:473)\n"
                 + "at com.hortonworks.beacon.api.PolicyResource.submitAndSchedule(PolicyResource.java:97)\n"
                 + "at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n"
                 + "at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n"
                 + "at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)", },
            //file beacon-application.log.2017-04-24-08
            {"2017-04-24 08:53:20,070 DEBUG - [1445222369@qtp-1726780304-0:2825ac8a-31e8-4611-9516-32576c2d752b "
                 + "POLICYNAME[fspolicy]] ~ Checking for HCFS path: hdfs://ctr-e138-1518143905142-14738-01-000009.hwx."
                 + "site:8020/data/repl1 (FSUtils:50)", },
            //file beacon-application.log
            {"2017-04-24 09:53:20,072 DEBUG - [1445222369@qtp-1726780304-0:2825ac8a-31e8-4611-9516-32576c2d752b "
                 + "POLICYNAME[fspolicy]] ~ Validating if dir: hdfs://ctr-e138-1518143905142-14738-01-000009.hwx."
                 + "site:8020/data/repl1 is snapshotable. (FSSnapshotUtils:59)",
             "2017-04-24 09:54:57,268 DEBUG - [1445222369@qtp-1726780304-0:2825ac8a-31e8-4611-9516-32576c2d752b "
                 + "POLICYID[id1]] ~ Path to check: /data/repl1 (FSListing:57)",
             "2017-04-24 09:55:09,421 INFO  - [1445222369@qtp-1726780304-0:2825ac8a-31e8-4611-9516-32576c2d752b "
                 + "POLICYNAME[fspolicy]] ~ PolicyType FS is obtained for entity: fspolicy (ReplicationUtils:65)", },
        };
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
                    logLines.add(StringUtils.removeEnd(logTillNow.toString(), "\n"));
                    logTillNow = new StringBuilder();
                }
            }
            logTillNow.append(logLine).append("\n");
        }
        if (!logTillNow.toString().isEmpty()) {
            logLines.add(StringUtils.removeEnd(logTillNow.toString(), "\n"));
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
        String[] randomList = {"beacon-application-host.com.log.2017-04-11-08",
            "beacon-application-host.com.log.2017-04-11-10",
            "beacon-application-host.com.log.2017-04-12-08", "beacon-application-host.com.log.2017-05-11-08",
            "beacon-application-host.com.log.2016-04-11-08", "beacon-application-host.com.log.2017-04-11-07.gz",
            "beacon-application-host.com.log", };
        String[] orderedList = {"beacon-application-host.com.log", "beacon-application-host.com.log.2017-05-11-08",
            "beacon-application-host.com.log.2017-04-12-08", "beacon-application-host.com.log.2017-04-11-10",
            "beacon-application-host.com.log.2017-04-11-08", "beacon-application-host.com.log.2017-04-11-07.gz",
            "beacon-application-host.com.log.2016-04-11-08", };

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


