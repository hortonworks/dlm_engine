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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;

/**
 * Fetch Beacon logs:
 * 1. Orders the log files in the log directory using decreasing order of timestamp on the log filename
 * 2. If n loglines are required, reads loglines matching the pattern
 *      2.1 Uses circular FIFO queue of size n
 *      2.2 Reads from start of file and adds to the queue, reads till end of file
 *      2.3 Hanldes log lines spanning multiple lines
 *      2.4 Reverses the queue so that latest logs are first
 *      2.5 Returns log lines in the queue, number of logs returned, m <= n
 * 3. The remaining logs (n-m) are read from next file and so on
 */
class BeaconLogStreamer {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconLogStreamer.class);

    public static final String DATE_FORMAT_STRING = "yyyy-MM-dd-HH";
    private SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    static final String BEACON_LOG_PREFIX = "beacon-application";
    public static final String ZIPFILE_EXTENSION = ".gz";

    private String beaconLog;
    private BeaconLogFilter filter;

    BeaconLogStreamer(String beaconLog, BeaconLogFilter filter) {
        this.beaconLog = beaconLog;
        this.filter = filter;
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    void fetchLogs(int numLogs, Writer writer) throws BeaconException {
        LOG.debug("Fetch beacon logs for filter {}", filter.toString());
        try {
            List<File> fileList = getFileList(filter.getStartDate(), filter.getEndDate());
            TimeStampedMessageParser messageParser = new TimeStampedMessageParser(filter);
            int numLogsToRead = numLogs;
            for (File file : fileList) {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                int logsRead = messageParser.readLogs(reader, writer, numLogsToRead);
                numLogsToRead -= logsRead;
                if (numLogsToRead == 0) {
                    return;
                }
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                throw new BeaconException(e);
            }
        }
    }

    private List<File> getFileList(Date startTime, Date endTime) throws BeaconException {
        File dir = new File(beaconLog);
        //default end time to now
        endTime = endTime == null ? new Date() : endTime;

        //default start time to endtime - 1 hour
        startTime = startTime == null ? DateUtils.addHours(endTime, -1) : startTime;

        //if starttime >= endtime, starttime = endtime - 1 hour
        if (startTime.compareTo(endTime) >= 0) {
            endTime = DateUtils.addHours(startTime, -1);
        }

        LOG.debug("Start time: {}, end time: {}", DateUtil.formatDate(startTime), DateUtil.formatDate(endTime));
        return getFileList(dir.listFiles(), startTime, endTime);
    }

    @VisibleForTesting
    public List<File> getFileList(File[] files, Date logStartTime, Date logEndTime) throws BeaconException {
        List<File> fileList = new ArrayList<>();
        if (files == null) {
            return fileList;
        }

        for (File file : files) {
            String fileName = file.getName();
            Date fileStart = getDate(fileName);
            if (fileStart == null) {
                continue;
            }

            Date fileEnd = DateUtils.addHours(fileStart, 1);
            if (between(logStartTime, logEndTime, fileStart) || between(logStartTime, logEndTime, fileEnd)) {
                fileList.add(file);
            }
        }

        Collections.sort(fileList, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                try {
                    Date o1date = getDate(o1.getName());
                    Date o2date = getDate(o2.getName());
                    return o2date.compareTo(o1date);
                } catch (BeaconException e) {
                    throw new IllegalStateException(e);
                }
            }
        });

        StringBuilder sb = new StringBuilder();
        for (File file : fileList) {
            sb = sb.append(file.getAbsolutePath()).append(", ");
        }
        LOG.debug("Files shortlisted: {}", sb);
        return fileList;
    }

    private boolean between(Date start, Date end, Date time) {
        if (start.after(time) || end.before(time)) {
            return false;
        }
        return true;
    }

    private Date getDate(String fileName) throws BeaconException {
        if (!fileName.startsWith(BEACON_LOG_PREFIX)) {
            return null;
        }

        String localFileName = StringUtils.removeEnd(fileName, ZIPFILE_EXTENSION);
        int index = localFileName.lastIndexOf('.');
        String dateStr = index != -1 ? localFileName.substring(index + 1) : null;
        if (dateStr !=  null) {
            try {
                return dateFormat.parse(dateStr);
            } catch (ParseException e) {
                //ignore
            }
        }
        String currentDateStr = dateFormat.format(new Date());
        try {
            return dateFormat.parse(currentDateStr);
        } catch (ParseException e) {
            throw new BeaconException(e);
        }
    }
}
