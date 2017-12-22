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

    private List<File> getFileList(Date startTime, Date endTime) throws IOException {
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
    public List<File> getFileList(File[] files, Date logStartTime, Date logEndTime) {
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
                Date o1date = getDate(o1.getName());
                Date o2date = getDate(o2.getName());
                return o2date.compareTo(o1date);
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

    private Date getDate(String fileName) {
        String[] parts = fileName.split("\\.");
        if (!parts[0].startsWith(BEACON_LOG_PREFIX)) {
            return null;
        }

        String dateStr = null;
        if (parts.length == 2) {
            dateStr = dateFormat.format(new Date());
        } else if (parts.length > 2) {
            dateStr = parts[2];
        }

        try {
            return dateStr == null ? null : dateFormat.parse(dateStr);
        } catch (ParseException e) {
            return null;
        }
    }
}
