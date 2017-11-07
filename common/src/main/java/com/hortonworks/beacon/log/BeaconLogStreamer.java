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
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetch Beacon logs.
 */
class BeaconLogStreamer {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconLogStreamer.class);
    private static final int BUFFER_LEN = 4096;

    private String beaconLog;
    private BeaconLogFilter filter;

    BeaconLogStreamer(String beaconLog, BeaconLogFilter filter) {
        this.beaconLog = beaconLog;
        this.filter = filter;
    }

    void fetchLogs(Writer writer) throws BeaconException, IOException {
        LOG.debug("Fetch beacon logs for filter {}", filter.toString());
        try (BufferedReader reader = new BufferedReader(getReader(
                filter.getStartDate(), filter.getEndDate()))) {
            new TimeStampedMessageParser(reader, filter).processRemaining(writer, BUFFER_LEN);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    private MultiFileReader getReader(Date startTime, Date endTime) throws IOException {
        return new MultiFileReader(getFileList(startTime, endTime));
    }

    /**
     * File along with the modified time which will be used to sort later.
     */
    public static class FileInfo implements Comparable<FileInfo> {
        private File file;
        private long modTime;

        FileInfo(File file, long modTime) {
            this.file = file;
            this.modTime = modTime;
        }

        public File getFile() {
            return file;
        }

        @Override
        public int compareTo(FileInfo fileInfo) {
            long diff = this.modTime - fileInfo.modTime;
            return (diff > 0 ? 1 : (diff==0 ? 0 : -1));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileInfo fileInfo = (FileInfo) o;
            return modTime == fileInfo.modTime && file.equals(fileInfo.file);
        }

        @Override
        public int hashCode() {
            int result = file.hashCode();
            result = 31 * result + (int) (modTime ^ (modTime >>> 32));
            return result;
        }
    }

    private ArrayList<File> getFileList(Date startTime, Date endTime) throws IOException {
        File dir = new File(beaconLog);
        long startTimeinMillis = (startTime == null) ? 0 : startTime.getTime();
        long endTimeinMillis = (endTime == null) ? System.currentTimeMillis() : endTime.getTime();
        return getFileList(dir, startTimeinMillis, endTimeinMillis,
                BeaconLogHelper.BEACON_LOG_ROTATION_TIME, BeaconLogHelper.BEACON_LOG_PREFIX);
    }

    private ArrayList<File> getFileList(File dir, long startTime, long endTime,
                                        long logRotationTime, String logFile) {
        String[] children = dir.list();

        assert children != null;

        ArrayList<FileInfo> fileList = new ArrayList<>();
        for (String fileName : children) {
            if (!fileName.startsWith(logFile) && !fileName.equals(logFile)) {
                continue;
            }
            File file = new File(dir.getAbsolutePath(), fileName);
            long modTime = file.lastModified();
            if (modTime < startTime) {
                continue;
            }
            if (modTime / logRotationTime > (endTime / logRotationTime + 1)) {
                continue;
            }
            fileList.add(new FileInfo(file, modTime));
        }
        Collections.sort(fileList);
        ArrayList<File> files = new ArrayList<>(fileList.size());
        for (FileInfo info : fileList) {
            files.add(info.getFile());
        }
        return files;
    }
}
