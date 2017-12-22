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
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

/**
 * Beacon Log Message Parser class.
 */
class TimeStampedMessageParser {
    private static final int BUFFER_LEN = 4096;

    private BeaconLogFilter filter;
    private String nextLine;


    TimeStampedMessageParser(BeaconLogFilter filter) {
        this.filter = filter;
        filter.constructFilterPattern();
    }

    /**
     * Reads the next line from the Reader and checks if it matches the filter.
     * It can also handle multi-line messages (i.e. exception stack traces).
     * If it returns null, then there are no lines left in the Reader.
     */
    private String parseNextLog(BufferedReader reader) throws IOException {
        StringBuilder logLine = new StringBuilder();
        boolean readFirstLog = false;
        while (true) {
            String line = nextLine != null ? nextLine : reader.readLine();
            if (line == null) {
                return readFirstLog ? logLine.toString() : null;
            }

            nextLine = null;
            //If the log line is splittable, its part of log4j log message
            ArrayList<String> logParts = filter.splitLogMessage(line);
            if (logParts != null) { //Its log4j logline and not part of another logline
                boolean filterMatched = filter.matches(logParts);
                if (readFirstLog) {
                    nextLine = line;
                    return logLine.toString();
                } else {
                    if (filterMatched) {
                        logLine.append(line).append("\n");
                        readFirstLog = true;
                    }
                }
            } else {
                if (readFirstLog) { //part of current logline
                    logLine = logLine.append(line).append("\n");
                }
            }
        }
    }

    /**
     * Reads the full file from start to end, stores in circular buffer of size n.
     * Sorts the logs in reverse order
     * @param writer
     * @param numLogsToRead
     * @return m, number of logs read <= numLogsToRead
     * @throws IOException
     */
    int readLogs(BufferedReader reader, Writer writer, int numLogsToRead) throws IOException {
        int bytesWritten = 0;
        String logLine;
        CircularFifoBuffer buffer = new CircularFifoBuffer(numLogsToRead);
        while ((logLine = parseNextLog(reader)) != null) {
            buffer.add(logLine);
        }

        if (buffer.size() > 0) {
            ArrayList bufferList = new ArrayList<>(buffer);
            Collections.reverse(bufferList);
            Iterator it = bufferList.iterator();
            while (it.hasNext()) {
                String msg = (String) it.next();
                bytesWritten += msg.length();
                writer.write(msg);
                if (bytesWritten > BUFFER_LEN) {
                    writer.flush();
                    bytesWritten = 0;
                }
            }
        }
        writer.flush();
        return buffer.size();
    }
}
