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
