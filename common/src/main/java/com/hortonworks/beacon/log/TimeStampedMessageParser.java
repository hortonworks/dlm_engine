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

import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Beacon Log Message Parser class.
 */
class TimeStampedMessageParser {

    private BufferedReader reader;
    private BeaconLogFilter filter;
    private String nextLine = null;
    private boolean empty = false;
    private String lastMessage = null;
    private boolean patternMatched = false;


    TimeStampedMessageParser(BufferedReader reader, BeaconLogFilter filter) {
        this.reader = reader;
        this.filter = filter;
        filter.constructFilterPattern();
    }

    // Causes the next message and timestamp to be parsed from the BufferedReader.
    private boolean increment() throws IOException {
        if (empty) {
            return false;
        }

        if (nextLine == null) {     // first time only
            nextLine = parseNextLine();
            if (nextLine == null) { // reader finished
                empty = true;
                return false;
            }
        }

        StringBuilder message = new StringBuilder();
        String nextTimestamp = null;
        while (nextTimestamp == null) {
            message.append(nextLine).append(System.lineSeparator());
            nextLine = parseNextLine();
            if (nextLine != null) {
                nextTimestamp = parseTimestamp(nextLine);
            } else {
                empty = true;
                nextTimestamp = "";
            }
        }

        lastMessage = message.toString();
        return true;
    }

    /**
     * Reads the next line from the Reader and checks if it matches the filter.
     * It can also handle multi-line messages (i.e. exception stack traces).
     * If it returns null, then there are no lines left in the Reader.
     */
    private String parseNextLine() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            ArrayList<String> logParts = filter.splitLogMessage(line);
            if (logParts != null) {
                patternMatched = filter.matches(logParts);
            }
            if (patternMatched) {
                return line;
            }
        }
        return line;
    }

    /**
     * Parses the timestamp out of the passed in line.  If there isn't one, it returns null.
     */
    private String parseTimestamp(String line) {
        String timestamp = null;
        ArrayList<String> logParts = filter.splitLogMessage(line);
        if (logParts != null) {
            timestamp = logParts.get(0);
        }

        return timestamp;
    }

    void processRemaining(Writer writer, int bufferLen) throws IOException {
        int bytesWritten = 0;
        CircularFifoBuffer buffer = new CircularFifoBuffer(filter.getNumLogs());
        while (increment()) {
            buffer.add(lastMessage);
        }
        if (buffer.size() > 0) {
            Iterator it = buffer.iterator();
            while (it.hasNext()) {
                String msg = (String) it.next();
                bytesWritten += msg.length();
                writer.write(msg);
                if (bytesWritten > bufferLen) {
                    writer.flush();
                    bytesWritten = 0;
                }
            }
        }
        writer.flush();
    }
}
