/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.beacon.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;

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
        int numLogs = filter.getNumLogs();
        while (increment() && numLogs>0) {
            writer.write(lastMessage);
            bytesWritten += lastMessage.length();
            if (bytesWritten > bufferLen) {
                writer.flush();
                bytesWritten = 0;
            }
            numLogs--;
        }
        writer.flush();
    }
}
