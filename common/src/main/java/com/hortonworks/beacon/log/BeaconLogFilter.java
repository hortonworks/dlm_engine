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

import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Beacon Log Filter class.
 */
public class BeaconLogFilter {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconLogFilter.class);

    private static final String ALLOW_ALL_REGEX = "(.*)";
    private static final String TIMESTAMP_REGEX = "(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d,\\d\\d\\d)";
    private static final String WHITE_SPACE_REGEX = "\\s+";
    private static final String LOG_LEVEL_REGEX = "(\\w+)";
    private static final String PREFIX_REGEX = TIMESTAMP_REGEX + WHITE_SPACE_REGEX + LOG_LEVEL_REGEX
            + WHITE_SPACE_REGEX;
    private static final Pattern SPLITTER_PATTERN = Pattern.compile(PREFIX_REGEX + ALLOW_ALL_REGEX);

    private Pattern filterPattern;

    private Map<String, String> filterMap;
    private Date startDate;
    private Date endDate;
    private int numLogs;

    BeaconLogFilter(Map<String, String> filterMap, Date startDate, Date endDate, int numLogs) {
        this.filterMap = filterMap;
        this.startDate = startDate;
        this.endDate = endDate;
        this.numLogs = numLogs;
    }

    ArrayList<String> splitLogMessage(String logLine) {
        Matcher splitter = SPLITTER_PATTERN.matcher(logLine);
        if (splitter.matches()){
            ArrayList<String> logParts = new ArrayList<String>();
            logParts.add(splitter.group(1)); // timestamp
            logParts.add(splitter.group(2)); // log level
            logParts.add(splitter.group(3)); // Log Message
            return logParts;
        } else {
            return null;
        }
    }

    boolean matches(ArrayList<String> logParts) {
        if (getStartDate() != null) {
            if (logParts.get(0).substring(0, 19).compareTo(getFormatDate(getStartDate())) < 0) {
                return false;
            }
        }
        //String logLevel = logParts.get(1);
        String logMessage = logParts.get(2);
        Matcher logMatcher = filterPattern.matcher(logMessage);

        return logMatcher.matches();
    }

    Date getStartDate() {
        return startDate;
    }

    Date getEndDate() {
        return endDate;
    }

    int getNumLogs() {
        return numLogs;
    }

    private String getFormatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(date);
    }

    void constructFilterPattern() {
        StringBuilder sb = new StringBuilder();
        sb.append("(.* ");
        for (Map.Entry<String, String> kv : filterMap.entrySet()) {
            if (BeaconLogParams.checkParams(kv.getKey().toUpperCase())) {
                sb.append(kv.getKey()).append("\\[").append(filterMap.get(kv.getKey())).append("\\] ");
            }
        }
        sb.append(".*)");
        LOG.info("Filter Pattern constructed :{}", sb.toString());
        filterPattern = Pattern.compile(sb.toString());
    }

    void validateLogFilters() throws BeaconException {
        for(String filterKey : filterMap.keySet()) {
            if (!BeaconLogParams.checkParams(filterKey)) {
                throw new BeaconException("Particular filter key is not supported:" + filterKey);
            }
        }
    }

    @Override
    public String toString() {
        return "BeaconLogFilter : "
                + ", filterMap=" + filterMap
                + ", startDate=" + startDate
                + ", endDate=" + endDate
                + ", numLogs=" + numLogs;
    }
}
