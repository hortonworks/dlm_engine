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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    BeaconLogFilter() {
    }

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

        if (getEndDate() != null) {
            if (logParts.get(0).substring(0, 19).compareTo(getFormatDate(getEndDate())) > 0) {
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

    void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    void setFilterMap(Map<String, String> filterMap) {
        this.filterMap = filterMap;
    }

    int getNumLogs() {
        return numLogs;
    }

    String getFormatDate(Date date) {
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
        LOG.info("Filter Pattern constructed: {}", sb.toString());
        filterPattern = Pattern.compile(sb.toString());
    }

    void validateLogFilters() throws BeaconException {
        for(String filterKey : filterMap.keySet()) {
            if (!BeaconLogParams.checkParams(filterKey)) {
                throw new BeaconException("Particular filter key is not supported: {}", filterKey);
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
