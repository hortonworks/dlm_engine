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

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for Beacon logging.
 */
public final class BeaconLogHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconLogHelper.class);

    private static final String BEACON_LOG_HOME = System.getProperty("beacon.log.dir");
    static final String BEACON_LOG_PREFIX = "beacon-application";
    static final long BEACON_LOG_ROTATION_TIME = 3600000L;

    private BeaconLogHelper(){
    }

    public static String getPolicyLogs(String filters, String startStr, String endStr,
                                       int frequency, int numLogs) throws BeaconException {
        Date endDate = getEndDate(endStr);
        Date startDate = getStartDate(startStr, endDate, frequency);
        String logString;
        try(StringWriter out = new StringWriter()){
            BeaconLogFilter logFilter = new BeaconLogFilter(parseFilters(filters), startDate, endDate, numLogs);
            logFilter.validateLogFilters();
            BeaconLogStreamer logStreamer = new BeaconLogStreamer(BEACON_LOG_HOME, logFilter);
            logStreamer.fetchLogs(new PrintWriter(out));
            logString = out.toString();
        } catch (BeaconException e) {
            throw new BeaconException("Exception occurred in filter validation: ", e);
        } catch (IOException e) {
            throw new BeaconException("Exception occurred in filter validation: ", e);
        }
        return logString;
    }

    private static Date getEndDate(String endStr) {
        Date endDate;
        if (StringUtils.isEmpty(endStr)) {
            endDate = new Date();
        } else {
            endDate = DateUtil.parseDate(endStr);
        }
        return endDate;
    }

    private static Date getStartDate(String startStr, Date endDate, int frequency) {
        Date startDate;
        if (StringUtils.isEmpty(startStr)) {
            long startMillis = endDate.getTime();
            startMillis -= BEACON_LOG_ROTATION_TIME*frequency;
            startDate = new Date(startMillis);
        } else {
            startDate = DateUtil.parseDate(startStr);
        }

        if (startDate!=null && startDate.after(endDate)) {
            LOG.warn("Calculated start date: {} crossed end date: {} setting it to entity start date", startDate,
                endDate);
            startDate = endDate;
        }

        return startDate;
    }

    static Map<String, String> parseFilters(String filters) {
        Map<String, String> filterMap = new HashMap<>();
        String[] filterArray = filters.split(BeaconConstants.COMMA_SEPARATOR);
        if (filterArray.length > 0) {
            for (String pair : filterArray) {
                String[] keyValue = pair.split(BeaconConstants.COLON_SEPARATOR, 2);
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Invalid filter key:value pair provided: " + pair);
                }
                filterMap.put(keyValue[0].toUpperCase(), keyValue[1]);
            }
        } else {
            throw new IllegalArgumentException("Invalid filters provided: " + filters);
        }
        return filterMap;
    }
}
