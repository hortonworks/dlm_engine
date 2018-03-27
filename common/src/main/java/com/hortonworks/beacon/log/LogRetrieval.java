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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;

/**
 * Helper class for Beacon logging.
 */
public final class LogRetrieval {
    private static final Logger LOG = LoggerFactory.getLogger(LogRetrieval.class);

    private static final String BEACON_LOG_HOME = System.getProperty("beacon.log.dir");
    static final long BEACON_LOG_ROTATION_TIME = 3600000L;  //1 hour

    public String getPolicyLogs(String filters, String startStr, String endStr,
                                       int frequency, int numLogs) throws BeaconException {
        Date endDate = getEndDate(endStr);
        Date startDate = getStartDate(startStr, endDate, frequency);

        StringWriter out = new StringWriter();
        BeaconLogFilter logFilter = new BeaconLogFilter(parseFilters(filters), startDate, endDate);
        logFilter.validateLogFilters();

        BeaconLogStreamer logStreamer = new BeaconLogStreamer(BEACON_LOG_HOME, logFilter);
        logStreamer.fetchLogs(numLogs, new PrintWriter(out));
        return out.toString();
    }

    private Date getEndDate(String endStr) {
        Date endDate;
        if (StringUtils.isEmpty(endStr)) {
            endDate = new Date();
        } else {
            endDate = DateUtil.parseDate(endStr);
        }
        return endDate;
    }

    private Date getStartDate(String startStr, Date endDate, int frequency) {
        Date startDate;
        if (StringUtils.isEmpty(startStr)) {
            long startMillis = endDate.getTime();
            startMillis -= BEACON_LOG_ROTATION_TIME * frequency;
            startDate = new Date(startMillis);
        } else {
            startDate = DateUtil.parseDate(startStr);
        }

        if (startDate != null && startDate.after(endDate)) {
            LOG.warn("Calculated start date {} crossed end date {}. setting it to end date - 1 hour", startDate,
                endDate);
            startDate = DateUtils.addHours(endDate, -1);
        }

        return startDate;
    }

    @VisibleForTesting
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
