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
package com.hortonworks.beacon.metrics;

import com.google.gson.Gson;
import com.hortonworks.beacon.HiveReplEventType;
import com.hortonworks.beacon.HiveReplType;
import com.hortonworks.beacon.entity.HiveDumpMetrics;
import com.hortonworks.beacon.entity.HiveLoadMetrics;
import com.hortonworks.beacon.util.HiveActionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to parseQueryLog hive query log to obtain metrics.
 */
public final class ParseHiveQueryLogV2 {

    private static final Logger LOG = LoggerFactory.getLogger(ParseHiveQueryLogV2.class);

    private static final String REPL_LOG_REGEX = "REPL::(.*): (.*)";
    private static final Pattern PATTERN = Pattern.compile(REPL_LOG_REGEX);
    private static final Gson GSON = new Gson();

    private static final String ALLOW_ALL_REGEX = "(.*)";
    private static final String WHITE_SPACE_REGEX = "\\s+";
    private static final String LOG_LEVEL_REGEX = "(\\w+)";
    private static final String PREFIX_REGEX = LOG_LEVEL_REGEX + WHITE_SPACE_REGEX + ":" + WHITE_SPACE_REGEX;
    private static final Pattern SPLITTER_PATTERN = Pattern.compile(PREFIX_REGEX + ALLOW_ALL_REGEX);

    private HiveProgress hiveProgress = new HiveProgress();

    long getTotal() {
        return hiveProgress.getTotal();
    }

    long getCompleted() {
        return hiveProgress.getCompleted();
    }

    private void setHiveProgress(long total, long completed) {
        hiveProgress.setTotal(total);
        hiveProgress.setCompleted(completed);
    }

    private static String splitReplLogMessage(String logLine) {
        Matcher splitter = SPLITTER_PATTERN.matcher(logLine);
        if (splitter.matches()){
            return splitter.group(2); // Log Message
        }
        return null;
    }

    void parseQueryLog(List<String> str, HiveActionType actionType) {
        long total = 0, completed = 0;
        List<String> queryLogList = getQueryLogsToProcess(str);

        for (String queryLog : queryLogList) {
            String log = splitReplLogMessage(queryLog);
            if (log == null) {
                continue;
            }
            Matcher matcher = PATTERN.matcher(log);
            if (matcher.find()) {
                LOG.info("Processing Hive repl log: {}", queryLog);
                String replEventType = matcher.group(1);
                HiveReplEventType hiveReplEventType = HiveReplEventType.getHiveReplEventType(replEventType);
                String metricInfo = matcher.group(2);
                HiveReplType replType;
                if (actionType == HiveActionType.EXPORT) {
                    HiveDumpMetrics dumpMetrics = GSON.fromJson(metricInfo, HiveDumpMetrics.class);
                    switch (hiveReplEventType) {
                        case START:
                            replType = HiveReplType.valueOf(dumpMetrics.getDumpType());
                            total = replType == HiveReplType.BOOTSTRAP ? dumpMetrics.getEstimatedNumTables()
                                    : dumpMetrics.getEstimatedNumEvents();
                            break;
                        case TABLE_DUMP:
                        case EVENT_DUMP:
                            total = dumpMetrics.getTotalDumpTable(hiveReplEventType);
                            completed = dumpMetrics.getCompletedDumpTable(hiveReplEventType);
                            break;
                        case END:
                            replType = HiveReplType.valueOf(dumpMetrics.getDumpType());
                            completed = replType == HiveReplType.BOOTSTRAP  ? dumpMetrics.getActualNumTables()
                                    : dumpMetrics.getActualNumEvents();
                            total = completed;
                            break;
                        default:
                            LOG.debug("Metrics event type {} won't be processed", hiveReplEventType);
                    }
                } else {
                    HiveLoadMetrics loadMetrics = GSON.fromJson(metricInfo, HiveLoadMetrics.class);
                    switch (hiveReplEventType) {
                        case START:
                            replType = HiveReplType.valueOf(loadMetrics.getLoadType());
                            total = replType == HiveReplType.BOOTSTRAP ? loadMetrics.getNumTables()
                                    : loadMetrics.getNumEvents();
                            break;
                        case TABLE_LOAD:
                        case EVENT_LOAD:
                            total = loadMetrics.getTotalLoadTable(hiveReplEventType);
                            completed = loadMetrics.getCompletedLoadTable(hiveReplEventType);
                            break;
                        case END:
                            replType = HiveReplType.valueOf(loadMetrics.getLoadType());
                            completed = replType == HiveReplType.BOOTSTRAP ? loadMetrics.getNumTables()
                                    : loadMetrics.getNumEvents();
                            break;
                        default:
                            LOG.debug("Metrics event type {} won't be processed", hiveReplEventType);
                    }
                }
            }
        }
        setHiveProgress(total, completed);
    }

    private List<String> getQueryLogsToProcess(List<String> str) {
        List<String> queryLogList = new ArrayList<>();
        boolean startLogAvailable = false;
        for (String queryLog : str) {
            if (queryLog.contains(HiveReplEventType.START.getName())) {
                queryLogList.add(queryLog);
                startLogAvailable = true;
            }
            if (queryLog.contains(HiveReplEventType.END.getName())) {
                queryLogList.add(queryLog);
            }
        }
        if ((startLogAvailable && str.size() > 1) || queryLogList.size() == 0) {
            queryLogList.add(str.get(str.size() - 1));
        }
        return queryLogList;
    }
}
