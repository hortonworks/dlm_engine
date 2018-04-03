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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to parse hive query log to obtain metrics.
 */
class ParseHiveQueryLog {
    private static final Logger LOG = LoggerFactory.getLogger(ParseHiveQueryLog.class);

    private static final String ALLOW_ALL_REGEX = "(.*)";
    private static final String WHITE_SPACE_REGEX = "\\s+";
    private static final String LOG_LEVEL_REGEX = "(\\w+)";
    private static final String PREFIX_REGEX = LOG_LEVEL_REGEX + WHITE_SPACE_REGEX + ":" + WHITE_SPACE_REGEX;
    private static final Pattern SPLITTER_PATTERN = Pattern.compile(PREFIX_REGEX + ALLOW_ALL_REGEX);

    private boolean isDump = false;
    private boolean isLoad = false;
    private boolean isBootStrap = false;
    private boolean isIncremental = false;

    private HiveProgress hiveProgress = new HiveProgress();

    enum QueryLogParam {
        REPL_START("REPL::START:"),
        REPL_END("REPL::END:"),
        REPL_TABLE_DUMP("REPL::TABLE_DUMP:"),
        REPL_EVENT_DUMP("REPL::EVENT_DUMP:"),
        REPL_TABLE_LOAD("REPL::TABLE_LOAD:"),
        REPL_EVENT_LOAD("REPL::EVENT_LOAD:"),
        BOOTSTRAP("BOOTSTRAP"),
        INCREMENTAL("INCREMENTAL"),
        DUMPTYPE("dumpType"),
        LOADTYPE("loadType"),
        ESTIMATEDNUMTABLES("estimatedNumTables"),
        NUMTABLES("numTables"),
        ESTIMATEDNUMEVENTS("estimatedNumEvents"),
        NUMEVENTS("numEvents");

        private String name;

        QueryLogParam(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    ParseHiveQueryLog() {
    }

    private ParseHiveQueryLog(HiveProgress hiveProgress) {
        this.hiveProgress = hiveProgress;
    }

    private String splitReplLogMessage(String logLine) {
        Matcher splitter = SPLITTER_PATTERN.matcher(logLine);
        if (splitter.matches()){
            return splitter.group(2); // Log Message
        }
        return null;
    }

    long getTotal() {
        return hiveProgress.getTotal();
    }

    long getCompleted() {
        return hiveProgress.getCompleted();
    }

    public void setHiveProgress(HiveProgress hiveProgress) {
        this.hiveProgress = hiveProgress;
    }

    void parseQueryLog(List<String> str, HiveActionType type) throws BeaconException {
        String replStmt = splitReplLogMessage(str.get(0));
        if (StringUtils.isNotBlank(replStmt) && replStmt.startsWith(QueryLogParam.REPL_START.getName())) {
            String jsonStr = replStmt.split(QueryLogParam.REPL_START.getName())[1];
            try {
                analyzeReplStart(jsonStr, type);
            } catch (JSONException e) {
                throw new BeaconException("Exception occurred while analyzing Repl start statement", e);
            }
        }

        long completed = 0;
        for(int i=1; i<str.size(); i++) {
            String stmt = splitReplLogMessage(str.get(i));
            if (isDump && (isBootStrap || isIncremental)) {
                while ((StringUtils.isNotBlank(stmt)
                        && (stmt.startsWith(QueryLogParam.REPL_TABLE_DUMP.getName())
                        || stmt.startsWith(QueryLogParam.REPL_EVENT_DUMP.getName())))
                        && (i < str.size() && !stmt.contains(QueryLogParam.REPL_END.getName()))) {
                    completed++;
                    i++;
                    stmt = splitReplLogMessage(str.get(i));
                }
            } else if (isLoad && (isBootStrap || isIncremental)) {
                while ((StringUtils.isNotBlank(stmt)
                        && (stmt.startsWith(QueryLogParam.REPL_TABLE_LOAD.getName())
                        || stmt.startsWith(QueryLogParam.REPL_EVENT_LOAD.getName())))
                        && (i < str.size() && !stmt.contains(QueryLogParam.REPL_END.getName()))) {
                    completed++;
                    i++;
                    stmt = splitReplLogMessage(str.get(i));
                }
            }
        }
        setHiveProgress(new HiveProgress(getTotal(), completed));
    }

    private void analyzeReplStart(String stmtJson, HiveActionType type) throws JSONException {
        JSONObject jsonObject = new JSONObject(stmtJson);
        if (type.equals(HiveActionType.EXPORT) && jsonObject.get(QueryLogParam.DUMPTYPE.getName())!=null) {
            isDump = true;
            if (jsonObject.get(QueryLogParam.DUMPTYPE.getName()).equals(QueryLogParam.BOOTSTRAP.getName())) {
                isBootStrap = true;
                int estimatedNumTables = (int) jsonObject.get(QueryLogParam.ESTIMATEDNUMTABLES.getName());
                LOG.debug("Bootstrap export and estimated number of tables: {}", estimatedNumTables);
                hiveProgress.setTotal(estimatedNumTables);
            } else if (jsonObject.get(QueryLogParam.DUMPTYPE.getName()).equals(QueryLogParam.INCREMENTAL.getName())) {
                isIncremental = true;
                int estimatedNumEvents = (int) jsonObject.get(QueryLogParam.ESTIMATEDNUMEVENTS.getName());
                LOG.debug("Incremental export and estimated number of events: {}", estimatedNumEvents);
                hiveProgress.setTotal(estimatedNumEvents);
            }
        } else if (type.equals(HiveActionType.IMPORT) && jsonObject.get(QueryLogParam.LOADTYPE.getName())!=null) {
            isLoad = true;
            if (jsonObject.get(QueryLogParam.LOADTYPE.getName()).equals(QueryLogParam.BOOTSTRAP.getName())) {
                isBootStrap = true;
                int numTables = (int) jsonObject.get(QueryLogParam.NUMTABLES.getName());
                LOG.debug("Bootstrap import and estimated number of tables: {}", numTables);
                hiveProgress.setTotal(numTables);
            } else if (jsonObject.get(QueryLogParam.LOADTYPE.getName()).equals(QueryLogParam.INCREMENTAL.getName())) {
                isIncremental = true;
                int numEvents = (int) jsonObject.get(QueryLogParam.NUMEVENTS.getName());
                LOG.debug("Incremental import and estimated number of events: {}", numEvents);
                hiveProgress.setTotal(numEvents);
            }
        }
    }
}
