/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.metrics;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.util.HiveActionType;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to parse hive query log to obtain metrics.
 */
class ParseHiveQueryLog {
    private static final BeaconLog LOG = BeaconLog.getLog(ParseHiveQueryLog.class);

    private static final String ALLOW_ALL_REGEX = "(.*)";
    private static final String WHITE_SPACE_REGEX = "\\s+";
    private static final String LOG_LEVEL_REGEX = "(\\w+)";
    private static final String PREFIX_REGEX = LOG_LEVEL_REGEX + WHITE_SPACE_REGEX + ":" + WHITE_SPACE_REGEX;
    private static final Pattern SPLITTER_PATTERN = Pattern.compile(PREFIX_REGEX + ALLOW_ALL_REGEX);

    private boolean isDump = false;
    private boolean isLoad = false;
    private boolean isBootStrap = false;
    private boolean isIncremental = false;

    private long total;
    private long completed;

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

    private ParseHiveQueryLog(long total, long completed) {
        this.total = total;
        this.completed = completed;
    }

    private String splitReplLogMessage(String logLine) {
        Matcher splitter = SPLITTER_PATTERN.matcher(logLine);
        if (splitter.matches()){
            return splitter.group(2); // Log Message
        }
        return null;
    }

    long getTotal() {
        return total;
    }

    long getCompleted() {
        return completed;
    }

    ParseHiveQueryLog parseQueryLog(List<String> str, HiveActionType type) throws BeaconException {
        String replStmt = splitReplLogMessage(str.get(0));
        if (StringUtils.isNotBlank(replStmt) && replStmt.startsWith(QueryLogParam.REPL_START.getName())) {
            String jsonStr = replStmt.split(QueryLogParam.REPL_START.getName())[1];
            try {
                analyzeReplStart(jsonStr, type);
            } catch (JSONException e) {
                throw new BeaconException(MessageCode.REPL_000087.name(), e);
            }
        }

        for(int i=1; i<str.size(); i++) {
            String stmt = splitReplLogMessage(str.get(i));

            if (isDump && (isBootStrap || isIncremental)) {
                while ((StringUtils.isNotBlank(stmt)
                        && (stmt.startsWith(QueryLogParam.REPL_TABLE_DUMP.getName())
                        || stmt.startsWith(QueryLogParam.REPL_EVENT_DUMP.getName())))
                        && (i < str.size() && !stmt.contains(QueryLogParam.REPL_END.getName()))) {
                    total++;
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

        return new ParseHiveQueryLog(total, completed);
    }

    private void analyzeReplStart(String stmtJson, HiveActionType type) throws JSONException {
        JSONObject jsonObject = new JSONObject(stmtJson);
        if (type.equals(HiveActionType.EXPORT) && jsonObject.get(QueryLogParam.DUMPTYPE.getName())!=null) {
            isDump = true;
            if (jsonObject.get(QueryLogParam.DUMPTYPE.getName()).equals(QueryLogParam.BOOTSTRAP.getName())) {
                isBootStrap = true;
                int estimatedNumTables = (int) jsonObject.get(QueryLogParam.ESTIMATEDNUMTABLES.getName());
                LOG.debug(MessageCode.REPL_000088.name(), estimatedNumTables);
            } else if (jsonObject.get(QueryLogParam.DUMPTYPE.getName()).equals(QueryLogParam.INCREMENTAL.getName())) {
                isIncremental = true;
                int estimatedNumEvents = (int) jsonObject.get(QueryLogParam.ESTIMATEDNUMEVENTS.getName());
                LOG.debug(MessageCode.REPL_000089.name(), estimatedNumEvents);
            }
        } else if (type.equals(HiveActionType.IMPORT) && jsonObject.get(QueryLogParam.LOADTYPE.getName())!=null) {
            isLoad = true;
            if (jsonObject.get(QueryLogParam.LOADTYPE.getName()).equals(QueryLogParam.BOOTSTRAP.getName())) {
                isBootStrap = true;
                int numTables = (int) jsonObject.get(QueryLogParam.NUMTABLES.getName());
                LOG.debug(MessageCode.REPL_000090.name(), numTables);
            } else if (jsonObject.get(QueryLogParam.LOADTYPE.getName()).equals(QueryLogParam.INCREMENTAL.getName())) {
                isIncremental = true;
                int numEvents = (int) jsonObject.get(QueryLogParam.NUMEVENTS.getName());
                LOG.debug(MessageCode.REPL_000091.name(), numEvents);
            }
        }
    }
}
