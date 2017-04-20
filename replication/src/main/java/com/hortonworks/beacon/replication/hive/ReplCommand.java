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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Build Replication Command for Hive DR.
 */
public class ReplCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ReplCommand.class);

    private static final String REPL_DUMP = "REPL DUMP";
    private static final String REPL_LOAD = "REPL LOAD";
    private static final String REPL_STATUS = "REPL STATUS";
    private static final String NULL = "NULL";

    private String database;
    ReplCommand(String database) {
        this.database = database;
    }

    public String getReplDump(long fromEvent, long toEvent, int limit) {
        StringBuilder replDump = new StringBuilder();
        replDump.append(REPL_DUMP).append(' ').append(database);

        if (fromEvent > 0L) {
            replDump.append(" FROM ").append(fromEvent);
        }
        if (toEvent > 0L) {
            replDump.append(" TO ").append(toEvent);
        }
        if (fromEvent > 0L) {
            if (limit > 0) {
                replDump.append(" LIMIT ").append(limit);
            }
        }

        LOG.info("Repl Dump : {}", replDump.toString());
        return replDump.toString();
    }

    public String getReplLoad(String dumpDirectory) {
        StringBuilder replLoad = new StringBuilder();
        replLoad.append(REPL_LOAD).append(' ').append(database)
                .append(" FROM ").append("'"+dumpDirectory+"'");

        LOG.info("Repl Load : {}", replLoad.toString());
        return replLoad.toString();
    }

    protected String getReplStatus() {
        StringBuilder replStatus = new StringBuilder();
        replStatus.append(REPL_STATUS).append(' ').append(database);

        LOG.info("Repl Status : {}", replStatus.toString());
        return replStatus.toString();
    }

    protected long getReplicatedEventId(Statement statement) throws BeaconException {
        long eventReplId = -1L;
        String replStatus = getReplStatus();
        try (ResultSet res = statement.executeQuery(replStatus)) {
            if (res.next() && !(res.getString(1).equals(NULL))) {
                eventReplId = Long.parseLong(res.getString(1));
            }
        } catch (NumberFormatException | SQLException e) {
            LOG.error("Exception occurred while obtaining Repl event Id : {} "
                    + "for database : {}", e.getMessage(), database);
            throw new BeaconException(e.getMessage());
        }
        return eventReplId;
    }

}
