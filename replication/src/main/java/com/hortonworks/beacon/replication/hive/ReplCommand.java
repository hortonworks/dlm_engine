/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.exceptions.BeaconException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Build Replication Command for Hive DR.
 */
public class ReplCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ReplCommand.class);

    private static final String REPL_DUMP = "REPL DUMP";
    private static final String REPL_LOAD = "REPL LOAD";
    private static final String REPL_STATUS = "REPL STATUS";
    private static final String SHOW_TABLES = "SHOW TABLES";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS";
    private static final String SHOW_FUNCTIONS = "SHOW FUNCTIONS";
    private static final String DROP_FUNCTION = "DROP FUNCTION";
    private static final String DROP_DATABASE = "DROP DATABASE IF EXISTS";
    private static final String CASCADE = "CASCADE";
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

    List<String> dropTableList(Statement statement) throws BeaconException {
        List<String> dropTable = new ArrayList<>();
        try (ResultSet res = statement.executeQuery(SHOW_TABLES)) {
            while (res.next()) {
                String tableName = res.getString(1);
                dropTable.add(DROP_TABLE + ' ' + tableName);
            }
        } catch (SQLException e) {
            LOG.error("Exception occurred for drop table list : {} ", e.getMessage());
            throw new BeaconException(e.getMessage());
        }
        return dropTable;
    }

    List<String> dropFunctionList(Statement statement) throws BeaconException {
        List<String> dropFunction = new ArrayList<>();
        try (ResultSet res = statement.executeQuery(SHOW_FUNCTIONS)) {
            while(res.next()) {
                if (res.getString(1).startsWith(database+".")) {
                    dropFunction.add(DROP_FUNCTION + ' ' + res.getString(1));
                }
            }
        } catch (SQLException e) {
            LOG.error("Exception occurred for drop function list : {} ", e.getMessage());
            throw new BeaconException(e.getMessage());
        }
        return dropFunction;
    }

    String dropDatabaseQuery() {
        return DROP_DATABASE + ' ' + database + ' ' + CASCADE;
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
