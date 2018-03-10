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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.exceptions.BeaconException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
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

    protected String getReplStatus(Properties properties) {
        StringBuilder replStatus = new StringBuilder();
        replStatus.append(REPL_STATUS).append(' ').append(database);
        boolean isDataLake = Boolean.valueOf(properties.getProperty(Cluster.ClusterFields.CLOUDDATALAKE.getName()));
        StringBuilder configParams = new StringBuilder();
        if (isDataLake) {
            HiveDRUtils.appendConfig(properties, configParams, Cluster.ClusterFields.HMSENDPOINT.getName());
            if (properties.containsKey(HiveDRProperties.TARGET_HMS_KERBEROS_PRINCIPAL.getName())) {
                HiveDRUtils.appendConfig(configParams, HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.
                        toString(), "true");
                HiveDRUtils.appendConfig(properties, configParams,
                        HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.toString(),
                        properties.getProperty(HiveDRProperties.TARGET_HMS_KERBEROS_PRINCIPAL.getName()));

            }
            String params = configParams.substring(0, configParams.toString().length() - 1);
            replStatus.append(" WITH (").append(params).append(")");
        }
        LOG.info("Repl Status : {}", replStatus.toString());
        return replStatus.toString();
    }

    protected long getReplicatedEventId(Statement statement, Properties properties) throws BeaconException {
        long eventReplId = -1L;
        String replStatus = getReplStatus(properties);
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
