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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

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
        this.database = PolicyHelper.escapeDataSet(database);
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
        String targetDatabase = properties.getProperty(HiveDRProperties.TARGET_DATASET.getName());
        targetDatabase = PolicyHelper.escapeDataSet(targetDatabase);
        replStatus.append(REPL_STATUS).append(' ').append(targetDatabase);
        boolean isDataLake = Boolean.valueOf(properties.getProperty(Cluster.ClusterFields.CLOUDDATALAKE.getName()));
        StringBuilder configParams = new StringBuilder();
        if (isDataLake) {
            HiveDRUtils.appendConfig(properties, configParams, Cluster.ClusterFields.HMSENDPOINT.getName());
            HiveDRUtils.setHMSKerberosProperties(configParams, properties);
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
