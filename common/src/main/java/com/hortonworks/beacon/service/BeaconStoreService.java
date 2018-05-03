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

package com.hortonworks.beacon.service;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.DbStore;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.StringFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Properties;

/**
 * Configuration for Beacon Store.
 */
public final class BeaconStoreService implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconStoreService.class);
    private static final String HSQL_DB = "hsqldb";
    private static final String DERBY_DB = "derby";
    private static final String MYSQL_DB = "mysql";
    private static final String POSTGRESQL_DB = "postgresql";

    private EntityManagerFactory factory = null;

    @Override
    public void init() throws BeaconException {
        DbStore dbStore = BeaconConfig.getInstance().getDbStore();

        String user = dbStore.getUser();
        String driver = dbStore.getDriver();
        String url = dbStore.getUrl();
        int maxConn = dbStore.getMaxConnections();
        String dbType = url.substring("jdbc:".length());
        dbType = dbType.substring(0, dbType.indexOf(":"));
        String dataSource = "org.apache.commons.dbcp.BasicDataSource";
        String connProps = StringFormat.format("DriverClassName={},Url={},Username={},MaxActive={}"
                        + ",MaxIdle={},MinIdle={},MaxWait={}",
                driver, url, user, maxConn, dbStore.getMaxIdleConnections(), dbStore.getMinIdleConnections(),
                dbStore.getMaxWaitMSecs());

        dbStore.setValidateDbConn(isNotDerbyAndHSQLDB(dbType));
        if (dbStore.isValidateDbConn()) {
            connProps += ",TestOnBorrow=true,TestOnReturn=true,TestWhileIdle=true";
            connProps += ",ValidationQuery=" + BeaconConstants.VALIDATION_QUERY;
        }

        String connectTimeoutStr = null;
        long conenctTimeoutVal = dbStore.getConnectTimeoutMSecs();

        if (conenctTimeoutVal > 0) {
            if (MYSQL_DB.equalsIgnoreCase(dbType)) {
                connectTimeoutStr = "connectTimeout=" + conenctTimeoutVal;
            } else if (POSTGRESQL_DB.equalsIgnoreCase(dbType)) {
                //convert in seconds
                conenctTimeoutVal = conenctTimeoutVal / 1000L;
                connectTimeoutStr = "connectTimeout=" + conenctTimeoutVal;
            }
        }
        if (connectTimeoutStr != null) {
            connProps += ("," + connectTimeoutStr);
        }

        LOG.info("Using connection properties {}", connProps);
        connProps += ",Password=" + dbStore.resolvePassword();
        Properties props = new Properties();
        props.setProperty("openjpa.ConnectionProperties", connProps);
        props.setProperty("openjpa.ConnectionDriverName", dataSource);

        String unitName = "beacon-" + dbType;
        factory = Persistence.createEntityManagerFactory(unitName, props);
    }

    private boolean isNotDerbyAndHSQLDB(String dbType) {
        return !dbType.equalsIgnoreCase(HSQL_DB) && !dbType.equalsIgnoreCase(DERBY_DB);
    }

    @Override
    public void destroy() {
        if (factory != null && factory.isOpen()) {
            factory.close();
        }
    }

    public EntityManager getEntityManager() {
        return factory.createEntityManager();
    }

    public void closeEntityManager(EntityManager entityManager) {
        if (entityManager != null && entityManager.isOpen()) {
            entityManager.close();
        }
    }
}
