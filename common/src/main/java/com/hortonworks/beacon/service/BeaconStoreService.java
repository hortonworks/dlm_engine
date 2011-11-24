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

    private EntityManagerFactory factory = null;

    @Override
    public void init() throws BeaconException {
        DbStore dbStore = BeaconConfig.getInstance().getDbStore();

        String user = dbStore.getUser();
        String driver = dbStore.getDriver();
        int maxConn = dbStore.getMaxConnections();

        String url = appendJDBCParameters(dbStore);
        String dataSource = "org.apache.commons.dbcp.BasicDataSource";
        String connProps = StringFormat.format("DriverClassName={},Url={},Username={},MaxActive={}"
                        + ",MaxIdle={},MinIdle={},MaxWait={}",
                driver, url, user, maxConn, dbStore.getMaxIdleConnections(), dbStore.getMinIdleConnections(),
                dbStore.getMaxWaitMSecs());

        dbStore.setValidateDbConn(isNotDerbyAndHSQLDB(dbStore.getDBType()));
        if (dbStore.isValidateDbConn()) {
            connProps += ",TestOnBorrow=true,TestOnReturn=true,TestWhileIdle=true";
            connProps += ",ValidationQuery=" + BeaconConstants.VALIDATION_QUERY;
        }

        LOG.info("Using connection properties {}", connProps);
        connProps += ",Password=" + dbStore.resolvePassword();
        Properties props = new Properties();
        props.setProperty("openjpa.ConnectionProperties", connProps);
        props.setProperty("openjpa.ConnectionDriverName", dataSource);

        String unitName = "beacon-" + dbStore.getDBType().name().toLowerCase();
        factory = Persistence.createEntityManagerFactory(unitName, props);
    }

    private String appendJDBCParameters(DbStore dbStore) {
        //mysql jdbc parameters -
        // https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-configuration-properties.html
        //postgresql jdbc parameters - https://jdbc.postgresql.org/documentation/head/connect.html

        String url = dbStore.getUrl();
        switch (dbStore.getDBType()) {
            case MYSQL:
                if (dbStore.getConnectTimeoutMSecs() > 0) {
                    url = appendUrlParameter(url, "connectTimeout", dbStore.getConnectTimeoutMSecs());
                }
                url = appendUrlParameter(url, "autoReconnect", "true");
                break;

            case POSTGRESQL:
                if (dbStore.getConnectTimeoutMSecs() > 0) {
                    url = appendUrlParameter(url, "connectTimeout", dbStore.getConnectTimeoutMSecs() / 1000L);
                }
                break;

            default:
                break;
        }
        return url;
    }

    private String appendUrlParameter(String url, String paramName, Object paramValue) {
        StringBuilder urlBuilder = new StringBuilder(url);
        urlBuilder = url.contains("?") ? urlBuilder.append("&") : urlBuilder.append("?");
        urlBuilder.append(paramName).append("=").append(paramValue);
        return urlBuilder.toString();
    }

    private boolean isNotDerbyAndHSQLDB(DbStore.DBType dbType) {
        return !(dbType == DbStore.DBType.HSQLDB || dbType == DbStore.DBType.DERBY);
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
