/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.service;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.DbStore;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Configuration for Beacon Store.
 */
public final class BeaconStoreService implements BeaconService {

    public static final String SERVICE_NAME = BeaconStoreService.class.getName();
    private static final String HSQL_DB = "hsqldb";
    private static final String DERBY_DB = "derby";
    private static final String MYSQL_DB = "mysql";

    private EntityManagerFactory factory = null;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws BeaconException {
        BeaconConfig config =  BeaconConfig.getInstance();
        DbStore dbStore = config.getDbStore();

        String user = dbStore.getUser();
        String driver = dbStore.getDriver();
        String url = dbStore.getUrl();
        int maxConn = dbStore.getMaxConnections();
        String dbType = url.substring("jdbc:".length());
        dbType = dbType.substring(0, dbType.indexOf(":"));

        String dataSource = "org.apache.commons.dbcp.BasicDataSource";
        String connProps = "DriverClassName={0},Url={1},Username={2},Password={3},MaxActive={4}";
        connProps = MessageFormat.format(connProps, driver, url, user, dbStore.resolvePassword(), maxConn);
        // Check BUG-85932 and BUG-86505
        dbStore.setValidateDbConn(isNotDerbyAndHSQLDB(dbType));
        if (dbStore.isValidateDbConn()) {
            connProps += ",TestOnBorrow=true,TestOnReturn=true,TestWhileIdle=true";
            connProps += ",ValidationQuery=" + BeaconConstants.VALIDATION_QUERY;
        }

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
    public void destroy() throws BeaconException {
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
