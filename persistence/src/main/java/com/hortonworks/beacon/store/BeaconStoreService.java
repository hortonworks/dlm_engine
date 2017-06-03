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

package com.hortonworks.beacon.store;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.DbStore;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.service.BeaconService;

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
        if (dbStore.isValidateDbConn()
                && !dbType.equalsIgnoreCase(HSQL_DB)
                && !dbType.equalsIgnoreCase(DERBY_DB)) {
            connProps += ",TestOnBorrow=true,TestOnReturn=true,TestWhileIdle=true";
            connProps += ",ValidationQuery=select count(*) from beacon_sys";
        }

        Properties props = new Properties();
        props.setProperty("openjpa.ConnectionProperties", connProps);
        props.setProperty("openjpa.ConnectionDriverName", dataSource);

        String unitName = "beacon-" + dbType;
        factory = Persistence.createEntityManagerFactory(unitName, props);
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
}
