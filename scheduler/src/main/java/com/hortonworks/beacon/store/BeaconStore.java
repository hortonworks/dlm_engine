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

import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Properties;

public class BeaconStore {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconStore.class);
    private static EntityManagerFactory emf = null;

    public static void init() {
        URL resource = Resources.getResource("store.properties");
        InputSupplier<InputStream> inputSupplier = Resources.newInputStreamSupplier(resource);
        Properties properties = new Properties();
        try {
            properties.load(inputSupplier.getInput());
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        String driver = properties.getProperty("beacon.store.driver");
        String url = properties.getProperty("beacon.store.url");
        String user = properties.getProperty("beacon.store.username");
        String password = properties.getProperty("beacon.store.password");
        String maxConn = properties.getProperty("beacon.store.maxConnection");
        String dataSource = "org.apache.commons.dbcp.BasicDataSource";

        String connProps = "DriverClassName={0},Url={1},Username={2},Password={3},MaxActive={4}";
        connProps = MessageFormat.format(connProps, driver, url, user, password, maxConn);
        connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";

        Properties props = new Properties();
        props.setProperty("openjpa.ConnectionProperties", connProps);
        props.setProperty("openjpa.ConnectionDriverName", dataSource);

        String dbType = url.substring("jdbc:".length());
        dbType = dbType.substring(0, dbType.indexOf(":"));
        String unitName = "beacon-" + dbType;
        emf = Persistence.createEntityManagerFactory(unitName, props);
    }

    public static EntityManager getEntityManager() {
        if (emf == null) {
            init();
        }
        return emf.createEntityManager();
    }
}
