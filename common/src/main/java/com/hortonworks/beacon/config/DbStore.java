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

package com.hortonworks.beacon.config;


import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.CredentialProviderHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration parameters for Beacon store.
 */
public class DbStore {

    private String driver;
    private String url;
    private String user;
    private String password;
    private String passwordAlias;
    private int maxConnections;
    private String schemaDirectory;

    public void copy(DbStore o) {
        setDriver(o.getDriver());
        setUrl(o.getUrl());
        setUser(o.getUser());
        setPassword(o.getPassword());
        setPasswordAlias(o.getPasswordAlias());
        setMaxConnections(o.getMaxConnections());
        setSchemaDirectory(o.getSchemaDirectory());
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPasswordAlias() {
        return passwordAlias;
    }

    public void setPasswordAlias(String passwordAlias) {
        this.passwordAlias = passwordAlias;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public String getSchemaDirectory() {
        return schemaDirectory;
    }

    public void setSchemaDirectory(String schemaDirectory) {
        this.schemaDirectory = schemaDirectory;
    }

    public String resolvePassword() throws BeaconException {
        String dbPassword;
        if (StringUtils.isNotBlank(passwordAlias)) {
            Configuration conf = new Configuration();
            dbPassword = CredentialProviderHelper.resolveAlias(conf, passwordAlias);
        } else {
            dbPassword = getPassword();
        }
        return dbPassword;
    }
}
