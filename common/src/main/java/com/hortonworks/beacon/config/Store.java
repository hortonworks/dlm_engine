/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Store {
    Logger LOG = LoggerFactory.getLogger(Store.class);

    private String driver;
    private String url;
    private String user;
    private String password;
    private int maxConnections;

    public Store() {
        setDriver("org.hsqldb.jdbcDriver");
        setUrl("jdbc:hsqldb:mem:quartz");
        setUser("quartz");
        setPassword("quartz");
        setMaxConnections(4);
    }
    public void copy(Store o) {
        setDriver(o.getDriver());
        setUrl(o.getUrl());
        setUser(o.getUser());
        setPassword(o.getPassword());
        setMaxConnections(o.getMaxConnections());
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

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }
}
