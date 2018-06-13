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

package com.hortonworks.beacon.config;


import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.BeaconCredentialProvider;
import org.apache.commons.lang3.StringUtils;

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
    private int maxIdleConnections;
    private int minIdleConnections;
    private long maxWaitMSecs;
    private long connectTimeoutMSecs;
    private String schemaDirectory;
    private boolean validateDbConn;

    /**
     * Enum for db type.
     */
    public enum DBType {
        MYSQL, ORACLE, POSTGRESQL, DERBY, HSQLDB
    }

    public void copy(DbStore o) {
        setDriver(o.getDriver());
        setUrl(o.getUrl());
        setUser(o.getUser());
        setPassword(o.getPassword());
        setPasswordAlias(o.getPasswordAlias());
        setMaxConnections(o.getMaxConnections());
        setMaxIdleConnections(o.getMaxIdleConnections());
        setConnectTimeoutMSecs(o.connectTimeoutMSecs);
        setMaxWaitMSecs(o.maxWaitMSecs);
        setMinIdleConnections(o.getMinIdleConnections());
        setSchemaDirectory(o.getSchemaDirectory());
        setValidateDbConn(o.isValidateDbConn());
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

    public DBType getDBType() {
        String[] parts = url.split(":");
        if (parts.length >= 2) {
            return DBType.valueOf(parts[1].toUpperCase());
        }
        throw new IllegalStateException("Unrecognised db type in url " + url);
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

    public boolean isValidateDbConn() {
        return validateDbConn;
    }

    public void setValidateDbConn(boolean validateDbConn) {
        this.validateDbConn = validateDbConn;
    }

    public String resolvePassword() throws BeaconException {
        String dbPassword;
        if (StringUtils.isNotBlank(passwordAlias)) {
            dbPassword = new BeaconCredentialProvider().resolveAlias(passwordAlias);
        } else {
            dbPassword = getPassword();
        }
        return dbPassword;
    }

    public int getMinIdleConnections() {
        return minIdleConnections;
    }

    public void setMinIdleConnections(int minIdleConnections) {
        this.minIdleConnections = minIdleConnections;
    }

    public long getMaxWaitMSecs() {
        return maxWaitMSecs;
    }

    public void setMaxWaitMSecs(long maxWaitMSecs) {
        this.maxWaitMSecs = maxWaitMSecs;
    }

    public long getConnectTimeoutMSecs() {
        return connectTimeoutMSecs;
    }

    public void setConnectTimeoutMSecs(long connectTimeoutMSecs) {
        this.connectTimeoutMSecs = connectTimeoutMSecs;
    }

    public int getMaxIdleConnections() {
        return maxIdleConnections;
    }

    public void setMaxIdleConnections(int maxIdleConnections) {
        this.maxIdleConnections = maxIdleConnections;
    }
}
