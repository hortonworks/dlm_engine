/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
            Configuration conf = new Configuration();
            dbPassword = CredentialProviderHelper.resolveAlias(conf, passwordAlias);
        } else {
            dbPassword = getPassword();
        }
        return dbPassword;
    }
}
