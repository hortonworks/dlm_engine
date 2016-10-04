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

package com.hortonworks.beacon.scheduler.hive;

public class ReplicationDefinition {
    private String hiveServerURL;
    private String dbName;
    private String tableName;
    private String stagingDir;
    private String kerberosCredential;


    public ReplicationDefinition(String hiveServerURL, String dbName, String tableName,
                                 String stagingDir, String kerberosCredential) {
        this.hiveServerURL = hiveServerURL;
        this.dbName = dbName;
        this.tableName = tableName;
        this.stagingDir = stagingDir;
        this.kerberosCredential = kerberosCredential;
    }

    public String getHiveServerURL() {
        return hiveServerURL;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getStagingDir() {
        return stagingDir;
    }

    public String getKerberosCredential() {
        return kerberosCredential;
    }

    public void setHiveServerURL(String hiveServerURL) {
        this.hiveServerURL = hiveServerURL;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setStagingDir(String stagingDir) {
        this.stagingDir = stagingDir;
    }

    public void setKerberosCredential(String kerberosCredential) {
        this.kerberosCredential = kerberosCredential;
    }

    @Override
    public String toString() {
        return "ReplicationDefinition{" +
                "hiveServerURL='" + hiveServerURL + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", stagingDir='" + stagingDir + '\'' +
                ", kerberosCredential='" + kerberosCredential + '\'' +
                '}';
    }
}

