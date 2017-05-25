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

package com.hortonworks.beacon.store.bean;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.util.Date;

/**
 * Cluster properties bean.
 */
@SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL", "UWF_UNWRITTEN_FIELD"})
@Entity
@Table(name = "BEACON_CLUSTER_PROP")
@NamedQueries({
        @NamedQuery(name = "GET_CLUSTER_PROP", query = "select OBJECT(b) from ClusterPropertiesBean b "
        + "where b.clusterName = :clusterName AND b.clusterVersion = :clusterVersion")
        })
public class ClusterPropertiesBean {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private long id;

    @Column(name = "cluster_name")
    private String clusterName;

    @Column(name = "cluster_version")
    private int clusterVersion;

    @Column(name = "created_time")
    private java.sql.Timestamp createdTime;

    @Column(name = "name")
    private String name;

    @Column(name = "value")
    private String value;

    @Column(name = "type")
    private String type;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public int getClusterVersion() {
        return clusterVersion;
    }

    public void setClusterVersion(int clusterVersion) {
        this.clusterVersion = clusterVersion;
    }

    public Date getCreatedTime() {
        if (createdTime != null) {
            return createdTime;
        }
        return null;
    }

    public void setCreatedTime(Date createdTime) {
        if (createdTime != null) {
            this.createdTime = new java.sql.Timestamp(createdTime.getTime());
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ClusterPropertiesBean() {
    }

    public ClusterPropertiesBean(String clusterName, int clusterVersion) {
        this.clusterName = clusterName;
        this.clusterVersion = clusterVersion;
    }
}
