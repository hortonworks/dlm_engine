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
 * Cluster pairing bean.
 */
@SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL", "UWF_UNWRITTEN_FIELD"})
@Entity
@Table(name = "BEACON_CLUSTER_PAIR")
@NamedQueries({
        @NamedQuery(name = "GET_CLUSTER_PAIR", query = "select OBJECT(b) from ClusterPairBean b where "
                + "(b.clusterName = :clusterName AND b.clusterVersion = :clusterVersion) OR "
                + "(b.pairedClusterName = :pairedClusterName AND b.pairedClusterVersion = :pairedClusterVersion)"),
        @NamedQuery(name = "UPDATE_CLUSTER_PAIR_STATUS", query = "update ClusterPairBean b set b.status = :status, "
                + "b.lastModifiedTime = :lastModifiedTime "
                + "where (b.clusterName = :clusterName AND b.clusterVersion = :clusterVersion) AND "
                + "(b.pairedClusterName = :pairedClusterName AND b.pairedClusterVersion = :pairedClusterVersion)"),
        @NamedQuery(name = "EXIST_CLUSTER_PAIR", query = "select OBJECT(b) from ClusterPairBean b "
                + "where (b.clusterName = :clusterName AND b.clusterVersion = :clusterVersion) AND "
                + "(b.pairedClusterName = :pairedClusterName AND b.pairedClusterVersion = :pairedClusterVersion)")
        })
public class ClusterPairBean {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private long id;

    @Column(name = "cluster_name")
    private String clusterName;

    @Column(name = "cluster_version")
    private int clusterVersion;

    @Column(name = "paired_cluster_name")
    private String pairedClusterName;

    @Column(name = "paired_cluster_version")
    private int pairedClusterVersion;

    @Column(name = "status")
    private String status;

    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTime;

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

    public String getPairedClusterName() {
        return pairedClusterName;
    }

    public void setPairedClusterName(String pairedClusterName) {
        this.pairedClusterName = pairedClusterName;
    }

    public int getPairedClusterVersion() {
        return pairedClusterVersion;
    }

    public void setPairedClusterVersion(int pairedClusterVersion) {
        this.pairedClusterVersion = pairedClusterVersion;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getLastModifiedTime() {
        if (lastModifiedTime != null) {
            return lastModifiedTime;
        }
        return null;
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        if (lastModifiedTime != null) {
            this.lastModifiedTime = new java.sql.Timestamp(lastModifiedTime.getTime());
        }
    }

    public ClusterPairBean() {
    }
}