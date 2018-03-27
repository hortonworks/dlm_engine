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
