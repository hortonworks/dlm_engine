/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
        + "where b.clusterName = :clusterName AND b.clusterVersion = :clusterVersion"),
        @NamedQuery(name = "UPDATE_CLUSTER_PROP", query = "update ClusterPropertiesBean b set b.value = :valueParam "
                + "where b.clusterName = :clusterNameParam AND b.clusterVersion = :clusterVersionParam "
                + "AND b.name = :nameParam")
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
