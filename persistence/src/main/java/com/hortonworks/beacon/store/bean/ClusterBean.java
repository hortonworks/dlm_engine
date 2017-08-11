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
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.util.Date;
import java.util.List;

/**
 * Cluster entity bean.
 */
@SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL", "UWF_UNWRITTEN_FIELD"})
@Entity
@Table(name = "BEACON_CLUSTER")
@IdClass(ClusterKey.class)
@NamedQueries({
        @NamedQuery(name = "GET_CLUSTER_LATEST", query = "select OBJECT(b) from ClusterBean b where b.name = :name "
                + "order by b.version DESC"),
        @NamedQuery(name = "GET_CLUSTER_ACTIVE", query = "select OBJECT(b) from ClusterBean b where b.name = :name "
                + " AND b.retirementTime IS NULL"),
        @NamedQuery(name = "RETIRE_CLUSTER", query = "update ClusterBean b set b.retirementTime = :retirementTime "
                + "where b.name = :name AND b.retirementTime IS NULL")
    })
public class ClusterBean {

    @Id
    @Column(name = "name")
    private String name;

    @Id
    @Column(name = "version")
    private int version;

    @Column(name = "change_id")
    private int changeId;

    @Column(name = "description")
    private String description;

    @Column(name = "beacon_uri")
    private String beaconUri;

    @Column(name = "fs_endpoint")
    private String fsEndpoint;

    @Column(name = "hs_endpoint")
    private String hsEndpoint;

    @Column(name = "atlas_endpoint")
    private String atlasEndpoint;

    @Column(name = "ranger_endpoint")
    private String rangerEndpoint;

    @Column(name = "created_time")
    private java.sql.Timestamp creationTime;

    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTime;

    @Column(name = "retirement_time")
    private java.sql.Timestamp retirementTime;

    @Column(name = "tags")
    private String tags;

    private List<ClusterPropertiesBean> customProperties;

    private List<ClusterPairBean> clusterPairs;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getChangeId() {
        return changeId;
    }

    public void setChangeId(int changeId) {
        this.changeId = changeId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getBeaconUri() {
        return beaconUri;
    }

    public void setBeaconUri(String beaconUri) {
        this.beaconUri = beaconUri;
    }

    public String getFsEndpoint() {
        return fsEndpoint;
    }

    public void setFsEndpoint(String fsEndpoint) {
        this.fsEndpoint = fsEndpoint;
    }

    public String getHsEndpoint() {
        return hsEndpoint;
    }

    public void setHsEndpoint(String hsEndpoint) {
        this.hsEndpoint = hsEndpoint;
    }

    public String getAtlasEndpoint() {
        return atlasEndpoint;
    }

    public void setAtlasEndpoint(String atlasEndpoint) {
        this.atlasEndpoint = atlasEndpoint;
    }

    public String getRangerEndpoint() {
        return rangerEndpoint;
    }

    public void setRangerEndpoint(String rangerEndpoint) {
        this.rangerEndpoint = rangerEndpoint;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Date createdTime) {
        if (createdTime != null) {
            this.creationTime = new java.sql.Timestamp(createdTime.getTime());
        }
    }

    public Date getLastModifiedTime() {
        if (lastModifiedTime != null) {
            return new Date(lastModifiedTime.getTime());
        }
        return null;
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        if (lastModifiedTime != null) {
            this.lastModifiedTime = new java.sql.Timestamp(lastModifiedTime.getTime());
        }
    }

    public Date getRetirementTime() {
        if (retirementTime != null) {
            return new Date(retirementTime.getTime());
        }
        return null;
    }

    public void setRetirementTime(Date retirementTime) {
        if (retirementTime != null) {
            this.retirementTime = new java.sql.Timestamp(retirementTime.getTime());
        }
    }

    public List<ClusterPropertiesBean> getCustomProperties() {
        return customProperties;
    }

    public void setCustomProperties(List<ClusterPropertiesBean> customProperties) {
        this.customProperties = customProperties;
    }

    public List<ClusterPairBean> getClusterPairs() {
        return clusterPairs;
    }

    public void setClusterPairs(List<ClusterPairBean> pairBeans) {
        this.clusterPairs = pairBeans;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public ClusterBean() {
    }

    public ClusterBean(String name) {
        this.name = name;
    }
}
