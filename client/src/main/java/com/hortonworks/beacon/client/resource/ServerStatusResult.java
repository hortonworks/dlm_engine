/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client.resource;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.hortonworks.beacon.RequestContext;

/**
 * REST API response for beacon server status.
 */
@XmlRootElement(name = "status")
@XmlAccessorType(XmlAccessType.FIELD)
public class ServerStatusResult {

    @XmlElement
    private String requestId;

    @XmlElement
    private String status;

    @XmlElement
    private String version;

    @XmlElement
    private String plugins;

    @XmlElement
    private String security;

    @XmlElement
    private Boolean wireEncryption;

    @XmlElement
    private Boolean rangerCreateDenyPolicy;

    @XmlElement(name = "replication_TDE")
    private Boolean replicationTDE;

    @XmlElement(name = "replication_cloud_fs")
    private Boolean replicationCloudFS;

    @XmlElement(name = "replication_cloud_hive_withCluster")
    private Boolean replicationCloudHiveWithCluster;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getPlugins() {
        return plugins;
    }

    public void setPlugins(String plugin) {
        this.plugins = plugin;
    }

    public String getSecurity() {
        return security;
    }

    public void setSecurity(String security) {
        this.security = security;
    }

    public boolean isWireEncryptionEnabled() {
        return wireEncryption;
    }

    public void setWireEncryption(Boolean wireEncryption) {
        this.wireEncryption = wireEncryption;
    }

    public boolean doesRangerCreateDenyPolicy() {
        return rangerCreateDenyPolicy;
    }

    public void setRangerCreateDenyPolicy(boolean rangerCreateDenyPolicy) {
        this.rangerCreateDenyPolicy = rangerCreateDenyPolicy;
    }

    public void setReplicationTDE(Boolean replicationTDE) {
        this.replicationTDE = replicationTDE;
    }

    public boolean isTDEReplicationEnabled() {
        return replicationTDE;
    }

    public void setReplicationCloudFS(Boolean replicationCloudFS) {
        this.replicationCloudFS = replicationCloudFS;
    }

    public boolean isCloudFSReplicationEnabled() {
        return replicationCloudFS;
    }

    public void setReplicationCloudHiveWithCluster(Boolean replicationCloudHiveWithCluster) {
        this.replicationCloudHiveWithCluster = replicationCloudHiveWithCluster;
    }

    public boolean isCloudHiveReplicationWithClusterEnabled() {
        return replicationCloudHiveWithCluster;
    }

    public ServerStatusResult() {
        this.requestId = RequestContext.get().getRequestId();
    }
}
