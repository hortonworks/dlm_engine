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

import org.codehaus.jackson.annotate.JsonProperty;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * REST API response for beacon server status.
 */
@XmlRootElement(name = "status")
@XmlAccessorType(XmlAccessType.FIELD)
public class ServerStatusResult {

    @XmlElement
    private String status;

    @XmlElement
    private String version;

    @XmlElement
    private String plugins;

    @XmlElement
    private String security;

    @XmlElement
    private boolean wireEncryption;

    @XmlElement
    private boolean rangerCreateDenyPolicy;

    @XmlElement(name = "replication_TDE")
    private boolean replicationTDE;

    @XmlElement(name = "replication_cloud_fs")
    private boolean replicationCloudFS;

    @XmlElement(name = "replication_cloud_hive_withCluster")
    private boolean replicationCloudHiveWithCluster;

    private boolean cloudHosted;

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

    @JsonProperty
    public boolean getWireEncryption() {
        return wireEncryption;
    }

    public void setWireEncryption(boolean wireEncryption) {
        this.wireEncryption = Boolean.valueOf(wireEncryption);
    }

    public boolean doesRangerCreateDenyPolicy() {
        return rangerCreateDenyPolicy;
    }

    @JsonProperty   //Maps boolean to String in json for backward compatibility with 1.0
    public String getRangerCreateDenyPolicy() {
        return String.valueOf(rangerCreateDenyPolicy);
    }

    public void setRangerCreateDenyPolicy(boolean rangerCreateDenyPolicy) {
        this.rangerCreateDenyPolicy = rangerCreateDenyPolicy;
    }

    public void setReplicationTDE(boolean replicationTDE) {
        this.replicationTDE = replicationTDE;
    }

    public boolean isTDEReplicationEnabled() {
        return replicationTDE;
    }

    @JsonProperty
    public boolean getReplicationTDE() {
        return replicationTDE;
    }

    public void setReplicationCloudFS(boolean replicationCloudFS) {
        this.replicationCloudFS = replicationCloudFS;
    }

    public boolean isCloudFSReplicationEnabled() {
        return replicationCloudFS;
    }

    @JsonProperty
    public boolean getReplicationCloudFS() {
        return replicationCloudFS;
    }

    public void setReplicationCloudHiveWithCluster(boolean replicationCloudHiveWithCluster) {
        this.replicationCloudHiveWithCluster = replicationCloudHiveWithCluster;
    }

    public boolean isCloudHiveReplicationWithClusterEnabled() {
        return replicationCloudHiveWithCluster;
    }

    @JsonProperty
    public boolean getReplicationCloudHiveWithCluster() {
        return replicationCloudHiveWithCluster;
    }

    public boolean isCloudHosted() {
        return cloudHosted;
    }

    public void setCloudHosted(boolean cloudHosted) {
        this.cloudHosted = cloudHosted;
    }

    public ServerStatusResult() {
    }
}
