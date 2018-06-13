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

    @XmlElement
    private boolean enableSourceSnapshottable;

    @XmlElement
    private boolean knoxProxyingSupported;

    @XmlElement
    private boolean knoxProxyingEnabled;

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

    @JsonProperty
    public boolean getEnableSourceSnapshottable() {
        return enableSourceSnapshottable;
    }

    public void setEnableSourceSnapshottable(boolean enableSourceSnapshottable) {
        this.enableSourceSnapshottable = enableSourceSnapshottable;
    }

    @JsonProperty
    public boolean getKnoxProxyingEnabled() {
        return  knoxProxyingEnabled;
    }

    public boolean isKnoxProxyingEnabled() {
        return  knoxProxyingEnabled;
    }

    public void setKnoxProxyingEnabled(boolean knoxProxyingEnabled) {
        this.knoxProxyingEnabled = knoxProxyingEnabled;
    }


    @JsonProperty
    public boolean getKnoxProxyingSupported() {
        return  knoxProxyingSupported;
    }

    public boolean isKnoxProxyingSupported() {
        return  knoxProxyingSupported;
    }

    public void setKnoxProxyingSupported(boolean knoxProxyingSupported) {
        this.knoxProxyingSupported = knoxProxyingSupported;
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
