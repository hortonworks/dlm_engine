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

package com.hortonworks.beacon.client.entity;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.util.KnoxTokenUtils;

import java.util.List;
import java.util.Properties;

/**
 * The cluster contains the definition of different endpoints which are used by Beacon like HDFS, HS2 and others.
 */
public class Cluster extends Entity {
    private String name;
    private int version;
    private String description;
    private String fsEndpoint;
    private String hsEndpoint;
    private String beaconEndpoint;
    private String atlasEndpoint;
    private String rangerEndpoint;
    private boolean local;
    private String tags;
    private String peers;
    private List<PeerInfo> peersInfo;
    private Properties customProperties;
    private String user;

    /**
     * Cluster fields used in cluster properties.
     */
    public enum ClusterFields {
        NAME("name"),
        DESCRIPTION("description"),
        FSENDPOINT("fsEndpoint"),
        HSENDPOINT("hsEndpoint"),
        BEACONENDPOINT("beaconEndpoint"),
        ATLASENDPOINT("atlasEndpoint"),
        RANGERENDPOINT("rangerEndpoint"),
        HMSENDPOINT("hive.metastore.uris"),
        HIVE_CLOUD_ENCRYPTION_ALGORITHM("hive.cloud.encryptionAlgorithm"),
        HIVE_CLOUD_ENCRYPTION_KEY("hive.cloud.encryptionKey"),
        HIVE_WAREHOUSE("hive.metastore.warehouse.dir"),
        HIVE_METASTORE_PRINCIPAL("hive.metastore.kerberos.principal"),
        HIVE_INHERIT_PERMS("hive.warehouse.subdir.inherit.perms"),
        HIVE_FUNCTIONS_DIR("hive.repl.replica.functions.root.dir"),
        HIVE_SERVER_AUTHENTICATION("hive.server2.authentication"),
        CLOUDDATALAKE("cloudDataLake"),
        KNOX_GATEWAY_URL("knox.gateway.url"),
        LOCAL("local"),
        TAGS("tags"),
        PEERS("peers"),
        PEERSINFO("peersinfo"),
        USER("user");

        private final String name;

        ClusterFields(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }

    }

    public Cluster() {
    }

    public Cluster(Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.fsEndpoint = builder.fsEndpoint;
        this.beaconEndpoint = builder.beaconEndpoint;
        this.atlasEndpoint = builder.atlasEndpoint;
        this.rangerEndpoint = builder.rangerEndpoint;
        this.hsEndpoint = builder.hsEndpoint;
        this.local = builder.local;
        this.tags = builder.tags;
        this.peers = builder.peers;
        this.peersInfo = builder.peersInfo;
        this.customProperties = builder.customProperties;
        this.user = builder.user;
    }

    /**
     * Builder class for Beacon Cluster resource.
     */
    public static class Builder {
        private String name;
        private String description;
        private String fsEndpoint;
        private String beaconEndpoint;
        private String atlasEndpoint;
        private String rangerEndpoint;
        private String hsEndpoint;
        private boolean local;
        private String tags;
        private String peers;
        private List<PeerInfo> peersInfo;
        private Properties customProperties;
        private String user;

        public Builder(String nameValue, String description, String beaconEndpointValue) {
            this.name = nameValue;
            this.description = description;
            this.beaconEndpoint = beaconEndpointValue;
        }

        public Builder fsEndpoint(String fsEndpointValue) {
            this.fsEndpoint = fsEndpointValue;
            return this;
        }


        public Builder hsEndpoint(String hsEndpointValue) {
            this.hsEndpoint = hsEndpointValue;
            return this;
        }

        public Builder atlasEndpoint(String atlasEndpointValue) {
            this.atlasEndpoint = atlasEndpointValue;
            return this;
        }

        public Builder rangerEndpoint(String rangerEndpointValue) {
            this.rangerEndpoint = rangerEndpointValue;
            return this;
        }

        public Builder local(boolean localValue) {
            this.local = localValue;
            return this;
        }

        public Builder tags(String tagsValue) {
            this.tags = tagsValue;
            return this;
        }

        public Builder peers(String peersValue) {
            this.peers = peersValue;
            return this;
        }

        public Builder peersInfo(List<PeerInfo> peersInfoValue) {
            this.peersInfo = peersInfoValue;
            return this;
        }

        public Builder customProperties(Properties customPropertiesValue) {
            this.customProperties = customPropertiesValue;
            return this;
        }

        public Builder user(String userValue) {
            this.user = userValue;
            return this;
        }

        public Cluster build() {
            return new Cluster(this);
        }
    }

    @Override
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFsEndpoint() {
        return fsEndpoint;
    }

    public void setFsEndpoint(String fsEndpoint) {
        this.fsEndpoint = fsEndpoint;
    }

    public String getBeaconEndpoint() {
        if (BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()) {
            return KnoxTokenUtils.getKnoxProxiedURL(getKnoxGatewayURL(), "beacon");
        }
        return beaconEndpoint;
    }

    public void setBeaconEndpoint(String beaconEndpoint) {
        this.beaconEndpoint = beaconEndpoint;
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

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    @Override
    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getPeers() {
        return peers;
    }

    public void setPeers(String peers) {
        this.peers = peers;
    }

    public List<PeerInfo> getPeersInfo() {
        return peersInfo;
    }

    public void setPeersInfo(List<PeerInfo> peersInfo) {
        this.peersInfo = peersInfo;
    }

    public Properties getCustomProperties() {
        return customProperties;
    }

    public void setCustomProperties(Properties customProperties) {
        this.customProperties = customProperties;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHmsEndpoint() {
        return customProperties.getProperty(ClusterFields.HMSENDPOINT.getName());
    }

    public String getHiveCloudEncryptionAlgorithm() {
        return customProperties.getProperty(ClusterFields.HIVE_CLOUD_ENCRYPTION_ALGORITHM.getName());
    }

    public String getHiveCloudEncryptionKey() {
        return customProperties.getProperty(ClusterFields.HIVE_CLOUD_ENCRYPTION_KEY.getName());
    }

    public String getHiveWarehouseLocation() {
        return customProperties.getProperty(ClusterFields.HIVE_WAREHOUSE.getName());
    }

    public String getHiveMetastoreKerberosPrincipal() {
        return customProperties.getProperty(ClusterFields.HIVE_METASTORE_PRINCIPAL.getName());
    }

    public String getHiveServerAuthentication() {
        return customProperties.getProperty(ClusterFields.HIVE_SERVER_AUTHENTICATION.getName());
    }

    public String getKnoxGatewayURL() {
        return customProperties.getProperty(ClusterFields.KNOX_GATEWAY_URL.getName());
    }

    @Override
    public String toString() {
        StringBuilder clusterDefinition = new StringBuilder();
        appendNonEmpty(clusterDefinition, ClusterFields.NAME.getName(), name);
        appendNonEmpty(clusterDefinition, ClusterFields.DESCRIPTION.getName(), description);
        appendNonEmpty(clusterDefinition, ClusterFields.FSENDPOINT.getName(), fsEndpoint);
        appendNonEmpty(clusterDefinition, ClusterFields.HSENDPOINT.getName(), hsEndpoint);
        appendNonEmpty(clusterDefinition, ClusterFields.BEACONENDPOINT.getName(), beaconEndpoint);
        appendNonEmpty(clusterDefinition, ClusterFields.ATLASENDPOINT.getName(), atlasEndpoint);
        appendNonEmpty(clusterDefinition, ClusterFields.RANGERENDPOINT.getName(), rangerEndpoint);
        appendNonEmpty(clusterDefinition, ClusterFields.LOCAL.getName(), local);
        appendNonEmpty(clusterDefinition, ClusterFields.TAGS.getName(), tags);
        appendNonEmpty(clusterDefinition, ClusterFields.PEERS.getName(), peers);
        appendNonEmpty(clusterDefinition, ClusterFields.USER.getName(), user);
        for (String propertyKey : customProperties.stringPropertyNames()) {
            appendNonEmpty(clusterDefinition, propertyKey, customProperties.getProperty(propertyKey));
        }
        return clusterDefinition.toString();
    }
}
