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

package com.hortonworks.beacon.client.entity;

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
    private String tags;
    private String peers;
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
        TAGS("tags"),
        PEERS("peers"),
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
        this.tags = builder.tags;
        this.peers = builder.peers;
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
        private String tags;
        private String peers;
        private Properties customProperties;
        private String user;

        public Builder(String nameValue, String description, String fsEndpointValue, String beaconEndpointValue) {
            this.name = nameValue;
            this.description = description;
            this.fsEndpoint = fsEndpointValue;
            this.beaconEndpoint = beaconEndpointValue;
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

        public Builder tags(String tagsValue) {
            this.tags = tagsValue;
            return this;
        }

        public Builder peers(String peersValue) {
            this.peers = peersValue;
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
        appendNonEmpty(clusterDefinition, ClusterFields.TAGS.getName(), tags);
        appendNonEmpty(clusterDefinition, ClusterFields.PEERS.getName(), peers);
        appendNonEmpty(clusterDefinition, ClusterFields.USER.getName(), user);
        for (String propertyKey : customProperties.stringPropertyNames()) {
            appendNonEmpty(clusterDefinition, propertyKey, customProperties.getProperty(propertyKey));
        }
        return clusterDefinition.toString();
    }
}
