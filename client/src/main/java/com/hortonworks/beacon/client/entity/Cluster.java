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

public class Cluster extends Entity {
    private String name;
    private String description;
    private String dataCenter;
    private String fsEndpoint;
    private String hsEndpoint;
    private String beaconEndpoint;
    private String tags;
    private String peers;
    private Properties customProperties;
    private Acl acl;

    public enum ClusterFields {
        NAME("name"),
        DECRIPTION("description"),
        DATACENTER("dataCenter"),
        FSENDPOINT("fsEndpoint"),
        HSENDPOINT("hsEndpoint"),
        BEACONENDPOINT("beaconEndpoint"),
        TAGS("tags"),
        PEERS("peers"),
        ACLOWNER("aclOwner"),
        ACLGROUP("aclGroup"),
        ACLPERMISSION("aclPermission");

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
        this.dataCenter = builder.dataCenter;
        this.fsEndpoint = builder.fsEndpoint;
        this.beaconEndpoint = builder.beaconEndpoint;
        this.hsEndpoint = builder.hsEndpoint;
        this.tags = builder.tags;
        this.peers = builder.peers;
        this.customProperties = builder.customProperties;
        this.acl = builder.acl;
    }

    public static class Builder {
        private String name;
        private String description;
        private String dataCenter;
        private String fsEndpoint;
        private String beaconEndpoint;
        private String hsEndpoint;
        private String tags;
        private String peers;
        private Properties customProperties;
        private Acl acl;

        public Builder(String name, String description, String fsEndpoint, String beaconEndpoint) {
            this.name = name;
            this.description = description;
            this.fsEndpoint = fsEndpoint;
            this.beaconEndpoint = beaconEndpoint;
        }

        public Builder dataCenter(String dataCenter) {
            this.dataCenter = dataCenter;
            return this;
        }

        public Builder hsEndpoint(String hsEndpoint) {
            this.hsEndpoint = hsEndpoint;
            return this;
        }

        public Builder tags(String tags) {
            this.tags = tags;
            return this;
        }

        public Builder peers(String peers) {
            this.peers = peers;
            return this;
        }

        public Builder customProperties(Properties customProperties) {
            this.customProperties = customProperties;
            return this;
        }

        public Builder acl(Acl acl) {
            this.acl = acl;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
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

    @Override
    public Acl getAcl() {
        return acl;
    }

    public void setAcl(Acl acl) {
        this.acl = acl;
    }

    @Override
    public String toString() {
        final String EQUALS = "=";
        StringBuilder clusterDefinition = new StringBuilder();
        clusterDefinition.append(ClusterFields.NAME.getName()).append(EQUALS).append(getField(name))
                .append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.DECRIPTION.getName()).append(EQUALS).append(getField(description))
                .append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.DATACENTER.getName()).append(EQUALS).append(getField(dataCenter))
                .append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.FSENDPOINT.getName()).append(EQUALS).append(getField(fsEndpoint))
                .append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.HSENDPOINT.getName()).append(EQUALS).append(getField(hsEndpoint))
                .append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.BEACONENDPOINT.getName()).append(EQUALS)
                .append(getField(beaconEndpoint)).append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.TAGS.getName()).append(EQUALS).append(getField(tags))
                .append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.PEERS.getName()).append(EQUALS).append(getField(peers))
                .append(System.lineSeparator());
        for (String propertyKey : customProperties.stringPropertyNames()) {
            clusterDefinition.append(propertyKey).append(EQUALS)
                    .append(getField(customProperties.getProperty(propertyKey))).append(System.lineSeparator());
        }
        clusterDefinition.append(ClusterFields.ACLOWNER.getName()).append(EQUALS)
                .append(getField(acl.getOwner())).append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.ACLGROUP.getName()).append(EQUALS)
                .append(getField(acl.getGroup())).append(System.lineSeparator());
        clusterDefinition.append(ClusterFields.ACLPERMISSION.getName()).append(EQUALS)
                .append(getField(acl.getPermission())).append(System.lineSeparator());

        return clusterDefinition.toString();
    }
}
