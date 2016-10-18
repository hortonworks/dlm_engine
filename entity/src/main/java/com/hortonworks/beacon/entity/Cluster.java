package com.hortonworks.beacon.entity;

import java.util.Properties;

public class Cluster extends Entity {
    private String name;
    private String description;
    private String dataCenter;
    private String fsEndpoint;
    private String hsEndpoint;
    private String tags;
    private String peers;
    private Properties customProperties;
    private Acl acl;

    public Cluster() {
    }

    public Cluster(Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.dataCenter = builder.dataCenter;
        this.fsEndpoint = builder.fsEndpoint;
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
        private String hsEndpoint;
        private String tags;
        private String peers;
        private Properties customProperties;
        private Acl acl;

        public Builder(String name, String description, String fsEndpoint) {
            this.name = name;
            this.description = description;
            this.fsEndpoint = fsEndpoint;
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
        return "Cluster{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", dataCenter='" + dataCenter + '\'' +
                ", fsEndpoint='" + fsEndpoint + '\'' +
                ", hsEndpoint='" + hsEndpoint + '\'' +
                ", tags='" + tags + '\'' +
                ", peers='" + peers + '\'' +
                ", customProperties=" + customProperties +
                ", acl=" + acl +
                '}';
    }

}
