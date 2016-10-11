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

    public String getPeers() {
        return peers;
    }

    public void setPeers(String peers) {
        this.peers = peers;
    }

    @Override
    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
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
