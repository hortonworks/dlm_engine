package com.hortonworks.beacon.entity;

import java.util.Properties;

/**
 * Created by sramesh on 9/30/16.
 */
public class ReplicationPolicy extends Entity {
    private String name;
    private String tags;
    private Properties customProperties;
    //    private int version;
    /* TODO: only one cluster ? */
    private String cluster;
    /* Freq string? */
    private String frequency;
    private Retry retry;
    private Acl acl;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

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

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getFrequency() {
        return frequency;
    }

    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
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
        return "ReplicationPolicy{" +
                "name='" + name + '\'' +
                ", tags='" + tags + '\'' +
                ", customProperties=" + customProperties +
                ", cluster='" + cluster + '\'' +
                ", frequency='" + frequency + '\'' +
                ", retry=" + retry +
                ", acl=" + acl +
                '}';
    }
}


