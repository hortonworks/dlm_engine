package com.hortonworks.beacon.entity;

import java.util.Properties;

public class ReplicationPolicy extends Entity {
    private String name;
    private String type;
    private String dataset;
    private String sourceCluster;
    private String targetCluster;
    private long frequencyInSec;
    private String tags;
    private Properties customProperties;
    private Retry retry;
    private Acl acl;
    private Notification notification;

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public long getFrequencyInSec() {
        return frequencyInSec;
    }

    public void setFrequencyInSec(long frequencyInSec) {
        this.frequencyInSec = frequencyInSec;
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

    public String getSourceCluster() {
        return sourceCluster;
    }

    public void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    public String getTargetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
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

    public Notification getNotification() {
        return notification;
    }

    public void setNotification(Notification notification) {
        this.notification = notification;
    }

    @Override
    public String toString() {
        return "ReplicationPolicy{" +
                "name='" + name + '\'' +
                ", tags='" + tags + '\'' +
                ", customProperties=" + customProperties +
                ", sourceCluster='" + sourceCluster + '\'' +
                ", targetCluster='" + targetCluster + '\'' +
                ", frequencyInSec='" + frequencyInSec + '\'' +
                ", retry=" + retry +
                ", acl=" + acl +
                ", notification=" + notification +
                '}';
    }


}


