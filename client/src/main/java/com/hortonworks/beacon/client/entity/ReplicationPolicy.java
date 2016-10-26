package com.hortonworks.beacon.client.entity;

import java.util.Date;
import java.util.Properties;

public class ReplicationPolicy extends Entity {
    private String name;
    private String type;
    private String dataset;
    private String sourceCluster;
    private String targetCluster;
    private Date startTime;
    private Date endTime;
    private long frequencyInSec;
    private String tags;
    private Properties customProperties;
    private Retry retry;
    private Acl acl;
    private Notification notification;

    public ReplicationPolicy() {
    }

    public ReplicationPolicy(Builder builder) {
        this.name = builder.name;
        this.type = builder.type;
        this.dataset = builder.dataset;
        this.sourceCluster = builder.sourceCluster;
        this.targetCluster = builder.targetCluster;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.tags = builder.tags;
        this.frequencyInSec = builder.frequencyInSec;
        this.customProperties = builder.customProperties;
        this.retry = builder.retry;
        this.acl = builder.acl;
        this.notification = builder.notification;
    }

    public static class Builder {
        private String name;
        private String type;
        private String dataset;
        private String sourceCluster;
        private String targetCluster;
        private Date startTime;
        private Date endTime;
        private long frequencyInSec;
        private String tags;
        private Properties customProperties;
        private Retry retry;
        private Acl acl;
        private Notification notification;

        public Builder(String name, String type, String dataset, String sourceCluster,
                       String targetCluster, long frequencyInSec) {
            this.name = name;
            this.type = type;
            this.dataset = dataset;
            this.sourceCluster = sourceCluster;
            this.targetCluster = targetCluster;
            this.frequencyInSec = frequencyInSec;
        }

        public Builder startTime(Date startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder endTime(Date endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder tags(String tags) {
            this.tags = tags;
            return this;
        }

        public Builder customProperties(Properties customProperties) {
            this.customProperties = customProperties;
            return this;
        }

        public Builder retry(Retry retry) {
            this.retry = retry;
            return this;
        }

        public Builder acl(Acl acl) {
            this.acl = acl;
            return this;
        }

        public Builder notification(Notification notification) {
            this.notification = notification;
            return this;
        }

        public ReplicationPolicy build() {
            return new ReplicationPolicy(this);
        }
    }

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

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
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
                ", type='" + type + '\'' +
                ", dataset='" + dataset + '\'' +
                ", sourceCluster='" + sourceCluster + '\'' +
                ", targetCluster='" + targetCluster + '\'' +
                ", start=" + startTime +
                ", end=" + endTime +
                ", frequencyInSec=" + frequencyInSec +
                ", tags='" + tags + '\'' +
                ", customProperties=" + customProperties +
                ", retry=" + retry +
                ", acl=" + acl +
                ", notification=" + notification +
                '}';
    }
}


