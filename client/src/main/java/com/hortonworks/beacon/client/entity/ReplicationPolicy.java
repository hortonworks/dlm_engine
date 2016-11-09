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

    public enum ReplicationPolicyFields {
        NAME("name"),
        TYPE("type"),
        DATASET("dataset"),
        SOURCECLUSTER("sourceCluster"),
        TARGETCLUSTER("targetCluster"),
        STARTTIME("startTime"),
        ENDTIME("endTime"),
        FREQUENCYINSEC("frequencyInSec"),
        TAGS("tags"),
        RETRYATTEMPTS("retryAttempts"),
        RETRYDELAY("retryDelay"),
        ACLOWNER("aclOwner"),
        ACLGROUP("aclGroup"),
        ACLPERMISSION("aclPermission"),
        NOTIFICATIONTYPE("notificationType"),
        NOTIFICATIONTO("notificationReceivers");

        private final String name;

        ReplicationPolicyFields(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }

    }

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
        final String EQUALS = "=";
        StringBuilder policyDefinition = new StringBuilder();
        policyDefinition.append(ReplicationPolicyFields.NAME.getName()).append(EQUALS).append(getField(name))
                .append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.TYPE.getName()).append(EQUALS).append(getField(type))
                .append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.DATASET.getName()).append(EQUALS)
                .append(getField(dataset)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.SOURCECLUSTER.getName()).append(EQUALS)
                .append(getField(sourceCluster)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.TARGETCLUSTER.getName()).append(EQUALS)
                .append(getField(targetCluster)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.STARTTIME.getName()).append(EQUALS)
                .append(getField(startTime)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.ENDTIME.getName()).append(EQUALS)
                .append(getField(endTime)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.FREQUENCYINSEC.getName()).append(EQUALS)
                .append(getField(frequencyInSec)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.TAGS.getName()).append(EQUALS).append(getField(tags))
                .append(System.lineSeparator());
        for (String propertyKey : customProperties.stringPropertyNames()) {
            policyDefinition.append(propertyKey).append(EQUALS)
                    .append(getField(customProperties.getProperty(propertyKey))).append(System.lineSeparator());
        }
        policyDefinition.append(ReplicationPolicyFields.RETRYATTEMPTS.getName()).append(EQUALS)
                .append(getField(retry.getAttempts())).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.RETRYDELAY.getName()).append(EQUALS)
                .append(getField(retry.getDelay())).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.ACLOWNER.getName()).append(EQUALS)
                .append(getField(acl.getOwner())).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.ACLGROUP.getName()).append(EQUALS)
                .append(getField(acl.getGroup())).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.ACLPERMISSION.getName()).append(EQUALS)
                .append(getField(acl.getPermission())).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.NOTIFICATIONTO.getName()).append(EQUALS)
                .append(getField(notification.getTo())).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.NOTIFICATIONTYPE.getName()).append(EQUALS)
                .append(getField(notification.getType())).append(System.lineSeparator());

        return policyDefinition.toString();
    }
}


