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

import com.hortonworks.beacon.util.DateUtil;

import java.util.Date;
import java.util.Properties;

/**
 * The ReplicationPolicy contains the definition of policy to be replicated like sourceDataset, targetDataset and
 * others.
 */
public class ReplicationPolicy extends Entity {
    private String policyId;
    private String name;
    private String type;
    private String sourceDataset;
    private String targetDataset;
    private String sourceCluster;
    private String targetCluster;
    private Date startTime;
    private Date endTime;
    private int frequencyInSec;
    private String tags;
    private Properties customProperties;
    private Retry retry;
    private Acl acl;
    private Notification notification;

    private static final String EQUALS = "=";

    /**
     * ReplicationPolicy fields used in policy properties.
     */
    public enum ReplicationPolicyFields {
        NAME("name"),
        TYPE("type"),
        SOURCEDATASET("sourceDataset"),
        TARGETDATASET("targetDataset"),
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
        this.sourceDataset = builder.sourceDataset;
        this.targetDataset = builder.targetDataset;
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

    /**
     * Builder class for Beacon ReplicationPolicy resource.
     */
    public static class Builder {
        private String name;
        private String type;
        private String sourceDataset;
        private String targetDataset;
        private String sourceCluster;
        private String targetCluster;
        private Date startTime;
        private Date endTime;
        private int frequencyInSec;
        private String tags;
        private Properties customProperties;
        private Retry retry;
        private Acl acl;
        private Notification notification;

        public Builder(String nameValue, String typeValue, String sourceDatasetValue,
                       String targetDatasetValue, String sourceClusterValue,
                       String targetClusterValue, int frequencyInSecValue) {
            this.name = nameValue;
            this.type = typeValue;
            this.sourceDataset = sourceDatasetValue;
            this.targetDataset = targetDatasetValue;
            this.sourceCluster = sourceClusterValue;
            this.targetCluster = targetClusterValue;
            this.frequencyInSec = frequencyInSecValue;
        }

        public Builder startTime(Date startTimeValue) {
            this.startTime = startTimeValue;
            return this;
        }

        public Builder endTime(Date endTimeValue) {
            this.endTime = endTimeValue;
            return this;
        }

        public Builder tags(String tagsValue) {
            this.tags = tagsValue;
            return this;
        }

        public Builder customProperties(Properties customPropertiesValue) {
            this.customProperties = customPropertiesValue;
            return this;
        }

        public Builder retry(Retry retryValue) {
            this.retry = retryValue;
            return this;
        }

        public Builder acl(Acl aclValue) {
            this.acl = aclValue;
            return this;
        }

        public Builder notification(Notification notificationValue) {
            this.notification = notificationValue;
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

    public String getSourceDataset() {
        return sourceDataset;
    }

    public void setSourceDataset(String sourceDataset) {
        this.sourceDataset = sourceDataset;
    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public void setTargetDataset(String targetDataset) {
        this.targetDataset = targetDataset;
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

    public int getFrequencyInSec() {
        return frequencyInSec;
    }

    public void setFrequencyInSec(int frequencyInSec) {
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

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    @Override
    public String toString() {
        StringBuilder policyDefinition = new StringBuilder();
        policyDefinition.append(ReplicationPolicyFields.NAME.getName()).append(EQUALS).append(getField(name))
                .append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.TYPE.getName()).append(EQUALS).append(getField(type))
                .append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.SOURCEDATASET.getName()).append(EQUALS)
                .append(getField(sourceDataset)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.TARGETDATASET.getName()).append(EQUALS)
                .append(getField(targetDataset)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.SOURCECLUSTER.getName()).append(EQUALS)
                .append(getField(sourceCluster)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.TARGETCLUSTER.getName()).append(EQUALS)
                .append(getField(targetCluster)).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.STARTTIME.getName()).append(EQUALS)
                .append(getField(DateUtil.formatDate(startTime))).append(System.lineSeparator());
        policyDefinition.append(ReplicationPolicyFields.ENDTIME.getName()).append(EQUALS)
                .append(getField(DateUtil.formatDate(endTime))).append(System.lineSeparator());
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


