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
import org.apache.commons.lang3.StringUtils;

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
    private String description;
    private String status;
    private String lastInstanceStatus;
    private String executionType;
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
    private String user;
    private Notification notification;

    /**
     * ReplicationPolicy fields used in policy properties.
     */
    public enum ReplicationPolicyFields {
        ID("id"),
        NAME("name"),
        TYPE("type"),
        DESCRIPTION("description"),
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
        USER("user"),
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
        this.description = builder.description;
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
        this.user = builder.user;
        this.notification = builder.notification;
    }

    /**
     * Builder class for Beacon ReplicationPolicy resource.
     */
    public static class Builder {
        private String name;
        private String type;
        private String description;
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
        private String user;
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

        public Builder description(String descriptionValue) {
            if (StringUtils.isNotBlank(descriptionValue)) {
                this.description = descriptionValue;
            }
            return this;
        }

        public Builder startTime(Date startTimeValue) {
            if (startTimeValue != null) {
                this.startTime = new Date(startTimeValue.getTime());
            }
            return this;
        }

        public Builder endTime(Date endTimeValue) {
            if (endTimeValue != null) {
                this.endTime = new Date(endTimeValue.getTime());
            }
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

        public Builder user(String username) {
            this.user = username;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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
        return startTime != null ? new Date(startTime.getTime()) : null;

    }

    public void setStartTime(Date startTime) {
        if (startTime != null) {
            this.startTime = new Date(startTime.getTime());
        }
    }

    public Date getEndTime() {
        return endTime != null ? new Date(endTime.getTime()) : null;
    }

    public void setEndTime(Date endTime) {
        if (endTime != null) {
            this.endTime = new Date(endTime.getTime());
        }
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLastInstanceStatus() {
        return lastInstanceStatus;
    }

    public void setLastInstanceStatus(String lastInstanceStatus) {
        this.lastInstanceStatus = lastInstanceStatus;
    }

    public String getExecutionType() {
        return executionType;
    }

    public void setExecutionType(String executionType) {
        this.executionType = executionType;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public String toString() {
        StringBuilder policyDefinition = new StringBuilder();
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.ID.getName(), policyId);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.NAME.getName(), name);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.TYPE.getName(), type);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.SOURCEDATASET.getName(), sourceDataset);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.TARGETDATASET.getName(), targetDataset);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.SOURCECLUSTER.getName(), sourceCluster);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.TARGETCLUSTER.getName(), targetCluster);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.STARTTIME.getName(), DateUtil.formatDate(startTime));
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.ENDTIME.getName(), DateUtil.formatDate(endTime));
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.FREQUENCYINSEC.getName(), frequencyInSec);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.TAGS.getName(), tags);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.RETRYATTEMPTS.getName(), retry.getAttempts());
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.RETRYDELAY.getName(), retry.getDelay());
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.USER.getName(), user);
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.NOTIFICATIONTO.getName(), notification.getTo());
        appendNonEmpty(policyDefinition, ReplicationPolicyFields.NOTIFICATIONTYPE.getName(), notification.getType());
        for (String propertyKey : customProperties.stringPropertyNames()) {
            appendNonEmpty(policyDefinition, propertyKey, customProperties.getProperty(propertyKey));
        }
        return policyDefinition.toString();
    }
}


