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

package com.hortonworks.beacon.store.bean;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.util.Date;
import java.util.List;

/**
 * Bean of policy.
 */
@SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL", "UWF_UNWRITTEN_FIELD"})
@Entity
@Table(name = "BEACON_POLICY")
@NamedQueries({
        @NamedQuery(name = "GET_ACTIVE_POLICY", query = "select OBJECT(b) from PolicyBean b where b.name = :name "
                + "AND b.retirementTime IS NULL"),
        @NamedQuery(name = "GET_POLICY", query = "select OBJECT(b) from PolicyBean b where b.name = :name "
                + "order by b.version DESC"),
        @NamedQuery(name = "GET_POLICIES_FOR_TYPE", query = "select OBJECT(b) from PolicyBean b "
                + "where b.type = :policyType AND b.retirementTime IS NULL"),
        @NamedQuery(name = "GET_SUBMITTED_POLICY", query = "select OBJECT(b) from PolicyBean b "
                + "where b.name = :name AND b.retirementTime IS NULL AND b.status = :status"),
        @NamedQuery(name = "GET_PAIRED_CLUSTER_POLICY", query = "select COUNT(b.id) from PolicyBean b "
                + "where b.retirementTime IS NULL AND ("
                + "(b.sourceCluster = :sourceCluster AND b.targetCluster = :targetCluster) OR "
                + "(b.sourceCluster = :targetCluster AND b.targetCluster = :sourceCluster) "
                + ")"),
        @NamedQuery(name = "GET_POLICY_BY_ID", query = "select OBJECT(b) from PolicyBean b "
                + "where b.id = :id AND b.retirementTime IS NULL"),
        @NamedQuery(name = "GET_ARCHIVED_POLICY", query = "select OBJECT(b) from PolicyBean b "
                + "where b.name = :name AND b.retirementTime IS NOT NULL"),
        @NamedQuery(name = "DELETE_POLICY", query = "update PolicyBean b set b.retirementTime = :retirementTime, "
                + "b.status = :status where b.name = :name AND b.retirementTime IS NULL"),
        @NamedQuery(name = "UPDATE_STATUS", query = "update PolicyBean b set b.status = :status, "
                + "b.lastModifiedTime = :lastModifiedTime "
                + "where b.name = :name AND b.type = :policyType AND b.retirementTime IS NULL"),
        @NamedQuery(name = "UPDATE_JOBS", query = "update PolicyBean b set b.jobs = :jobs, "
                + "b.lastModifiedTime = :lastModifiedTime where b.id = :id"),
        @NamedQuery(name = "UPDATE_POLICY_LAST_INS_STATUS", query = "update PolicyBean b "
                + "set b.lastInstanceStatus = :lastInstanceStatus "
                + "where b.id = :id AND b.retirementTime IS NULL"),
        @NamedQuery(name = "DELETE_RETIRED_POLICY", query = "delete from PolicyBean b "
                + "where b.retirementTime < :retirementTime")
    })
public class PolicyBean {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "name")
    private String name;

    @Column(name = "version")
    private int version;

    @Column(name = "change_id")
    private int changeId;

    @Column(name = "status")
    private String status;

    @Column(name = "last_instance_status")
    private String lastInstanceStatus;

    @Column(name = "type")
    private String type;

    @Column(name = "source_cluster")
    private String sourceCluster;

    @Column(name = "target_cluster")
    private String targetCluster;

    @Column(name = "source_dataset")
    private String sourceDataset;

    @Column(name = "target_dataset")
    private String targetDataset;

    @Column(name = "created_time")
    private java.sql.Timestamp creationTime;

    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTime;

    @Column(name = "start_time")
    private java.sql.Timestamp startTime;

    @Column(name = "end_time")
    private java.sql.Timestamp endTime;

    @Column(name = "frequency")
    private int frequencyInSec;

    @Column(name = "notification_type")
    private String notificationType;

    @Column(name = "notification_to")
    private String notificationTo;

    @Column(name = "retry_count")
    private int retryCount;

    @Column(name = "retry_delay")
    private long retryDelay;

    @Column(name = "tags")
    private String tags;

    @Column(name = "execution_type")
    private String executionType;

    @Column(name = "retirement_time")
    private java.sql.Timestamp retirementTime;

    @Column(name = "jobs")
    private String jobs;

    @Column(name = "username")
    private String user;

    private List<PolicyPropertiesBean> customProperties;

    public String getId() {
        return id;
    }

    public void setId(String policyId) {
        this.id = policyId;
    }

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

    public int getChangeId() {
        return changeId;
    }

    public void setChangeId(int changeId) {
        this.changeId = changeId;
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public Date getCreationTime() {
        return new Date(this.creationTime.getTime());
    }

    public void setCreationTime(Date creationTime) {
        this.creationTime = new java.sql.Timestamp(creationTime.getTime());
    }

    public Date getLastModifiedTime() {
        return new Date(this.lastModifiedTime.getTime());
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTime = new java.sql.Timestamp(lastModifiedTime.getTime());
    }

    public Date getStartTime() {
        if (this.startTime != null) {
            return new Date(this.startTime.getTime());
        } else {
            return null;
        }
    }

    public void setStartTime(Date startTime) {
        if (startTime != null) {
            this.startTime = new java.sql.Timestamp(startTime.getTime());
        }
    }

    public Date getEndTime() {
        if (this.endTime != null) {
            return new Date(this.endTime.getTime());
        } else {
            return null;
        }
    }

    public void setEndTime(Date endTime) {
        if (endTime != null) {
            this.endTime = new java.sql.Timestamp(endTime.getTime());
        }
    }

    public int getFrequencyInSec() {
        return frequencyInSec;
    }

    public void setFrequencyInSec(int frequencyInSec) {
        this.frequencyInSec = frequencyInSec;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public long getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
    }

    public List<PolicyPropertiesBean> getCustomProperties() {
        return customProperties;
    }

    public void setCustomProperties(List<PolicyPropertiesBean> customProperties) {
        this.customProperties = customProperties;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getNotificationType() {
        return notificationType;
    }

    public void setNotificationType(String notificationType) {
        this.notificationType = notificationType;
    }

    public String getNotificationTo() {
        return notificationTo;
    }

    public void setNotificationTo(String notificationTo) {
        this.notificationTo = notificationTo;
    }

    public String getExecutionType() {
        return executionType;
    }

    public void setExecutionType(String executionType) {
        this.executionType = executionType;
    }

    public Date getRetirementTime() {
        if (retirementTime != null) {
            return new Date(retirementTime.getTime());
        } else {
            return null;
        }
    }

    public void setRetirementTime(Date retirementTime) {
        if (retirementTime != null) {
            this.retirementTime = new java.sql.Timestamp(retirementTime.getTime());
        }
    }

    public String getJobs() {
        return jobs;
    }

    public void setJobs(String jobs) {
        this.jobs = jobs;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public PolicyBean() {
    }

    public PolicyBean(String name) {
        this.name = name;
    }
}
