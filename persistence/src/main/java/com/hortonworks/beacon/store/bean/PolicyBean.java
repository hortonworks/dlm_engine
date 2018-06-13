/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
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
                + "where b.type = :policyType AND b.retirementTime IS NULL "
                + "AND b.status NOT IN ('SUCCEEDED', 'FAILED', 'SUCCEEDEDWITHSKIPPED', 'FAILEDWITHSKIPPED')"),
        @NamedQuery(name = "GET_PAIRED_CLUSTER_POLICY", query = "select COUNT(b.id) from PolicyBean b "
                + "where b.retirementTime IS NULL AND b.status IN ('RUNNING', 'SUBMITTED', 'SUSPENDED') AND ("
                + "(b.sourceCluster = :sourceCluster AND b.targetCluster = :targetCluster) OR "
                + "(b.sourceCluster = :targetCluster AND b.targetCluster = :sourceCluster) )"),
        @NamedQuery(name = "GET_CLUSTER_CLOUD_POLICY", query = "select COUNT(b.id) from PolicyBean b, "
                + "PolicyPropertiesBean pb where b.retirementTime IS NULL AND b.status IN"
                + " ('RUNNING', 'SUBMITTED', 'SUSPENDED') AND pb.policyId = b.id AND pb.value = :cloudCred"),
        @NamedQuery(name = "GET_POLICY_BY_ID", query = "select OBJECT(b) from PolicyBean b "
                + "where b.id = :id"),
        @NamedQuery(name = "GET_ARCHIVED_POLICY", query = "select OBJECT(b) from PolicyBean b "
                + "where b.name = :name AND b.retirementTime IS NOT NULL order by b.creationTime DESC"),
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
                + "where b.retirementTime < :retirementTime"),
        @NamedQuery(name = "UPDATE_FINAL_STATUS", query = "update PolicyBean b set b.status = :status, "
                + "b.lastModifiedTime = :lastModifiedTime where b.id = :id"),
        @NamedQuery(name = "UPDATE_POLICY_RETIREMENT", query = "update PolicyBean b "
                + "set b.retirementTime = :retirementTime where b.id = :id"),
        @NamedQuery(name = "GET_POLICY_RECOVERY", query = "select OBJECT(b) from PolicyBean b "
                + "where b.status = 'SUBMITTED'")
    })
public class PolicyBean {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "name")
    private String name;

    @Column(name = "description")
    private String description;

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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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
        return creationTime != null ? new Date(creationTime.getTime()) : null;
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
