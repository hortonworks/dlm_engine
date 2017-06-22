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

/**
 * Beacon for policy instances.
 */
@SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL", "UWF_UNWRITTEN_FIELD"})
@Entity
@Table(name = "BEACON_POLICY_INSTANCE")
@NamedQueries({
        @NamedQuery(name = "UPDATE_INSTANCE_COMPLETE", query = "update PolicyInstanceBean b "
                + "set b.endTime = :endTime, b.status = :status, b.message = :message "
                + "where b.instanceId =: instanceId"),
        @NamedQuery(name = "SELECT_POLICY_INSTANCE", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.policyId = :policyId AND b.retirementTime IS NULL"),
        @NamedQuery(name ="DELETE_POLICY_INSTANCE", query = "update PolicyInstanceBean b "
                + "set b.retirementTime = :retirementTime "
                + "where b.instanceId = :instanceId AND b.retirementTime IS NULL"),
        @NamedQuery(name = "UPDATE_CURRENT_OFFSET", query = "update PolicyInstanceBean b "
                + "set b.currentOffset = :currentOffset where b.instanceId = :instanceId"),
        @NamedQuery(name = "DELETE_RETIRED_INSTANCE", query = "delete from PolicyInstanceBean b "
                + "where b.retirementTime < :retirementTime"),
        @NamedQuery(name = "UPDATE_INSTANCE_TRACKING_INFO", query = "update PolicyInstanceBean b "
                + "set b.trackingInfo = :trackingInfo where b.instanceId = :instanceId"),
        @NamedQuery(name = "SELECT_INSTANCE_RUNNING", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.status = :status AND b.retirementTime IS NULL"),
        @NamedQuery(name = "GET_INSTANCE_FAILED", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.policyId = :policyId AND b.status = :status order by b.endTime DESC"),
        @NamedQuery(name = "GET_INSTANCE_RECENT", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.policyId = :policyId order by b.startTime DESC"),
        @NamedQuery(name = "GET_INSTANCE_BY_ID", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.instanceId = :instanceId"),
        @NamedQuery(name = "UPDATE_INSTANCE_RETRY_COUNT", query = "update PolicyInstanceBean b "
                + "set b.runCount = :runCount where b.instanceId = :instanceId"),
        @NamedQuery(name = "UPDATE_INSTANCE_STATUS", query = "update PolicyInstanceBean b "
                + "set b.status = :status where b.policyId = :policyId AND b.status = 'RUNNING'")
        }
)
public class PolicyInstanceBean {

    @Id
    @Column (name = "id")
    private String instanceId;

    @Column (name = "policy_id")
    private String policyId;

    @Column (name = "start_time")
    private java.sql.Timestamp startTime;

    @Column (name = "end_time")
    private java.sql.Timestamp endTime;

    @Column (name = "status")
    private String status;

    @Column (name = "message")
    private String message;

    @Column(name = "retirement_time")
    private java.sql.Timestamp retirementTime;

    @Column(name = "run_count")
    private int runCount;

    @Column(name = "current_offset")
    private int currentOffset;

    @Column(name = "tracking_info")
    private String trackingInfo;

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String id) {
        this.instanceId = id;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public java.sql.Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = new java.sql.Timestamp(startTime.getTime());
    }

    public java.sql.Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = new java.sql.Timestamp(endTime.getTime());
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public java.sql.Timestamp getRetirementTime() {
        return retirementTime;
    }

    public void setRetirementTime(Date retirementTime) {
        if (retirementTime != null) {
            this.retirementTime = new java.sql.Timestamp(retirementTime.getTime());
        }
    }

    public int getRunCount() {
        return runCount;
    }

    public void setRunCount(int runCount) {
        this.runCount = runCount;
    }

    public int getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(int currentOffset) {
        this.currentOffset = currentOffset;
    }

    public String getTrackingInfo() {
        return trackingInfo;
    }

    public void setTrackingInfo(String trackingInfo) {
        this.trackingInfo = trackingInfo;
    }

    public PolicyInstanceBean() {
    }

    public PolicyInstanceBean(String instanceId) {
        this.instanceId = instanceId;
    }
}
