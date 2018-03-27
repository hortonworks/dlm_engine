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
        @NamedQuery(name = "UPDATE_INSTANCE_FAIL_RETIRE", query = "update PolicyInstanceBean b "
                + "set b.endTime = :endTime, b.status = :status, b.message = :message, "
                + "b.retirementTime = :retirementTime where b.instanceId =: instanceId"),
        @NamedQuery(name = "SELECT_POLICY_INSTANCE", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.policyId = :policyId AND b.retirementTime IS NULL"),
        @NamedQuery(name ="DELETE_POLICY_INSTANCE", query = "update PolicyInstanceBean b "
                + "set b.retirementTime = :retirementTime "
                + "where b.policyId = :policyId AND b.retirementTime IS NULL"),
        @NamedQuery(name = "UPDATE_CURRENT_OFFSET", query = "update PolicyInstanceBean b "
                + "set b.currentOffset = :currentOffset where b.instanceId = :instanceId"),
        @NamedQuery(name = "DELETE_RETIRED_INSTANCE", query = "delete from PolicyInstanceBean b "
                + "where b.retirementTime < :retirementTime"),
        @NamedQuery(name = "GET_INSTANCE_TRACKING_INFO", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.instanceId = :instanceId"),
        @NamedQuery(name = "UPDATE_INSTANCE_TRACKING_INFO", query = "update PolicyInstanceBean b "
                + "set b.trackingInfo = :trackingInfo where b.instanceId = :instanceId"),
        @NamedQuery(name = "SELECT_INSTANCE_RUNNING", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.status = :status AND b.retirementTime IS NULL"),
        @NamedQuery(name = "GET_INSTANCE_FAILED", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.policyId = :policyId AND b.status = :status order by b.endTime DESC"),
        @NamedQuery(name = "GET_INSTANCE_RECENT", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.policyId = :policyId order by b.startTime DESC"),
        @NamedQuery(name = "GET_INSTANCE_FOR_RERUN", query = "select b.instanceId, b.currentOffset, b.status "
                + "from PolicyInstanceBean b where b.policyId = :policyId AND b.status <> 'SKIPPED' "
                + "order by b.startTime DESC"),
        @NamedQuery(name = "GET_INSTANCE_BY_ID", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.instanceId = :instanceId"),
        @NamedQuery(name = "UPDATE_INSTANCE_RETRY_COUNT", query = "update PolicyInstanceBean b "
                + "set b.runCount = :runCount where b.instanceId = :instanceId"),
        @NamedQuery(name = "UPDATE_INSTANCE_STATUS", query = "update PolicyInstanceBean b "
                + "set b.status = :status where b.policyId = :policyId AND b.status = 'RUNNING'"),
        @NamedQuery(name = "UPDATE_INSTANCE_STATUS_RETIRE", query = "update PolicyInstanceBean b "
                + "set b.status = :status, b.retirementTime = :retirementTime "
                + "where b.instanceId = :instanceId and b.retirementTime IS NULL"),
        @NamedQuery(name = "UPDATE_INSTANCE_RERUN", query = "update PolicyInstanceBean b "
                + "set b.status = :status, b.endTime = :endTime, b.message = :message, b.runCount = b.runCount+1 "
                + "where b.instanceId = :instanceId"),
        @NamedQuery(name = "GET_INSTANCE_STATUS_RECENT", query = "select b.status, max (b.startTime) as startTime "
                + "from PolicyInstanceBean b "
                + "where b.policyId = :policyId group by b.status order by startTime DESC"),
        @NamedQuery(name = "GET_INSTANCE_REPORT", query = "select b.status, max(b.endTime) as endTime "
                + "from PolicyInstanceBean b "
                + "where b.policyId = :policyId AND b.status <> 'RUNNING' group by b.status order by endTime")
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
