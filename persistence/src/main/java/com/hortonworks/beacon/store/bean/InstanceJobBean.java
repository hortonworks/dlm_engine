/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.bean;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.util.Date;

/**
 * Policy instance job bean.
 */
@SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL", "UWF_UNWRITTEN_FIELD"})
@Entity
@Table(name = "BEACON_INSTANCE_JOB")
@IdClass(InstanceJobKey.class)
@NamedQueries({
        @NamedQuery(name = "GET_INSTANCE_JOB", query = "select OBJECT(b) from InstanceJobBean b "
                + " where b.instanceId = :instanceId AND b.offset = :offset"),
        @NamedQuery(name = "UPDATE_STATUS_START", query = "update InstanceJobBean b set b.status = :status, "
                + "b.startTime = :startTime where b.instanceId = :instanceId AND b.offset = :offset"),
        @NamedQuery(name = "INSTANCE_JOB_UPDATE_STATUS", query = "update InstanceJobBean b set b.status = :status "
                + "where b.instanceId = :instanceId AND b.endTime IS NULL"),
        @NamedQuery(name = "INSTANCE_JOB_REMAIN_RETIRE", query = "update InstanceJobBean b set b.status = :status, "
                + "b.retirementTime = :retirementTime  where b.instanceId = :instanceId AND b.endTime IS NULL"),
        @NamedQuery(name = "UPDATE_JOB_COMPLETE", query = "update InstanceJobBean b set b.status = :status, "
                + "b.message = :message, b.endTime = :endTime, b.contextData = :contextData "
                + "where b.instanceId = :instanceId AND b.offset = :offset"),
        @NamedQuery(name = "UPDATE_JOB_FAIL_RETIRE", query = "update InstanceJobBean b set b.status = :status, "
                + "b.message = :message, b.endTime = :endTime, b.contextData = :contextData, "
                + "b.retirementTime = :retirementTime where b.instanceId = :instanceId AND b.offset = :offset"),
        @NamedQuery(name = "UPDATE_JOB_RETRY_COUNT", query = "update InstanceJobBean b set b.runCount = :runCount "
                + "where b.instanceId = :instanceId AND b.offset = :offset"),
        @NamedQuery(name = "DELETE_INSTANCE_JOB", query = "update InstanceJobBean b "
                + "set b.retirementTime = :retirementTime "
                + "where b.instanceId = :instanceId AND b.retirementTime IS NULL"),
        @NamedQuery(name = "DELETE_RETIRED_JOBS", query = "delete from InstanceJobBean b "
                + "where b.retirementTime < :retirementTime")
    })
public class InstanceJobBean {

    @Id
    @Column(name = "instance_id")
    private String instanceId;

    @Id
    @Column(name = "offset")
    private int offset;

    @Column(name = "status")
    private String status;

    @Column(name = "start_time")
    private java.sql.Timestamp startTime;

    @Column(name = "end_time")
    private java.sql.Timestamp endTime;

    @Column(name = "message")
    private String message;

    @Column(name = "retirement_time")
    private java.sql.Timestamp retirementTime;

    @Column(name = "run_count")
    private int runCount;

    @Column(name = "context_data")
    private String contextData;

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getStartTime() {
        if (startTime != null) {
            return new Date(startTime.getTime());
        }
        return null;
    }

    public void setStartTime(Date startTime) {
        if (startTime != null) {
            this.startTime = new java.sql.Timestamp(startTime.getTime());
        }
    }

    public Date getEndTime() {
        if (endTime != null) {
            return new Date(endTime.getTime());
        }
        return null;
    }

    public void setEndTime(Date endTime) {
        if (endTime != null) {
            this.endTime = new java.sql.Timestamp(endTime.getTime());
        }
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Date getRetirementTime() {
        if (retirementTime != null) {
            return new Date(retirementTime.getTime());
        }
        return null;
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

    public String getContextData() {
        return contextData;
    }

    public void setContextData(String contextData) {
        this.contextData = contextData;
    }

    public InstanceJobBean() {
    }

    public InstanceJobBean(String instanceId, int offset) {
        this.instanceId = instanceId;
        this.offset = offset;
    }
}
