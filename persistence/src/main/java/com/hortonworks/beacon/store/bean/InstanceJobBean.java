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
    @Column(name = "\"offset\"")
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
