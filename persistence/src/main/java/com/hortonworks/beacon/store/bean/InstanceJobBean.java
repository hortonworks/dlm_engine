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
        @NamedQuery(name = "UPDATE_JOB_COMPLETE", query = "update InstanceJobBean b set b.status = :status, "
                + "b.message = :message, b.endTime = :endTime, b.contextData = :contextData "
                + "where b.instanceId = :instanceId AND b.offset = :offset"),
        @NamedQuery(name = "DELETE_INSTANCE_JOB", query = "update InstanceJobBean b "
                + "set b.status = :status, b.retirementTime = :retirementTime "
                + "where b.instanceId = :instanceId AND b.retirementTime IS NULL")
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
