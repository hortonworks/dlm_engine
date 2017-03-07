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
import javax.persistence.Table;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Policy instance job bean.
 */
@Entity
@Table(name = "BEACON_INSTANCE_JOB")
@IdClass(InstanceJobKey.class)
public class InstanceJobBeacon {

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

    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        if (startTime != null) {
            this.startTime = new java.sql.Timestamp(startTime.getTime());
        }
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Timestamp endTime) {
        this.endTime = endTime;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Timestamp getRetirementTime() {
        return retirementTime;
    }

    public void setRetirementTime(Timestamp retirementTime) {
        this.retirementTime = retirementTime;
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
}
