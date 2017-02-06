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

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Date;

/**
 * Beacon for policy instances.
 */
@Entity
@Table(name = "policy_instance")
@NamedQueries({
        @NamedQuery(name = "UPDATE_JOB_INSTANCE", query = "update PolicyInstanceBean b "
                + "set b.jobExecutionType = :jobExecutionType, b.endTime = :endTime, b.status = :status,"
                + " b.duration= :duration, b.message = :message where b.id =: id "),
        @NamedQuery(name = "SELECT_JOB_INSTANCE", query = "select OBJECT(b) from PolicyInstanceBean b "
                + "where b.name = :name AND b.type = :type AND b.deleted = :deleted"),
        @NamedQuery(name ="SET_DELETED", query = "update PolicyInstanceBean b set b.id = :id_new, "
                + "b.deleted = :deleted " + "where b.id = :id")
        }
)
public class PolicyInstanceBean implements Serializable {

    @Id
    @Column (name = "id")
    private String id;

    @Basic
    @Column (name = "class_name")
    private String className;

    @Basic
    @Column (name = "name")
    private String name;

    @Basic
    @Column (name = "job_type")
    private String type;

    @Basic
    @Column (name = "job_execution_type")
    private String jobExecutionType;

    @Basic
    @Column (name = "start_time")
    private java.sql.Timestamp startTime;

    @Basic
    @Column (name = "end_time")
    private java.sql.Timestamp endTime;

    @Basic
    @Column (name = "frequency")
    private int frequency;

    @Basic
    @Column (name = "duration")
    private long duration;

    @Basic
    @Column (name = "status")
    private String status;

    @Basic
    @Column (name = "message")
    private String message;

    @Basic
    @Column (name = "deleted")
    private int deleted = 0;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

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

    public String getJobExecutionType() {
        return jobExecutionType;
    }

    public void setJobExecutionType(String jobExecutionType) {
        this.jobExecutionType = jobExecutionType;
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

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
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

    public int getDeleted() {
        return deleted;
    }

    public void setDeleted(int deleted) {
        this.deleted = deleted;
    }
}
