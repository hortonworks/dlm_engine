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
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * Bean for job chaining.
 */
@Entity
@Table(name = "chained_jobs")
@NamedQueries({
        @NamedQuery(name = "GET_SECOND_JOB", query = "select OBJECT(b) from ChainedJobsBean b "
                + "where b.firstJobName = :firstJobName AND b.firstJobGroup = :firstJobGroup")
    })
public class ChainedJobsBean implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private long id;

    @Basic
    @Column(name = "first_job_name")
    private String firstJobName = null;

    @Basic
    @Column(name = "first_job_group")
    private String firstJobGroup = null;

    @Basic
    @Column(name = "second_job_name")
    private String secondJobName = null;

    @Basic
    @Column(name = "second_job_group")
    private String secondJobGroup = null;

    @Basic
    @Column(name = "created_time")
    private long createdTime = 0;

    public ChainedJobsBean() {
    }

    public ChainedJobsBean(String firstJobName, String firstJobGroup, String secondJobName, String secondJobGroup) {
        this.firstJobName = firstJobName;
        this.firstJobGroup = firstJobGroup;
        this.secondJobName = secondJobName;
        this.secondJobGroup = secondJobGroup;
        createdTime = System.currentTimeMillis();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getFirstJobName() {
        return firstJobName;
    }

    public void setFirstJobName(String firstJobName) {
        this.firstJobName = firstJobName;
    }

    public String getFirstJobGroup() {
        return firstJobGroup;
    }

    public void setFirstJobGroup(String firstJobGroup) {
        this.firstJobGroup = firstJobGroup;
    }

    public String getSecondJobName() {
        return secondJobName;
    }

    public void setSecondJobName(String secondJobName) {
        this.secondJobName = secondJobName;
    }

    public String getSecondJobGroup() {
        return secondJobGroup;
    }

    public void setSecondJobGroup(String secondJobGroup) {
        this.secondJobGroup = secondJobGroup;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }
}
