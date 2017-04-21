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

package com.hortonworks.beacon.replication;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import org.apache.hadoop.mapreduce.Job;

import java.util.Properties;

/**
 * Abstract class for Replication.
 */
public abstract class InstanceReplication {

    protected static final String DUMP_DIRECTORY = "dumpDirectory";
    public static final String INSTANCE_EXECUTION_STATUS = "instanceExecutionStatus";

    private ReplicationJobDetails details;
    private Properties properties;
    private InstanceExecutionDetails instanceExecutionDetails;


    public InstanceReplication(ReplicationJobDetails details) {
        this.details = details;
        this.properties = details.getProperties();
        this.instanceExecutionDetails = new InstanceExecutionDetails();
    }

    public Properties getProperties() {
        return properties;
    }

    public ReplicationJobDetails getDetails() {
        return details;
    }

    private InstanceExecutionDetails getInstanceExecutionDetails() {
        return instanceExecutionDetails;
    }

    protected void setInstanceExecutionDetails(JobContext jobContext, JobStatus jobStatus) throws BeaconException {
        setInstanceExecutionDetails(jobContext, jobStatus, jobStatus.name());
    }

    protected void setInstanceExecutionDetails(JobContext jobContext, JobStatus jobStatus,
                                               String message) throws BeaconException {
        setInstanceExecutionDetails(jobContext, jobStatus, message, null);
    }

    protected void setInstanceExecutionDetails(JobContext jobContext, JobStatus jobStatus,
                                               String message, Job job) throws BeaconException {
        getInstanceExecutionDetails().updateJobExecutionDetails(jobStatus.name(), message, getJob(job));
        jobContext.getJobContextMap().put(INSTANCE_EXECUTION_STATUS,
                getInstanceExecutionDetails().toJsonString());
    }

    protected String getJob(Job job) {
        return ((job != null) && (job.getJobID() != null)) ? job.getJobID().toString() : null;
    }
}
