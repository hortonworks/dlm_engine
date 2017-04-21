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

package com.hortonworks.beacon.job;

import com.google.gson.Gson;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to capture the Replication Policy instance execution details.
 */
public class InstanceExecutionDetails {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceExecutionDetails.class);

    private String jobId;
    private String jobStatus;
    private String jobMessage;

    public InstanceExecutionDetails() {
    }

    private String getJobId() {
        return jobId;
    }

    private void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String getJobMessage() {
        return jobMessage;
    }

    public void setJobMessage(String jobMessage) {
        this.jobMessage = jobMessage;
    }

    public InstanceExecutionDetails getInstanceExecutionDetails(String jsonString)  {
        Gson gson = new Gson();
        return gson.fromJson(jsonString, InstanceExecutionDetails.class);
    }

    public String toJsonString() throws BeaconException {
        Gson gson = new Gson();
        String jsonString =  gson.toJson(this);
        LOG.info("Instance Execution Details : {}", jsonString);
        return jsonString;
    }

    public void updateJobExecutionDetails(String status, String message, String distcpJob) {
        this.setJobStatus(status);
        this.setJobMessage(message);
        if (StringUtils.isNotBlank(distcpJob)) {
            this.setJobId(distcpJob);
        } else {
            this.setJobId("NA");
        }
    }

    @Override
    public String toString() {
        return "jobId:" + jobId
                + "\tjobStatus:'" + jobStatus
                + "\tjobMessage:'" + jobMessage;
    }
}
