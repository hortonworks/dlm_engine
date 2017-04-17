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

import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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

    /**
     * Arguments related to Policy instance execution.
     */
    public enum InstanceExecutionDetailsArgs {
        JOBID("jobId", "distcp job id"),
        JOBEXECUTIONTYPE("jobExecutionType", "Replication type"),
        JOBSTATUS("jobStatus", "Status of the executed job"),
        JOBMESSAGE("jobMessage", "Message from the executed job");

        private String name;
        private String description;
        InstanceExecutionDetailsArgs(String name, String description) {
            this.name = name;
            this.description = description;
        }
    };

    public InstanceExecutionDetails() {
    }

    public InstanceExecutionDetails(String jsonString)  {
        try {
            JSONObject object = new JSONObject(jsonString);
            this.jobId = object.getString(InstanceExecutionDetailsArgs.JOBID.name());
            this.jobStatus = object.getString(InstanceExecutionDetailsArgs.JOBSTATUS.name());
            this.jobMessage = object.getString(InstanceExecutionDetailsArgs.JOBMESSAGE.name());

        } catch (JSONException e) {
            LOG.error("Unable to deserialize JobExecutionDetails ", e);
        }
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
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

    public String toJsonString() throws BeaconException {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(InstanceExecutionDetailsArgs.JOBSTATUS.name(), getJobStatus());
            if (StringUtils.isNotBlank(getJobId())) {
                jsonObject.put(InstanceExecutionDetailsArgs.JOBID.name(), getJobId());
            }

            if (StringUtils.isNotBlank(getJobMessage())) {
                jsonObject.put(InstanceExecutionDetailsArgs.JOBMESSAGE.name(), getJobMessage());
            }

            LOG.info("JobExecutionDetails : {}", jsonObject.toString());
            return jsonObject.toString();
        } catch (JSONException e) {
            throw new BeaconException("Unable to serialize JobExecutionDetails ", e);
        }
    }

    public void updateJobExecutionDetails(String status, String message) {
        updateJobExecutionDetails(status, message, null);
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
