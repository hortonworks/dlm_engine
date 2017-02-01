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
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobExecutionDetails {
    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionDetails.class);

    private String jobId;
    private String jobExecutionType;
    private String jobStatus;

    public static enum JobExecutionDetailsArgs {
        JOBID("jobId", "distcp job id"),
        JOBEXECUTIONTYPE("jobExecutionType", "Replication type"),
        JOBSTATUS("jobStatus", "Status of the executed job");

        String name;
        String description;
        JobExecutionDetailsArgs(String name, String description) {
            this.name = name;
            this.description = description;
        }
    };


    public JobExecutionDetails() {
    }

    public JobExecutionDetails (String jsonString)  {
        try {
            JSONObject object = new JSONObject(jsonString);
            this.jobId = object.getString(JobExecutionDetailsArgs.JOBID.name());
            this.jobExecutionType = object.getString(JobExecutionDetailsArgs.JOBEXECUTIONTYPE.name());
            this.jobStatus = object.getString(JobExecutionDetailsArgs.JOBSTATUS.name());

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

    public String getJobExecutionType() {
        return jobExecutionType;
    }

    public void setJobExecutionType(String jobExecutionType) {
        this.jobExecutionType = jobExecutionType;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String toJsonString() throws BeaconException {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(JobExecutionDetailsArgs.JOBSTATUS.name(), getJobStatus());
            if (StringUtils.isNotBlank(getJobId())) {
                jsonObject.put(JobExecutionDetailsArgs.JOBID.name(), getJobId());
            }

            if (StringUtils.isNotBlank(getJobExecutionType())) {
                jsonObject.put(JobExecutionDetailsArgs.JOBEXECUTIONTYPE.name(), getJobExecutionType());
            }

            LOG.info("JobExecutionDetails : {}"+jsonObject.toString());
            return jsonObject.toString();
        } catch (JSONException e) {
            throw new BeaconException("Unable to serialize JobExecutionDetails ", e);
        }
    }


    @Override
    public String toString() {
        return "jobId:" + jobId +
                "\tjobExecutionType:'" + jobExecutionType +
                "\tjobStatus:'" + jobStatus;
    }
}