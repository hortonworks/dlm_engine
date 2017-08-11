/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.job;

import com.google.gson.Gson;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;

import org.apache.commons.lang3.StringUtils;

/**
 * Class to capture the Replication Policy instance execution details.
 */
public class InstanceExecutionDetails {
    private static final BeaconLog LOG = BeaconLog.getLog(InstanceExecutionDetails.class);

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

    public static InstanceExecutionDetails getInstanceExecutionDetails(String jsonString)  {
        Gson gson = new Gson();
        return gson.fromJson(jsonString, InstanceExecutionDetails.class);
    }

    public String toJsonString() throws BeaconException {
        Gson gson = new Gson();
        String jsonString =  gson.toJson(this);
        LOG.info(MessageCode.COMM_000038.name(), jsonString);
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
