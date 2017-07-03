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
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.metrics.JobMetrics;
import com.hortonworks.beacon.metrics.JobMetricsHandler;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.metrics.util.ReplicationMetricsUtils;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hortonworks.beacon.replication.ReplicationUtils.getInstanceTrackingInfo;

/**
 * Abstract class for Replication.
 */
public abstract class InstanceReplication {
    private static final BeaconLog LOG = BeaconLog.getLog(InstanceReplication.class);

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

    private String getTrackingInfoAsJsonString(String jobId, String instanceId,
                                               ReplicationMetrics.JobType jobType,
                                               Map<String, Long> metrics) throws BeaconException {
        ReplicationMetrics replicationMetrics = new ReplicationMetrics();
        if (metrics != null) {
            replicationMetrics.updateReplicationMetricsDetails(jobId, jobType, metrics);
        } else {
            replicationMetrics.updateReplicationMetricsDetails(jobId, jobType, new HashMap<String, Long>());
        }
        return getTrackingInfoAsJsonString(instanceId, jobType, replicationMetrics);
    }

    private static String getTrackingInfoAsJsonString(String instanceId,
                                                      ReplicationMetrics.JobType jobType,
                                                      ReplicationMetrics replicationMetrics) throws BeaconException {
        String trackingInfo = getInstanceTrackingInfo(instanceId);

        if (StringUtils.isNotBlank(trackingInfo)) {
            List<ReplicationMetrics> metrics = ReplicationMetricsUtils.getListOfReplicationMetrics(trackingInfo);
            if (metrics.isEmpty()) {
                throw new BeaconException(MessageCode.COMM_010008.name(), "metrics");
            } else {
                switch (jobType) {
                    case MAIN:
                        // Only main job. Just update it.
                        return replicationMetrics.toJsonString();

                    case RECOVERY:
                        // Check if there is already an recovery job
                        if (metrics.size() > 1) {
                            metrics.remove(1);
                            metrics.add(replicationMetrics);
                        } else {
                            // Currently no recovery job in tracking info. Add it
                            metrics.add(replicationMetrics);
                        }
                        return ReplicationMetricsUtils.toJsonString(metrics);

                    default:
                        LOG.error("Current job type is not MAIN or RECOVERY");
                        break;
                }
            }
        }
        return replicationMetrics.toJsonString();
    }

    private void setReplicationMetrics(JobContext jobContext, String jobId, ReplicationMetrics.JobType jobType,
                                       Map<String, Long> metrics) throws BeaconException {
        try {
            ReplicationUtils.storeTrackingInfo(jobContext,
                    getTrackingInfoAsJsonString(jobId, jobContext.getJobInstanceId(), jobType, metrics));
        } catch (BeaconException e) {
            LOG.error("Exception occurred while storing replication metrics info: {}", e.getMessage());
            throw new BeaconException(e);
        }
    }

    protected void captureReplicationMetrics(Job job, ReplicationMetrics.JobType jobType,
                                             JobContext jobContext, ReplicationType replicationType,
                                             boolean isJobComplete) throws IOException, BeaconException {
        JobMetrics fsReplicationCounters = JobMetricsHandler.getMetricsType(replicationType);
        if (fsReplicationCounters != null) {
            fsReplicationCounters.obtainJobMetrics(job, isJobComplete);
            Map<String, Long> counters = fsReplicationCounters.getCountersMap();
            setReplicationMetrics(jobContext, getJob(job), jobType, counters);
        }
    }

    protected void captureMetricsPeriodically(ScheduledThreadPoolExecutor timer, final JobContext jobContext,
                                              final Job job, final ReplicationMetrics.JobType jobType,
                                              int replicationMetricsInterval)
            throws IOException, BeaconException {
        timer.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    captureReplicationMetrics(job, jobType, jobContext, ReplicationType.FS, false);
                } catch (IOException | BeaconException e) {
                    String errorMsg = "Exception occurred while populating metrics periodically:" + e.getMessage();
                    LOG.error(errorMsg);
                }
            }
        }, 0, replicationMetricsInterval, TimeUnit.SECONDS);
    }
}
