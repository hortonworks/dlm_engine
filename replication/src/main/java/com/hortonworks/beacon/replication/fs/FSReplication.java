/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ScheduledThreadPoolExecutor;
/**
 * FileSystem Replication implementation.
 */
public abstract class FSReplication extends InstanceReplication {

    private static final Logger LOG = LoggerFactory.getLogger(FSReplication.class);

    private static final int MAX_JOB_RETRIES = 10;

    protected boolean isSnapshot;
    protected String sourceStagingUri;
    protected String targetStagingUri;
    protected Job job;

    FSReplication(ReplicationJobDetails details) {
        super(details);
        isSnapshot = false;
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        try {
            initializeProperties();
            initializeFileSystem();
            String sourceDataset = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
            String targetDataset = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
            sourceStagingUri = FSUtils.getStagingUri(properties.getProperty(FSDRProperties.SOURCE_NN.getName()),
                    sourceDataset);
            targetStagingUri = FSUtils.getStagingUri(properties.getProperty(FSDRProperties.TARGET_NN.getName()),
                    targetDataset);
        } catch (Exception e) {
            throw new BeaconException("Exception occurred in init: ", e);
        }
    }

    protected Job performCopy(JobContext jobContext, DistCpOptions options, Configuration conf,
                              ReplicationMetrics.JobType jobType) throws BeaconException, InterruptedException {
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        try {
            LOG.info("Started DistCp with source path: {} target path: {}", sourceStagingUri, targetStagingUri);
            DistCp distCp = new DistCp(conf, options);
            if (jobContext.shouldInterrupt().get()) {
                throw new InterruptedException("before job submit");
            }

            job = distCp.createAndSubmitJob();
            LOG.info("DistCp Hadoop job: {} for policy instance: [{}]", getJob(job), jobContext.getJobInstanceId());
            handlePostSubmit(timer, jobContext, jobType);
        } catch (InterruptedException | BeaconException e) {
            throw e;
        } catch (Exception e) {
            throw new BeaconException(e);
        } finally {
            timer.shutdown();
            captureFSReplicationMetrics(job, jobType, jobContext, true);
        }
        return job;
    }

    protected void handlePostSubmit(ScheduledThreadPoolExecutor timer, JobContext jobContext,
                                  ReplicationMetrics.JobType jobType) throws Exception {
        if (jobContext.shouldInterrupt().get()) {
            throw new InterruptedException("after job submit");
        }

        getFSReplicationProgress(timer, jobContext, job, jobType,
                ReplicationUtils.getReplicationMetricsInterval());
        if (!waitForCompletion(jobContext)) {
            JobStatus status = job.getStatus();
            throw new IOException("Job " + job.getJobID() + " failed with state " + status.getState()
                    + " due to: " + status.getFailureInfo());
        }
    }

    public boolean waitForCompletion(JobContext jobContext) throws IOException, InterruptedException {
        int completionPollIntervalMillis = job.getCompletionPollInterval(new Configuration());
        while (!job.isComplete()) {
            if (jobContext.shouldInterrupt().get()) {
                throw new InterruptedException("during job in progress");
            }
            try {
                Thread.sleep(completionPollIntervalMillis);
            } catch (InterruptedException ie) {
                throw ie;
            }
        }
        return job.isSuccessful();
    }

    JobClient getJobClient() throws BeaconException {
        try {
            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
            return loginUser.doAs(new PrivilegedExceptionAction<JobClient>() {
                public JobClient run() throws Exception {
                    return new JobClient(new JobConf());
                }
            });
        } catch (InterruptedException | IOException e) {
            throw new BeaconException("Exception creating job client: ", e);
        }
    }

    RunningJob getJobWithRetries(String jobId) throws BeaconException {
        RunningJob runningJob = null;
        if (jobId != null) {
            int hadoopJobLookupRetries = BeaconConfig.getInstance().getEngine().getHadoopJobLookupRetries();
            int hadoopJobLookupDelay = BeaconConfig.getInstance().getEngine().getHadoopJobLookupDelay();
            int retries = 0;
            int maxJobRetries = hadoopJobLookupRetries > MAX_JOB_RETRIES ? MAX_JOB_RETRIES : hadoopJobLookupRetries;
            while (retries++ < maxJobRetries) {
                LOG.info("Trying to get job [{}], attempt [{}]", jobId, retries);
                try {
                    runningJob = getJobClient().getJob(JobID.forName(jobId));
                } catch (IOException ioe) {
                    throw new BeaconException(ioe);
                }

                if (runningJob != null) {
                    break;
                }

                try {
                    Thread.sleep(hadoopJobLookupDelay * 1000);
                } catch (InterruptedException e) {
                    // Do nothing
                }
            }
        }
        return runningJob;
    }

    public void cleanUp(JobContext jobContext) {
        synchronized (this) {
            if (job != null) {
                LOG.debug("Closing the job {}" + job.getJobName());
                close(job);
            }
            job = null;
        }
    }

    @Override
    public void interrupt() throws BeaconException {
        try {
            if (job != null) {
                synchronized (this) {
                    if (job != null && job.getJobState() == JobStatus.State.RUNNING) {
                        LOG.error("Replication job: {} interrupted, killing it.", getJob(job));
                        job.killJob();
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new BeaconException(e);
        }
    }
}
