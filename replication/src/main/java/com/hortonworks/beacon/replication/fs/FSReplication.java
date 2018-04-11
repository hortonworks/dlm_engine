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
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * FileSystem Replication implementation.
 */
public class FSReplication extends InstanceReplication {

    private static final Logger LOG = LoggerFactory.getLogger(FSReplication.class);

    private static final int MAX_JOB_RETRIES = 10;
    protected FileSystem sourceFs;
    protected FileSystem targetFs;

    FSReplication(ReplicationJobDetails details) {
        super(details);
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

    String getFSReplicationName(boolean isSnapshot, FileSystem fileSystem, String sourceStagingUri)
            throws BeaconException {
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(properties.getProperty(
                FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        LOG.debug("TDE encryption enabled: {}", tdeEncryptionEnabled);
        // check if source and target path's exist and are snapshot-able
        String fsReplicationName = null;
        if (!tdeEncryptionEnabled) {
            if (isSnapshot
                    && properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()) != null) {
                fsReplicationName = FSSnapshotUtils.getSnapshotName(getDetails().getName());
                FSSnapshotUtils.handleSnapshotCreation(fileSystem, sourceStagingUri, fsReplicationName);
            }
        }
        return fsReplicationName;
    }

    public void cleanUp(JobContext jobContext) throws BeaconException {
        try {
            if (sourceFs != null) {
                sourceFs.close();
            }
            if (targetFs != null) {
                targetFs.close();
            }
        } catch (Exception e) {
            throw new BeaconException("Exception occurred while closing FileSystem: ", e);
        }
    }

    void checkJobInterruption(JobContext jobContext, Job job) throws BeaconException {
        if (job != null) {
            try {
                LOG.error("Replication job: {} interrupted, killing it.", getJob(job));
                job.killJob();
                setInstanceExecutionDetails(jobContext, JobStatus.KILLED, "job killed", job);
            } catch (IOException ioe) {
                LOG.error(ioe.getMessage(), ioe);
            }
        }
    }
}
