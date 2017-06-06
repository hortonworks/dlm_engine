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

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * FileSystem Replication implementation.
 */
public class FSReplication extends InstanceReplication implements BeaconJob {

    private static final BeaconLog LOG = BeaconLog.getLog(FSReplication.class);

    private String sourceStagingUri;
    private String targetStagingUri;
    private FileSystem sourceFs;
    private FileSystem targetFs;
    private boolean isSnapshot;
    private boolean isHCFS;

    public FSReplication(ReplicationJobDetails details) {
        super(details);
        isSnapshot = false;
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        BeaconLogUtils.setLogInfo(jobContext.getJobInstanceId());
        String sourceDataset = getProperties().getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetDataset = getProperties().getProperty(FSDRProperties.TARGET_DATASET.getName());

        try {
            initializeFileSystem();
            sourceStagingUri = FSUtils.getStagingUri(getProperties().getProperty(FSDRProperties.SOURCE_NN.getName()),
                    sourceDataset);
            targetStagingUri = FSUtils.getStagingUri(getProperties().getProperty(FSDRProperties.TARGET_NN.getName()),
                    targetDataset);

            if (FSUtils.isHCFS(new Path(sourceStagingUri)) || FSUtils.isHCFS(new Path(targetStagingUri))) {
                isHCFS = true;
            }
        } catch (BeaconException e) {
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
            cleanUp(jobContext);
            throw new BeaconException("Exception occurred in init : {}", e);
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        Job job = null;
        try {
            Properties fsDRProperties = getProperties();
            String fsReplicationName = getFSReplicationName(fsDRProperties);
            job = performCopy(jobContext, fsDRProperties, fsReplicationName);
            if (job == null) {
                throw new BeaconException("FS Replication job is null");
            }
            performPostReplJobExecution(jobContext, job, fsDRProperties, fsReplicationName);
        } catch (InterruptedException e) {
            cleanUp(jobContext);
            throw new BeaconException(e);
        } catch (Exception e) {
            LOG.error("Exception occurred in FS Replication: {}", e.getMessage());
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    Job performCopy(JobContext jobContext, Properties fsDRProperties,
                    String toSnapshot) throws BeaconException, InterruptedException {
        return performCopy(jobContext, fsDRProperties, toSnapshot,
                getLatestSnapshotOnTargetAvailableOnSource(), false);
    }

    Job performCopy(JobContext jobContext, Properties fsDRProperties, String toSnapshot, String fromSnapshot,
                    boolean isInRecoveryMode) throws BeaconException, InterruptedException {
        Job job = null;
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        try {
            DistCpOptions options = getDistCpOptions(fsDRProperties, toSnapshot,
                    fromSnapshot, isInRecoveryMode);

            options.setMaxMaps(Integer.parseInt(fsDRProperties.getProperty(
                    FSDRProperties.DISTCP_MAX_MAPS.getName())));
            options.setMapBandwidth(Integer.parseInt(fsDRProperties.getProperty(
                    FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())));

            LOG.info("Started DistCp with source Path: {}  target path: {}", sourceStagingUri, targetStagingUri);

            DistCp distCp = new DistCp(new Configuration(), options);
            job = distCp.createAndSubmitJob();
            LOG.info("DistCp Hadoop job: {} for policy instance: [{}]", getJob(job), jobContext.getJobInstanceId());
            ReplicationUtils.storeTrackingInfo(jobContext, getJsonString(getJob(job)));
            captureMetricsPeriodically(timer, jobContext, job, ReplicationUtils.getReplicationMetricsInterval());
            /* TODO- Handle storing tracking infor for recovery jobs */
            if (!isInRecoveryMode) {
                ReplicationUtils.storeTrackingInfo(jobContext, getJob(job));
            }
            distCp.waitForJobCompletion(job);
        } catch (InterruptedException e) {
            checkJobInterruption(jobContext, job);
            throw e;
        } catch (Exception e) {
            LOG.error("Exception occurred while performing copying of data : {}", e.getMessage());
            throw new BeaconException(e);
        } finally {
            timer.shutdown();
        }
        return job;
    }

    private String getLatestSnapshotOnTargetAvailableOnSource() throws BeaconException {
        String fromSnapshot = null;

        try {
            LOG.info("Checking Snapshot directory on Source and Target");
            if (isSnapshot && targetFs.exists(new Path(targetStagingUri))) {
                fromSnapshot = FSSnapshotUtils.findLatestReplicatedSnapshot((DistributedFileSystem) sourceFs,
                        (DistributedFileSystem) targetFs, sourceStagingUri, targetStagingUri);
            }
        } catch (IOException e) {
            String msg = "Error occurred when checking target dir : {} exists " + targetStagingUri;
            LOG.error(msg);
            throw new BeaconException(msg);
        }
        return fromSnapshot;
    }


    private void initializeFileSystem() throws BeaconException {
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            sourceFs = FSUtils.getFileSystem(getProperties().getProperty(
                    FSDRProperties.SOURCE_NN.getName()), conf, isHCFS);
            targetFs = FSUtils.getFileSystem(getProperties().getProperty(
                    FSDRProperties.TARGET_NN.getName()), conf, isHCFS);
        } catch (BeaconException e) {
            LOG.error("Exception occurred while initializing DistributedFileSystem:" + e);
            throw new BeaconException(e.getMessage());
        }
    }

    private String getFSReplicationName(Properties fsDRProperties)
            throws BeaconException {
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(fsDRProperties.getProperty(
                FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        LOG.info("TDE Encryption enabled : {}", tdeEncryptionEnabled);
        // check if source and target path's exist and are snapshot-able
        String fsReplicationName = fsDRProperties.getProperty(FSDRProperties.JOB_NAME.getName())
                + "-" + System.currentTimeMillis();
        if (!tdeEncryptionEnabled) {
            if (fsDRProperties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && fsDRProperties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()) != null
                    && fsDRProperties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && fsDRProperties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()) != null) {
                try {
                    isSnapshot = FSSnapshotUtils.isDirectorySnapshottable(sourceFs, targetFs,
                            sourceStagingUri, targetStagingUri);
                    if (isSnapshot) {
                        fsReplicationName = FSSnapshotUtils.SNAPSHOT_PREFIX
                                + fsDRProperties.getProperty(FSDRProperties.JOB_NAME.getName())
                                + "-" + System.currentTimeMillis();
                        FSSnapshotUtils.handleSnapshotCreation(sourceFs, sourceStagingUri, fsReplicationName);
                    }
                } catch (BeaconException e) {
                    throw new BeaconException(e);
                }
            }
        }
        return fsReplicationName;
    }

    private DistCpOptions getDistCpOptions(Properties fsDRProperties,
                                           String toSnapshot, String fromSnapshot,
                                           boolean isInRecoveryMode)
            throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths

        List<Path> sourceUris = new ArrayList<>();
        sourceUris.add(new Path(sourceStagingUri));

        return DistCpOptionsUtil.getDistCpOptions(fsDRProperties, sourceUris, new Path(targetStagingUri),
                isSnapshot, fromSnapshot, toSnapshot, isInRecoveryMode);
    }

    private void performPostReplJobExecution(JobContext jobContext, Job job, Properties fsDRProperties,
                                             String fsReplicationName) throws IOException, BeaconException {
        if (job.isComplete() && job.isSuccessful()) {
            if (isSnapshot) {
                try {
                    FSSnapshotUtils.handleSnapshotCreation(targetFs, targetStagingUri, fsReplicationName);
                    FSSnapshotUtils.handleSnapshotEviction(sourceFs, fsDRProperties, sourceStagingUri);
                    FSSnapshotUtils.handleSnapshotEviction(targetFs, fsDRProperties, targetStagingUri);
                } catch (BeaconException e) {
                    throw new BeaconException("Exception occurred while handling snapshot : {}", e);
                }
            }
            LOG.info("Distcp Copy is successful");
            captureReplicationMetrics(job, jobContext, ReplicationType.FS, true);
            setInstanceExecutionDetails(jobContext, JobStatus.SUCCESS, JobStatus.SUCCESS.name(), job);
        } else {
            String message = "Job exception occurred:" + getJob(job);
            throw new BeaconException(message);
        }
    }

    private void checkJobInterruption(JobContext jobContext, Job job) throws BeaconException {
        if (job != null) {
            try {
                LOG.error("replication job: {} interrupted, killing it.", getJob(job));
                job.killJob();
                setInstanceExecutionDetails(jobContext, JobStatus.KILLED, "job killed", job);
            } catch (IOException ioe) {
                LOG.error(ioe.getMessage(), ioe);
            }
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
        try {
            sourceFs.close();
            targetFs.close();
        } catch (IOException e) {
            throw new BeaconException("Exception occurred while closing FileSystem : {}", e);
        }
    }

    @Override
    public void recover(JobContext jobContext) throws BeaconException {
        if (!isSnapshot) {
            LOG.info("policy instance: [{}] not snapshottable, return", jobContext.getJobInstanceId());
            return;
        }
        LOG.info("recover policy instance: [{}]", jobContext.getJobInstanceId());

        // Current state on the target cluster
        String toSnapshot = ".";
        String fromSnapshot = getLatestSnapshotOnTargetAvailableOnSource();
        if (StringUtils.isBlank(fromSnapshot)) {
            LOG.info("replicatedSnapshotName is null. No recovery needed for policy instance: [{}], return",
                    jobContext.getJobInstanceId());
            return;
        }

        Job job = null;
        try {
            SnapshotDiffReport diffReport = ((DistributedFileSystem) targetFs).getSnapshotDiffReport(
                    new Path(targetStagingUri), fromSnapshot, toSnapshot);
            List diffList = diffReport.getDiffList();
            if (diffList == null || diffList.isEmpty()) {
                LOG.info("No recovery needed for policy instance: [{}], return", jobContext.getJobInstanceId());
                return;
            }
            LOG.info("Recovery needed for policy instance: [{}]. Start recovery!", jobContext.getJobInstanceId());
            Properties fsDRProperties = getProperties();
            try {
                job = performCopy(jobContext, fsDRProperties, toSnapshot, fromSnapshot, true);
            } catch (InterruptedException e) {
                cleanUp(jobContext);
                throw new BeaconException(e);
            }
            if (job == null) {
                throw new BeaconException("FS Replication recovery job is null");
            }
        } catch (Exception e) {
            String msg = "Error occurred when getting diff report for target dir: " + targetStagingUri + ", "
                    + "fromSnapshot: " + fromSnapshot + " & toSnapshot: " + toSnapshot;
            LOG.error(msg);
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            throw new BeaconException(msg, e);
        }
    }
}
