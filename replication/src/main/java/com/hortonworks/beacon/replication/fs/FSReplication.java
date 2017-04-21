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
import com.hortonworks.beacon.replication.InstanceReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FileSystem Replication implementation.
 */
public class FSReplication extends InstanceReplication implements BeaconJob {

    private static final Logger LOG = LoggerFactory.getLogger(FSReplication.class);

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
            CommandLine cmd = FSReplicationOptionsUtils.getCommand(getProperties());
            String fsReplicationName = getFSReplicationName(cmd);
            job = performCopy(jobContext, cmd, fsReplicationName);
            if (job == null) {
                throw new BeaconException("FS Replication job is null");
            }
            performPostReplJobExecution(jobContext, job, cmd, fsReplicationName);
        } catch (Exception e) {
            LOG.error("Exception occurred in FS Replication: {}", e.getMessage());
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            cleanUp(jobContext);
            throw new BeaconException(e);
        }
    }

    Job performCopy(JobContext jobContext, CommandLine cmd, String fSReplicationName) throws BeaconException {
        Configuration conf = new Configuration();
        Job job = null;
        try {
            DistCpOptions options = getDistCpOptions(conf, cmd, fSReplicationName);

            options.setMaxMaps(Integer.parseInt(cmd.getOptionValue(
                    FSDRProperties.DISTCP_MAX_MAPS.getName())));
            options.setMapBandwidth(Integer.parseInt(cmd.getOptionValue(
                    FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())));

            LOG.info("Started DistCp with source Path: {}  target path: {}", sourceStagingUri, targetStagingUri);

            DistCp distCp = new DistCp(conf, options);
            job = distCp.createAndSubmitJob();
            //TODO provide job context to handle the interruption between submission and waiting.
            distCp.waitForJobCompletion(job);
            LOG.info("DistCp Hadoop job: {}", getJob(job));
        } catch (InterruptedException e) {
            checkJobInterruption(jobContext, job);
            throw new BeaconException(e);
        } catch (Exception e) {
            LOG.error("Exception occurred while performing copying of data : {}", e.getMessage());
            throw new BeaconException(e);
        }
        return job;
    }


    private void initializeFileSystem() throws BeaconException {
        try {
            sourceFs = FSUtils.getFileSystem(getProperties().getProperty(
                    FSDRProperties.SOURCE_NN.getName()), new Configuration(), isHCFS);
            targetFs = FSUtils.getFileSystem(getProperties().getProperty(
                    FSDRProperties.TARGET_NN.getName()), new Configuration(), isHCFS);
        } catch (BeaconException e) {
            LOG.error("Exception occurred while initializing DistributedFileSystem:" + e);
            throw new BeaconException(e.getMessage());
        }
    }

    private String getFSReplicationName(CommandLine cmd)
            throws BeaconException {
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(cmd.getOptionValue(
                FSSnapshotUtils.TDE_ENCRYPTION_ENABLED));
        LOG.info("TDE Encryption enabled : {}", tdeEncryptionEnabled);
        // check if source and target path's exist and are snapshot-able
        String fsReplicationName = getProperties().getProperty(FSDRProperties.JOB_NAME.getName())
                + "-" + System.currentTimeMillis();
        if (!tdeEncryptionEnabled) {
            if (getProperties().getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && getProperties().getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()) != null
                    && getProperties().getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && getProperties().getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()) != null) {
                try {
                    isSnapshot = FSSnapshotUtils.isDirectorySnapshottable(sourceFs, targetFs,
                            sourceStagingUri, targetStagingUri);
                    if (isSnapshot) {
                        fsReplicationName = FSSnapshotUtils.SNAPSHOT_PREFIX
                                + getProperties().getProperty(FSDRProperties.JOB_NAME.getName())
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

    private DistCpOptions getDistCpOptions(Configuration conf, CommandLine cmd, String fsReplicationName)
            throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths

        List<Path> sourceUris = new ArrayList<>();
        sourceUris.add(new Path(sourceStagingUri));

        String replicatedSnapshotName = null;

        try {
            LOG.info("Checking Snapshot directory on Source and Target");
            if (isSnapshot && targetFs.exists(new Path(targetStagingUri))) {
                replicatedSnapshotName = FSSnapshotUtils.findLatestReplicatedSnapshot((DistributedFileSystem) sourceFs,
                        (DistributedFileSystem) targetFs, sourceStagingUri, targetStagingUri);
            }
        } catch (IOException e) {
            String msg = "Error occurred when checking target dir : {} exists " +targetStagingUri;
            LOG.error(msg);
            throw new BeaconException(msg);
        }

        return DistCpOptionsUtil.getDistCpOptions(cmd, sourceUris, new Path(targetStagingUri),
                isSnapshot, replicatedSnapshotName, fsReplicationName, conf);
    }

    private void performPostReplJobExecution(JobContext jobContext, Job job, CommandLine cmd,
                                             String fsReplicationName) throws IOException, BeaconException {
        if (job.isComplete() && job.isSuccessful()) {
            if (isSnapshot) {
                try {
                    FSSnapshotUtils.handleSnapshotCreation(targetFs, targetStagingUri, fsReplicationName);
                    FSSnapshotUtils.handleSnapshotEviction(sourceFs, cmd, sourceStagingUri);
                    FSSnapshotUtils.handleSnapshotEviction(targetFs, cmd, targetStagingUri);
                } catch (BeaconException e) {
                    throw new BeaconException("Exception occurred while handling snapshot : {}", e);
                }
            }
            LOG.info("Distcp Copy is successful");
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

}
