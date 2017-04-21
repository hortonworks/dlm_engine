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
import com.hortonworks.beacon.replication.utils.DistCPOptionsUtil;
import com.hortonworks.beacon.replication.utils.ReplicationOptionsUtils;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * FileSystem Replication implementation.
 */
public class FSDRImpl extends InstanceReplication implements BeaconJob {

    private static final Logger LOG = LoggerFactory.getLogger(FSDRImpl.class);

    private Properties properties = null;
    private String sourceStagingUri;
    private String targetStagingUri;
    private boolean isSnapshot;
    private boolean isHCFS;

    public FSDRImpl(ReplicationJobDetails details) {
        super(details);
        this.properties = getProperties();
        isSnapshot = false;
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {
        String sourceDataset = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetDataset = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());

        sourceStagingUri = FSUtils.getStagingUri(sourceDataset,
                properties.getProperty(FSDRProperties.SOURCE_NN.getName()));
        targetStagingUri = FSUtils.getStagingUri(targetDataset,
                properties.getProperty(FSDRProperties.TARGET_NN.getName()));

        if (FSUtils.isHCFS(new Path(sourceStagingUri)) || FSUtils.isHCFS(new Path(targetStagingUri))) {
            isHCFS = true;
        }
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        FileSystem sourceFs = null;
        FileSystem targetFs = null;
        String fSReplicationName = properties.getProperty(FSDRProperties.JOB_NAME.getName())
                + "-" + System.currentTimeMillis();

        try {
            sourceFs = FSUtils.getFileSystem(properties.getProperty(
                    FSDRProperties.SOURCE_NN.getName()), new Configuration(), isHCFS);
            targetFs = FSUtils.getFileSystem(properties.getProperty(
                    FSDRProperties.TARGET_NN.getName()), new Configuration(), isHCFS);
        } catch (BeaconException b) {
            LOG.error("Exception occurred while creating DistributedFileSystem:" + b);
            throw b;
        }

        //Add TDE as well
        CommandLine cmd = ReplicationOptionsUtils.getCommand(properties);
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(cmd.getOptionValue(FSUtils.TDE_ENCRYPTION_ENABLED));
        LOG.info("TDE Encryption enabled : {}", tdeEncryptionEnabled);
        // check if source and target path's exist and are snapshot-able
        if (!tdeEncryptionEnabled) {
            if (properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()) != null) {
                try {
                    isSnapshot = FSUtils.isDirectorySnapshottable(sourceFs, targetFs,
                            sourceStagingUri, targetStagingUri);
                    if (isSnapshot) {
                        fSReplicationName = FSUtils.SNAPSHOT_PREFIX
                                + properties.getProperty(FSDRProperties.JOB_NAME.getName())
                                + "-" + System.currentTimeMillis();
                        LOG.info("Creating snapshot on source fs: {} for URI: {}",
                                targetFs.toString(), sourceStagingUri);
                        FSUtils.createSnapshotInFileSystem(sourceStagingUri, fSReplicationName, sourceFs);
                    }
                } catch (BeaconException e) {
                    LOG.error("Exception occurred while checking directory for snapshot replication :" + e);
                    setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), null);
                }
            }
        }


        Job job = invokeCopy(cmd, jobContext, sourceFs, targetFs, fSReplicationName);
        LOG.info("Invoked copy of job. checking status complete and successful");
        try {
            if (job.isComplete() && job.isSuccessful()) {
                setInstanceExecutionDetails(jobContext,
                        JobStatus.SUCCESS, JobStatus.SUCCESS.name(), job);

                if (isSnapshot) {
                    LOG.info("Creating snapshot on target fs: {} for URI: {}", targetFs.toString(), targetStagingUri);
                    FSUtils.createSnapshotInFileSystem(targetStagingUri, fSReplicationName, targetFs);

                    String ageLimit = cmd.getOptionValue(
                            FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
                    int numSnapshots = Integer.parseInt(
                            cmd.getOptionValue(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
                    LOG.info("Snapshots Eviction on source FS :  {}", sourceFs.toString());
                    FSUtils.evictSnapshots((DistributedFileSystem) sourceFs, sourceStagingUri, ageLimit, numSnapshots);

                    ageLimit = cmd.getOptionValue(
                            FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
                    numSnapshots = Integer.parseInt(
                            cmd.getOptionValue(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()));
                    LOG.info("Snapshots Eviction on target FS :  {}", targetFs.toString());
                    FSUtils.evictSnapshots((DistributedFileSystem) targetFs, targetStagingUri, ageLimit, numSnapshots);
                }
            } else {
                String message = "Exception in job occurred:" + getJob(job);
                setInstanceExecutionDetails(jobContext, JobStatus.FAILED, message, job);
            }
        } catch (Exception e) {
            LOG.error("Exception occurred while checking job status: {}", e);
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            throw new BeaconException(e);
        }
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {
    }

    public Job invokeCopy(CommandLine cmd, JobContext jobContext, FileSystem sourceFs,
                          FileSystem targetFs, String fSReplicationName) throws BeaconException {
        Configuration conf = new Configuration();
        Job job = null;
        try {
            DistCpOptions options = getDistCpOptions(cmd, sourceFs, targetFs, fSReplicationName, conf);

            options.setMaxMaps(Integer.parseInt(cmd.getOptionValue(
                    FSDRProperties.DISTCP_MAX_MAPS.getName())));
            options.setMapBandwidth(Integer.parseInt(cmd.getOptionValue(
                    FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())));

            LOG.info("Started DistCp with source Path: {} \t target path: {}", sourceStagingUri, targetStagingUri);

            DistCp distCp = new DistCp(conf, options);
            job = distCp.createAndSubmitJob();
            //TODO provide job context to handle the interruption between submission and waiting.
            distCp.waitForJobCompletion(job);
            LOG.info("DistCp Hadoop job: {}", getJob(job));
        } catch (InterruptedException e) {
            if (job != null) {
                LOG.error("replication job: {} interrupted, killing it.", getJob(job));
                try {
                    job.killJob();
                    setInstanceExecutionDetails(jobContext, JobStatus.KILLED, e.getMessage(), job);
                } catch (IOException ioe) {
                    LOG.error(ioe.getMessage(), ioe);
                }
            }
            throw new BeaconException(e);
        } catch (Exception e) {
            LOG.error("Exception occurred while invoking while copying data : " + e);
            setInstanceExecutionDetails(jobContext, JobStatus.FAILED, e.getMessage(), job);
            throw new BeaconException(e);
        }
        return job;
    }

    public DistCpOptions getDistCpOptions(CommandLine cmd, FileSystem sourceFs,
                                          FileSystem targetFs, String fSReplicationName,
                                          Configuration conf) throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths

        List<Path> sourceUris = new ArrayList<>();
        sourceUris.add(new Path(sourceStagingUri));

        String replicatedSnapshotName = null;
        String sourceSnapshotDir = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetSnapshotDir = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());

        try {
            LOG.info("Target Snapshot directory : {} exist : {}", targetSnapshotDir,
                    targetFs.exists(new Path(targetSnapshotDir)));
            if (isSnapshot && targetFs.exists(new Path(targetSnapshotDir))) {
                replicatedSnapshotName = findLatestReplicatedSnapshot((DistributedFileSystem) sourceFs,
                        (DistributedFileSystem) targetFs, sourceSnapshotDir, targetSnapshotDir);
            }
        } catch (IOException e) {
            LOG.error("Error occurred when checking target dir : {} exists", targetSnapshotDir);
            throw e;
        }

        return DistCPOptionsUtil.getDistCpOptions(cmd, sourceUris, new Path(targetStagingUri),
                isSnapshot, replicatedSnapshotName, fSReplicationName, conf);
    }


    private String findLatestReplicatedSnapshot(DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                                                String sourceDir, String targetDir) throws BeaconException {
        try {
            FileStatus[] sourceSnapshots = sourceFs.listStatus(new Path(FSUtils.getSnapshotDir(sourceDir)));
            Set<String> sourceSnapshotNames = new HashSet<>();
            for (FileStatus snapshot : sourceSnapshots) {
                sourceSnapshotNames.add(snapshot.getPath().getName());
            }

            FileStatus[] targetSnapshots = targetFs.listStatus(new Path(FSUtils.getSnapshotDir(targetDir)));
            if (targetSnapshots.length > 0) {
                //sort target snapshots in desc order of creation time.
                Arrays.sort(targetSnapshots, new Comparator<FileStatus>() {
                    @Override
                    public int compare(FileStatus f1, FileStatus f2) {
                        return Long.compare(f2.getModificationTime(), f1.getModificationTime());
                    }
                });

                // get most recent snapshot name that exists in source.
                for (FileStatus targetSnapshot : targetSnapshots) {
                    String name = targetSnapshot.getPath().getName();
                    if (sourceSnapshotNames.contains(name)) {
                        return name;
                    }
                }
                // If control reaches here,
                // there are snapshots on target, but none are replicated from source. Return null.
            }
            return null;
        } catch (IOException e) {
            LOG.error("Unable to find latest snapshot on targetDir {} {}", targetDir, e.getMessage());
            throw new BeaconException("Unable to find latest snapshot on targetDir " + targetDir, e);
        }
    }
}
