/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.JobExecutionDetails;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.utils.DistCPOptionsUtil;
import com.hortonworks.beacon.replication.utils.ReplicationOptionsUtils;
import com.hortonworks.beacon.store.JobStatus;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class FSDRImpl implements DRReplication {

    private static final Logger LOG = LoggerFactory.getLogger(FSDRImpl.class);

    private Properties properties = null;
    private String sourceStagingUri;
    private String targetStagingUri;
    private boolean isSnapshot;
    private JobExecutionDetails jobExecutionDetails;
    private String replPolicyExecutionType;

    public FSDRImpl(ReplicationJobDetails details) {
        this.properties = details.getProperties();
        isSnapshot = false;
        jobExecutionDetails = new JobExecutionDetails();
        replPolicyExecutionType = details.getProperties().getProperty(PolicyHelper.INSTANCE_EXECUTION_TYPE);
    }

    public JobExecutionDetails getJobExecutionDetails() {
        return jobExecutionDetails;
    }

    public void setJobExecutionDetails(JobExecutionDetails jobExecutionDetails) {
        this.jobExecutionDetails = jobExecutionDetails;
    }

    @Override
    public void init() throws BeaconException {
        String sourceDataset = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetDataset = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());

        sourceStagingUri = getStagingUri(sourceDataset, properties.getProperty(FSDRProperties.SOURCE_NN.getName()));
        targetStagingUri = getStagingUri(targetDataset, properties.getProperty(FSDRProperties.TARGET_NN.getName()));
    }

    @Override
    public void performReplication() throws BeaconException {
        DistributedFileSystem sourceFs = null;
        DistributedFileSystem targetFs = null;
        String fSReplicationName = properties.getProperty(FSDRProperties.JOB_NAME.getName())
                + "-" + System.currentTimeMillis();

        try {
            sourceFs = FSUtils.getFileSystem(properties.getProperty(
                    FSDRProperties.SOURCE_NN.getName()), new Configuration());
            targetFs = FSUtils.getFileSystem(properties.getProperty(
                    FSDRProperties.TARGET_NN.getName()), new Configuration());
        } catch (BeaconException b) {
            LOG.error("Exception occurred while creating DistributedFileSystem:" + b);
            throw b;
        }

        //Add TDE as well
        CommandLine cmd = ReplicationOptionsUtils.getCommand(properties);
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(cmd.getOptionValue(FSUtils.TDE_ENCRYPTION_ENABLED));
        LOG.info("TDE Encryption enabled : {}", tdeEncryptionEnabled);
        jobExecutionDetails.setJobExecutionType(replPolicyExecutionType);
        // check if source and target path's exist and are snapshot-able
        if (!tdeEncryptionEnabled) {
            if (properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()) != null) {
                try {
                    isSnapshot = FSUtils.isDirectorySnapshottable(sourceFs, targetFs, sourceStagingUri, targetStagingUri);
                    if (isSnapshot) {
                        fSReplicationName = FSUtils.SNAPSHOT_PREFIX +
                                properties.getProperty(FSDRProperties.JOB_NAME.getName()) + "-" + System.currentTimeMillis();
                        LOG.info("Creating snapshot on source fs: {} for URI: {}", targetFs.toString(), sourceStagingUri);
                        FSUtils.createSnapshotInFileSystem(sourceStagingUri, fSReplicationName, sourceFs);
                    }
                } catch (BeaconException e) {
                    LOG.error("Exception occurred while checking directory for snapshot replication :"+e);
                    jobExecutionDetails.updateJobExecutionDetails(JobStatus.FAILED.name(), e.getMessage(), null);
                }
            }
        }


        Job job = invokeCopy(cmd, sourceFs, targetFs, fSReplicationName);
        LOG.info("Invoked copy of job. checking status complete and successful");
        try {
            if (job.isComplete() && job.isSuccessful()) {
                jobExecutionDetails.updateJobExecutionDetails(JobStatus.SUCCESS.name(), "Copy Successful", getJob(job));

                if (isSnapshot) {
                    LOG.info("Creating snapshot on target fs: {} for URI: {}", targetFs.toString(), targetStagingUri);
                    FSUtils.createSnapshotInFileSystem(targetStagingUri, fSReplicationName, targetFs);

                    String ageLimit = cmd.getOptionValue(
                            FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
                    int numSnapshots = Integer.parseInt(
                            cmd.getOptionValue(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
                    LOG.info("Snapshots Eviction on source FS :  {}", sourceFs.toString());
                    FSUtils.evictSnapshots(sourceFs, sourceStagingUri, ageLimit, numSnapshots);

                    ageLimit = cmd.getOptionValue(
                            FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
                    numSnapshots = Integer.parseInt(
                            cmd.getOptionValue(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()));
                    LOG.info("Snapshots Eviction on target FS :  {}", targetFs.toString());
                    FSUtils.evictSnapshots(targetFs, targetStagingUri, ageLimit, numSnapshots);
                }
            } else {
                String message = "Exception in job occurred:" +job.getJobID().toString();
                jobExecutionDetails.updateJobExecutionDetails(JobStatus.FAILED.name(), message, getJob(job));
            }
        } catch (Exception e) {
            LOG.error("Exception occurred while checking job status: {}", e);
            jobExecutionDetails.updateJobExecutionDetails(JobStatus.FAILED.name(), e.getMessage(), getJob(job));
            throw new BeaconException(e);
        }
    }

    public Job invokeCopy(CommandLine cmd, DistributedFileSystem sourceFs,
                          DistributedFileSystem targetFs, String fSReplicationName) throws BeaconException {
        Configuration conf = new Configuration();
        Job job = null;
        try {
            DistCpOptions options = getDistCpOptions(cmd, sourceFs, targetFs, fSReplicationName, conf);

            options.setMaxMaps(Integer.parseInt(cmd.getOptionValue(
                    FSDRProperties.DISTCP_MAX_MAPS.getName())));
            options.setMapBandwidth(Integer.parseInt(cmd.getOptionValue(
                    FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())));

            LOG.info("Started DistCp with source Path: {} \t target path: {}", sourceStagingUri, targetStagingUri);
            LOG.info("Perfoming FS replication of execution type: {}", replPolicyExecutionType );

            DistCp distCp = new DistCp(conf, options);
            job = distCp.execute();
            LOG.info("Distcp Hadoop job: {}", job.getJobID().toString());
        } catch (Exception e) {
            LOG.error("Exception occurred while invoking distcp : " + e);
            jobExecutionDetails.updateJobExecutionDetails(JobStatus.FAILED.name(), e.getMessage(), getJob(job));
            throw new BeaconException(e);
        }

        return job;
    }

    public DistCpOptions getDistCpOptions(CommandLine cmd, DistributedFileSystem sourceFs,
                                          DistributedFileSystem targetFs, String fSReplicationName,
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
                replicatedSnapshotName = findLatestReplicatedSnapshot(sourceFs, targetFs,
                        sourceSnapshotDir,
                        targetSnapshotDir);
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

    public String getJobExecutionContextDetails() throws BeaconException {
        LOG.info("Job status after replication : {}", getJobExecutionDetails().toJsonString());
        return getJobExecutionDetails().toJsonString();
    }

    private String getStagingUri(String dataset, String namenodeEndpoint) throws BeaconException {
        String stagingUri;
        if (FSUtils.isHCFS(new Path(dataset))) {
            // HCFS dataset has full path
            stagingUri = dataset;
        } else {
            try {
                URI pathUri = new URI(dataset.trim());
                String authority = pathUri.getAuthority();
                if (authority == null) {
                    stagingUri = new Path(namenodeEndpoint, dataset).toString();
                } else {
                    stagingUri = dataset;
                }
            } catch (URISyntaxException e) {
                throw new BeaconException(e);
            }
        }
        return stagingUri;
    }

    private String getJob(Job job) {
        return ((job != null)  && (job.getJobID() != null)) ? job.getJobID().toString() : null;
    }
}
