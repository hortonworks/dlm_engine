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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.JobExecutionDetails;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.utils.DistCPOptionsUtil;
import com.hortonworks.beacon.replication.utils.FSDRUtils;
import com.hortonworks.beacon.replication.utils.ReplicationOptionsUtils;
import com.hortonworks.beacon.store.JobStatus;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
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

    public FSDRImpl(ReplicationJobDetails details) {
        this.properties = details.getProperties();
        isSnapshot = false;
        jobExecutionDetails = new JobExecutionDetails();
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
            sourceFs = FSDRUtils.getSourceFileSystem(properties.getProperty(
                    FSDRProperties.SOURCE_NN.getName()), new Configuration());
            targetFs = FSDRUtils.getTargetFileSystem(properties.getProperty(
                    FSDRProperties.TARGET_NN.getName()), new Configuration());
        } catch (BeaconException b) {
            LOG.error("Exception occurred while creating DistributedFileSystem:" + b);
        }

        //Add TDE as well
        CommandLine cmd = ReplicationOptionsUtils.getCommand(properties);
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(cmd.getOptionValue(FSDRUtils.TDE_ENCRYPTION_ENABLED));

        // check if source and target path's exist and are snapshot-able
        if (!tdeEncryptionEnabled) {
            if (properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()) != null
                    && properties.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()) != null) {
                isSnapshot = isDirectorySnapshottable(sourceFs, targetFs, sourceStagingUri, targetStagingUri);
                if (isSnapshot) {
                    fSReplicationName = FSDRUtils.SNAPSHOT_PREFIX +
                            properties.getProperty(FSDRProperties.JOB_NAME.getName()) + "-" + System.currentTimeMillis();
                    LOG.info("Creating snapshot on source fs: {} for URI: {}", targetFs.toString(), sourceStagingUri);
                    FSDRUtils.createSnapshotInFileSystem(sourceStagingUri, fSReplicationName, sourceFs);
                }
            }
        }


        Job job = invokeCopy(cmd, sourceFs, targetFs, fSReplicationName);
        LOG.info("Invoked copy of job. checking status complete and successful");
        try {
            if (job.isComplete() && job.isSuccessful()) {
                jobExecutionDetails.setJobStatus(JobStatus.SUCCESS.name());
                jobExecutionDetails.setJobId(job.getJobID().toString());

                if (isSnapshot) {
                    LOG.info("Creating snapshot on target fs: {} for URI: {}", targetFs.toString(), targetStagingUri);
                    FSDRUtils.createSnapshotInFileSystem(targetStagingUri, fSReplicationName, targetFs);

                    String ageLimit = cmd.getOptionValue(
                            FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
                    int numSnapshots = Integer.parseInt(
                            cmd.getOptionValue(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
                    LOG.info("Snapshots Eviction on source FS :  {}", sourceFs.toString());
                    evictSnapshots(sourceFs, sourceStagingUri, ageLimit, numSnapshots);

                    ageLimit = cmd.getOptionValue(
                            FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
                    numSnapshots = Integer.parseInt(
                            cmd.getOptionValue(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()));
                    LOG.info("Snapshots Eviction on target FS :  {}", targetFs.toString());
                    evictSnapshots(targetFs, targetStagingUri, ageLimit, numSnapshots);
                }
            } else {
                jobExecutionDetails.setJobStatus(JobStatus.FAILED.name());
                if (job.getJobID() != null) {
                    jobExecutionDetails.setJobId(job.getJobID().toString());
                }
            }
        } catch (Exception e) {
            LOG.error("Exception occurred while checking job status: {}", e);
            jobExecutionDetails.setJobStatus(JobStatus.FAILED.name());
            if (job.getJobID() != null) {
                jobExecutionDetails.setJobId(job.getJobID().toString());
            }
        }
    }

    public Job invokeCopy(CommandLine cmd, DistributedFileSystem sourceFs,
                          DistributedFileSystem targetFs, String fSReplicationName) {
        Configuration conf = new Configuration();
        Job job = null;
        try {
            DistCpOptions options = getDistCpOptions(cmd, sourceFs, targetFs, fSReplicationName, conf);

            options.setMaxMaps(Integer.parseInt(cmd.getOptionValue(
                    FSDRProperties.DISTCP_MAX_MAPS.getName())));
            options.setMapBandwidth(Integer.parseInt(cmd.getOptionValue(
                    FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())));

            LOG.info("Started DistCp with source Path: {} \t target path: {}", sourceStagingUri, targetStagingUri);

            String tdeEncryptionEnabled = cmd.getOptionValue(FSDRUtils.TDE_ENCRYPTION_ENABLED);

            if (isSnapshot && (StringUtils.isNotBlank(tdeEncryptionEnabled)
                    && !(tdeEncryptionEnabled.equalsIgnoreCase(Boolean.TRUE.toString())))) {
                LOG.info("Perfoming FS Snapshot replication");
                jobExecutionDetails.setJobExecutionType("SNAPSHOT");
            } else {
                LOG.info("Performing FS replication");
                jobExecutionDetails.setJobExecutionType("FS");
            }
            DistCp distCp = new DistCp(conf, options);
            job = distCp.execute();
            LOG.info("Distcp Hadoop job: {}", job.getJobID().toString());
        } catch (Exception e) {
            LOG.error("Exception occurred while invoking distcp : " + e);
            jobExecutionDetails.setJobStatus(JobStatus.FAILED.name());
            if (job != null && job.getJobID() != null) {
                jobExecutionDetails.setJobId(job.getJobID().toString());
            }
        }

        return job;
    }

    private boolean isDirectorySnapshottable(DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                                             String sourceStagingUri, String targetStagingUri)
            throws BeaconException {
        try {
            if (sourceFs.exists(new Path(sourceStagingUri))) {
                if (!FSDRUtils.isDirSnapshotable(sourceFs, new Path(sourceStagingUri))) {
                    return false;
                }
            } else {
                throw new BeaconException(sourceStagingUri + " does not exist.");
            }

            if (targetFs.exists(new Path(targetStagingUri))) {
                if (!FSDRUtils.isDirSnapshotable(targetFs, new Path(targetStagingUri))) {
                    return false;
                }
            } else {
                throw new BeaconException(targetStagingUri + " does not exist.");
            }
        } catch (IOException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return true;
    }


    public DistCpOptions getDistCpOptions(CommandLine cmd, DistributedFileSystem sourceFs,
                                          DistributedFileSystem targetFs, String fSReplicationName, Configuration conf)
            throws BeaconException, IOException {
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
        }

        return DistCPOptionsUtil.getDistCpOptions(cmd, sourceUris, new Path(targetStagingUri),
                isSnapshot, replicatedSnapshotName, fSReplicationName, conf);
    }


    private String findLatestReplicatedSnapshot(DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                                                String sourceDir, String targetDir) throws BeaconException {
        try {
            FileStatus[] sourceSnapshots = sourceFs.listStatus(new Path(FSDRUtils.getSnapshotDir(sourceDir)));
            Set<String> sourceSnapshotNames = new HashSet<>();
            for (FileStatus snapshot : sourceSnapshots) {
                sourceSnapshotNames.add(snapshot.getPath().getName());
            }

            FileStatus[] targetSnapshots = targetFs.listStatus(new Path(FSDRUtils.getSnapshotDir(targetDir)));
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

    protected static void evictSnapshots(DistributedFileSystem fs, String dirName, String ageLimit,
                                         int numSnapshots) throws BeaconException {
        try {
            LOG.info("Started evicting snapshots on dir {} , agelimit {}, numSnapshot {}",
                    dirName, ageLimit, numSnapshots);

            long evictionTime = System.currentTimeMillis() - EvictionHelper.evalExpressionToMilliSeconds(ageLimit);

            dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
            String snapshotDir = dirName + Path.SEPARATOR + FSDRUtils.SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
            FileStatus[] snapshots = fs.listStatus(new Path(snapshotDir));
            if (snapshots.length <= numSnapshots) {
                LOG.info("No Eviction Required as number of snapshots : {} is less than " +
                        "numSnapshots: {}", snapshots.length, numSnapshots);
                // no eviction needed
                return;
            }

            // Sort by last modified time, ascending order.
            Arrays.sort(snapshots, new Comparator<FileStatus>() {
                @Override
                public int compare(FileStatus f1, FileStatus f2) {
                    return Long.compare(f1.getModificationTime(), f2.getModificationTime());
                }
            });

            for (int i = 0; i < (snapshots.length - numSnapshots); i++) {
                // delete if older than ageLimit while retaining numSnapshots
                if (snapshots[i].getModificationTime() < evictionTime) {
                    LOG.info("Deleting snapshots with path : {} and snapshot path: {}",
                            new Path(dirName), snapshots[i].getPath().getName());
                    fs.deleteSnapshot(new Path(dirName), snapshots[i].getPath().getName());
                }
            }

        } catch (ELException ele) {
            LOG.warn("Unable to parse retention age limit {} {}", ageLimit, ele.getMessage());
            throw new BeaconException("Unable to parse retention age limit " + ageLimit, ele);
        } catch (IOException ioe) {
            LOG.warn("Unable to evict snapshots from dir {} {}", dirName, ioe);
            throw new BeaconException("Unable to evict snapshots from dir " + dirName, ioe);
        }

    }

    public String getJobExecutionContextDetails() throws BeaconException {
        LOG.info("Job status after replication : {}", getJobExecutionDetails().toJsonString());
        return getJobExecutionDetails().toJsonString();
    }

    private static String getStagingUri(String dataset, String namenodeEndpoint) throws BeaconException {
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
}
