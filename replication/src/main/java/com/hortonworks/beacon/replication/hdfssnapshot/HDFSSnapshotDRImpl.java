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

package com.hortonworks.beacon.replication.hdfssnapshot;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.utils.DistCPOptionsUtil;
import com.hortonworks.beacon.replication.utils.ReplicationOptionsUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HDFSSnapshotDRImpl implements DRReplication {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSSnapshotDRImpl.class);

    final HDFSSnapshotReplicationJobDetails details;
    private String sourceStagingUri;
    private String targetStagingUri;

    public HDFSSnapshotDRImpl(ReplicationJobDetails details) {
        this.details = (HDFSSnapshotReplicationJobDetails)details;
    }

    @Override
    public void establishConnection() {
        sourceStagingUri = new Path(details.getSourceNN(), details.getSourceSnapshotDir()).toString();
        targetStagingUri = new Path(details.getTargetNN(), details.getTargetSnapshotDir()).toString();
    }

    @Override
    public void performReplication() throws BeaconException {
        DistributedFileSystem sourceFs = null;
        DistributedFileSystem targetFs = null;

        try {
        sourceFs = HDFSSnapshotUtil.getSourceFileSystem(details,
                new Configuration());
        targetFs = HDFSSnapshotUtil.getTargetFileSystem(details,
                new Configuration());
        } catch (BeaconException b) {
            LOG.error("Exception occurred while creating DistributedFileSystem:"+b);
        }

        // check if source and target path's exist and are snapshot-able
        try {
            if (sourceFs.exists(new Path(sourceStagingUri))) {
                if (!HDFSSnapshotUtil.isDirSnapshotable(sourceFs, new Path(details.getSourceSnapshotDir()))) {
                    throw new BeaconException(sourceStagingUri + " does not allow snapshots.");
                }
            } else {
                throw new BeaconException(sourceStagingUri + " does not exist.");
            }

            if (targetFs.exists(new Path(targetStagingUri))) {
                if (!HDFSSnapshotUtil.isDirSnapshotable(targetFs, new Path(details.getTargetSnapshotDir()))) {
                    throw new BeaconException(targetStagingUri+ " does not allow snapshots.");
                }
            } else {
                throw new BeaconException(targetStagingUri + " does not exist.");
            }
        } catch (IOException e) {
            throw new BeaconException(e.getMessage(), e);
        }

        String currentSnapshotName = HDFSSnapshotUtil.SNAPSHOT_PREFIX + details.getName() + "-" + System.currentTimeMillis();
        CommandLine cmd = ReplicationOptionsUtils.getCommand(details.getProperties());

        LOG.info("Creating snapshot on source fs: {} for URI: {}" ,targetFs.toString(), sourceStagingUri);
        createSnapshotInFileSystem(sourceStagingUri, currentSnapshotName, sourceFs);

        Job job = invokeCopy(cmd, sourceFs, targetFs, currentSnapshotName);

        try {
            if (job.isComplete() && job.isSuccessful()) {

                LOG.info("Creating snapshot on target fs: {} for URI: {}", targetFs.toString(), targetStagingUri);
                createSnapshotInFileSystem(targetStagingUri, currentSnapshotName, targetFs);
            }
        } catch (IOException ioe) {
            LOG.info("Exception occurred while checking job status: {}"+ioe);
        }

        String ageLimit = cmd.getOptionValue(
                HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
        int numSnapshots = Integer.parseInt(
                cmd.getOptionValue(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
        LOG.info("Snapshots Eviction on source FS :  {}", sourceFs.toString());
        evictSnapshots(sourceFs, sourceStagingUri, ageLimit, numSnapshots);


         ageLimit = cmd.getOptionValue(
                HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
         numSnapshots = Integer.parseInt(
                cmd.getOptionValue(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()));
        LOG.info("Snapshots Eviction on target FS :  {}", targetFs.toString());
        evictSnapshots(targetFs, targetStagingUri, ageLimit, numSnapshots);

    }

    public Job invokeCopy(CommandLine cmd, DistributedFileSystem sourceFs,
                           DistributedFileSystem targetFs, String currentSnapshotName  ) {
        Configuration conf = new Configuration();
        Job job = null;

        try {
            DistCpOptions options = getDistCpOptions(cmd, sourceFs, targetFs, currentSnapshotName, conf);

            options.setMaxMaps(Integer.parseInt(cmd.getOptionValue(
                    HDFSSnapshotDRProperties.DISTCP_MAX_MAPS.getName())));
            options.setMapBandwidth(Integer.parseInt(cmd.getOptionValue(
                    HDFSSnapshotDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())));

            LOG.info("Started DistCp with source Path: {} \t target path: {}", sourceStagingUri, targetStagingUri);
            DistCp distCp = new DistCp(conf, options);
            job = distCp.execute();
            LOG.info("Distp Hadoop job: {}", job.getJobID().toString());
            LOG.info("Completed Snapshot based DistCp");
        } catch (Exception e) {
            System.out.println("Exception occurred while invoking distcp : "+e);
        }

        return job;
    }

    private static void createSnapshotInFileSystem(String dirName, String snapshotName,
                                                   FileSystem fs) throws BeaconException {
        try {
            LOG.info("Creating snapshot {} in directory {}", snapshotName, dirName);
            fs.createSnapshot(new Path(dirName), snapshotName);
        } catch (IOException e) {
            LOG.warn("Unable to create snapshot {} in filesystem {}. Exception is {}",
                    snapshotName, fs.getConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY), e.getMessage());
            throw new BeaconException("Unable to create snapshot " + snapshotName, e);
        }
    }


    public DistCpOptions getDistCpOptions(CommandLine cmd, DistributedFileSystem sourceFs,
                                          DistributedFileSystem targetFs, String currentSnapshotName, Configuration conf)
            throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths

        List<Path> sourceUris = new ArrayList<>();
        sourceUris.add(new Path(sourceStagingUri));

        String replicatedSnapshotName = null;
        try {
            LOG.info("Target Snapshot directory : {} exist : {}", details.getTargetSnapshotDir(),
                    targetFs.exists(new Path(details.getTargetSnapshotDir())));
            if (targetFs.exists(new Path(details.getTargetSnapshotDir()))) {
                replicatedSnapshotName = findLatestReplicatedSnapshot(sourceFs, targetFs,
                        details.sourceSnapshotDir, details.targetSnapshotDir);
            }
        } catch (IOException e) {
            LOG.error("Error occurred when checking target dir : {} exists", details.targetSnapshotDir);
        }

        return DistCPOptionsUtil.getDistCpOptions(cmd, sourceUris, new Path(targetStagingUri),
                true, replicatedSnapshotName, currentSnapshotName, conf);
    }


    private String findLatestReplicatedSnapshot(DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                                                String sourceDir, String targetDir) throws BeaconException {
        try {
            FileStatus[] sourceSnapshots = sourceFs.listStatus(new Path(getSnapshotDir(sourceDir)));
            Set<String> sourceSnapshotNames = new HashSet<>();
            for (FileStatus snapshot : sourceSnapshots) {
                sourceSnapshotNames.add(snapshot.getPath().getName());
            }

            FileStatus[] targetSnapshots = targetFs.listStatus(new Path(getSnapshotDir(targetDir)));
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
    private String getSnapshotDir(String dirName) {
        dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
        return dirName + Path.SEPARATOR + HDFSSnapshotUtil.SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
    }


    protected static void evictSnapshots(DistributedFileSystem fs, String dirName, String ageLimit,
                                         int numSnapshots) throws BeaconException {
        try {
            LOG.info("Started evicting snapshots on dir {} , agelimit {}, numSnapshot {}",
                    dirName, ageLimit, numSnapshots);

            long evictionTime = System.currentTimeMillis() - EvictionHelper.evalExpressionToMilliSeconds(ageLimit);

            dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
            String snapshotDir = dirName + Path.SEPARATOR + HDFSSnapshotUtil.SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
            FileStatus[] snapshots = fs.listStatus(new Path(snapshotDir));
            if (snapshots.length <= numSnapshots) {
                LOG.info("No Eviction Required as number of snapshots : {} is less than " +
                        "numSnapshots: {}", snapshots.length, numSnapshots );
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
                            new Path(dirName), snapshots[i].getPath().getName()  );
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

}
