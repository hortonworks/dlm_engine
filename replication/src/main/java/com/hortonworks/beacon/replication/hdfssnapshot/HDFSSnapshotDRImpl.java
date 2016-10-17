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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by pbishnoi on 10/5/16.
 */
public class HDFSSnapshotDRImpl implements DRReplication {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSSnapshotDRImpl.class);

    final HDFSSnapshotReplicationJobDetails details;
    private String sourceStagingUri = null;
    private String targetStagingUri = null;

    public HDFSSnapshotDRImpl(ReplicationJobDetails details) {
        this.details = (HDFSSnapshotReplicationJobDetails)details;
        System.out.println("Inside snapshot constructor");
        //System.out.println(((HDFSReplicationJobDetails) details).getProperties().size());
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

        // Generate snapshot on source.
        createSnapshotInFileSystem(sourceStagingUri, currentSnapshotName, sourceFs);
        CommandLine cmd = ReplicationOptionsUtils.getCommand(details.getProperties());
        Configuration conf = new Configuration();
        //DistCpOptions options = null;
        Job job = null;

        try {
            DistCpOptions options = getDistCpOptions(cmd, sourceFs, targetFs, currentSnapshotName, conf);

            options.setMaxMaps(Integer.parseInt(cmd.getOptionValue(HDFSSnapshotDRProperties.MAX_MAPS.getName())));
            options.setMapBandwidth(Integer.parseInt(cmd.getOptionValue(HDFSSnapshotDRProperties.MAP_BANDWIDTH_IN_MB.getName())));

            LOG.info("Started DistCp with source Path: {} \t target path: {}", sourceStagingUri, targetStagingUri);
            DistCp distCp = new DistCp(conf, options);
            job = distCp.execute();
            LOG.info("Distp Hadoop job: {}", job.getJobID().toString());
            LOG.info("Completed Snapshot based DistCp");
        } catch (Exception e) {
            System.out.println("Exception occurred while invoking distcp : "+e);
        }

        // Generate snapshot on target if distCp succeeds.
        try {
            if (job.isComplete() && job.isSuccessful()) {
                LOG.info("Creating snapshot on target fs: {} for URI: {}" ,targetFs.toString(), targetStagingUri);
                createSnapshotInFileSystem(targetStagingUri, currentSnapshotName, targetFs);
            } else {
                LOG.info("Snapshot on target has not been occurred");
            }
        } catch (IOException e) {
            throw new BeaconException("Exception occurred while creating snapshot on target fs:");
        }
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

       /* DistCpOptions distcpOptions = new DistCpOptions(sourceUris, new Path(targetStagingUri));
        distcpOptions.setSyncFolder(true); //ensures directory structure is maintained when source is copied to target

        if (details.isTdeEncryptionEnabled()) {
            distcpOptions.setSkipCRC(true);
        }

        distcpOptions.setBlocking(true);
        distcpOptions.setDeleteMissing(true);
        distcpOptions.setMaxMaps(details.getMaxMaps());
        distcpOptions.setMapBandwidth(details.getMapBandwidth());

        if (StringUtils.isNotBlank(replicatedSnapshotName)) {
            distcpOptions.setUseDiff(true, replicatedSnapshotName, currentSnapshotName);
        }*/

        return DistCPOptionsUtil.getDistCpOptions(cmd, sourceUris, new Path(targetStagingUri),
                true, replicatedSnapshotName, currentSnapshotName, conf);
        //return distcpOptions;
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

}
