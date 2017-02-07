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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.entity.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.utils.ReplicationOptionsUtils;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

/**
 *  Test class to test the FS Repliction functionality.
 */
public class FSDRImplTest {

    private static final Logger LOG = LoggerFactory.getLogger(FSDRImplTest.class);
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String FS_ENDPOINT = "hdfs://localhost:54136";
    private static final String BEACON_ENDPOINT = "http://localhost:55000";

    private MiniDFSCluster miniDFSCluster;
    private DistributedFileSystem miniDfs;
    private File baseDir;
    private Path sourceDir = new Path("/apps/beacon/sourceDir/");
    private Path targetDir = new Path("/apps/beacon/targetDir/");
    private Path sourceSnapshotDir = new Path("/apps/beacon/snapshot/sourceDir/");
    private Path targetSnapshotDir = new Path("/apps/beacon/snapshot/targetDir/");
    private Path evictionDir = new Path("/apps/beacon/snapshot-eviction/");
    private static final int NUM_FILES = 7;
    private FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    private ConfigurationStore store = ConfigurationStore.getInstance();

    private PropertiesIgnoreCase sourceClusterProps = new PropertiesIgnoreCase();
    private PropertiesIgnoreCase targetClusterProps = new PropertiesIgnoreCase();
    private Properties fsSnapshotReplProps = new Properties();


    private String[][] sourceAttrs = {
            {Cluster.ClusterFields.FSENDPOINT.getName(), FS_ENDPOINT},
            {Cluster.ClusterFields.NAME.getName(), SOURCE},
            {Cluster.ClusterFields.DECRIPTION.getName(), "source cluster"},
            {Cluster.ClusterFields.BEACONENDPOINT.getName(), BEACON_ENDPOINT},

    };

    private String[][] targetAttrs = {
            {Cluster.ClusterFields.FSENDPOINT.getName(), FS_ENDPOINT},
            {Cluster.ClusterFields.NAME.getName(), TARGET},
            {Cluster.ClusterFields.DECRIPTION.getName(), "target cluster"},
            {Cluster.ClusterFields.BEACONENDPOINT.getName(), BEACON_ENDPOINT},

    };

    @BeforeClass
    public void init() {
        for (int i = 0; i < sourceAttrs.length; i++) {
            sourceClusterProps.setProperty(sourceAttrs[i][0], sourceAttrs[i][1]);
        }

        for (int i = 0; i < sourceAttrs.length; i++) {
            targetClusterProps.setProperty(targetAttrs[i][0], targetAttrs[i][1]);
        }

        try {
            store.init();
            store.publish(EntityType.CLUSTER, ClusterBuilder.buildCluster(sourceClusterProps, SOURCE));
            store.publish(EntityType.CLUSTER, ClusterBuilder.buildCluster(targetClusterProps, TARGET));
        } catch (BeaconException e) {
            LOG.error("Exception Occurred while initializing Configuration Store", e);
        }

        String[][] fsSnapshotReplAttrs = {
                {FSDRProperties.JOB_NAME.getName(), "testFSSnapshot"},
                {FSDRProperties.DISTCP_MAX_MAPS.getName(), "1"},
                {FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(), "10"},
                {FSDRProperties.JOB_FREQUENCY.getName(), "3600"},
                {FSDRProperties.SOURCE_NN.getName(), sourceClusterProps.getProperty(
                        Cluster.ClusterFields.FSENDPOINT.getName()), },
                {FSDRProperties.TARGET_NN.getName(), targetClusterProps.getProperty(
                        Cluster.ClusterFields.FSENDPOINT.getName()), },
                {FSDRProperties.SOURCE_DATASET.getName(), sourceSnapshotDir.toString()},
                {FSDRProperties.TARGET_DATASET.getName(), targetSnapshotDir.toString()},
                {FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false"},
                {FSDRProperties.JOB_TYPE.getName(), ReplicationType.FS.getName()},
        };

        for (int i = 0; i < fsSnapshotReplAttrs.length; i++) {
            fsSnapshotReplProps.setProperty(fsSnapshotReplAttrs[i][0], fsSnapshotReplAttrs[i][1]);
        }

        try {
            baseDir = Files.createTempDirectory("test_snapshot-replication").toFile().getAbsoluteFile();
            miniDFSCluster = MiniHDFSClusterUtil.initMiniDfs(
                    MiniHDFSClusterUtil.SNAPSHOT_REPL_TEST_PORT, baseDir);
            miniDfs = miniDFSCluster.getFileSystem();
            miniDfs.mkdirs(sourceSnapshotDir);
            miniDfs.mkdirs(targetSnapshotDir);

            miniDfs.allowSnapshot(sourceSnapshotDir);
            miniDfs.allowSnapshot(targetSnapshotDir);

            miniDfs.mkdirs(sourceDir);
            miniDfs.mkdirs(targetDir);
        } catch (IOException ioe) {
            LOG.error("Exception occurred while creating directory on miniDFS : {} ", ioe);
        } catch (Exception e) {
            LOG.error("Exception occurred while initializing the miniDFS : {} ", e);
        }
        BeaconConfig.getInstance().getEngine().setInTestMode(true);
    }


    @Test
    public void testReplicationPolicyType() throws Exception {

        String sourceDataset = fsSnapshotReplProps.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetDataset = fsSnapshotReplProps.getProperty(FSDRProperties.TARGET_DATASET.getName());

        ReplicationPolicy replicationPolicy = new ReplicationPolicy(new ReplicationPolicy.Builder(
                fsSnapshotReplProps.getProperty(FSDRProperties.JOB_NAME.getName()),
                fsSnapshotReplProps.getProperty(FSDRProperties.JOB_TYPE.getName()),
                sourceDataset,
                targetDataset,
                SOURCE,
                TARGET,
                Long.parseLong(fsSnapshotReplProps.getProperty(FSDRProperties.JOB_FREQUENCY.getName()))
        ));

        Properties customProps = new Properties();
        customProps.setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false");
        replicationPolicy.setCustomProperties(customProps);

        boolean isSnapshotable = FSUtils.isDirectorySnapshottable(
                FSUtils.getFileSystem(fsSnapshotReplProps.getProperty(FSDRProperties.SOURCE_NN.getName()), new
                        Configuration(), false),
                FSUtils.getFileSystem(fsSnapshotReplProps.getProperty(FSDRProperties.TARGET_NN.getName()), new
                        Configuration(), false),
                sourceClusterProps.getPropertyIgnoreCase(
                        Cluster.ClusterFields.FSENDPOINT.getName()) + sourceDataset,
                targetClusterProps.getPropertyIgnoreCase(
                        Cluster.ClusterFields.FSENDPOINT.getName()) + targetDataset);

        Assert.assertEquals(isSnapshotable, true);

        Assert.assertEquals(PolicyHelper.getReplicationPolicyType(replicationPolicy), "FS_SNAPSHOT");

        customProps.setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
        replicationPolicy.setCustomProperties(customProps);
        Assert.assertEquals(PolicyHelper.getReplicationPolicyType(replicationPolicy), "FS");

        //Non snapshot dataset
        sourceDataset = sourceDir.toString();
        targetDataset = targetDir.toString();

        ReplicationPolicy nonSnapshotreplicationPolicy = new ReplicationPolicy(new ReplicationPolicy.Builder(
                fsSnapshotReplProps.getProperty(FSDRProperties.JOB_NAME.getName()),
                fsSnapshotReplProps.getProperty(FSDRProperties.JOB_TYPE.getName()),
                sourceDataset,
                targetDataset,
                SOURCE,
                TARGET,
                Long.parseLong(fsSnapshotReplProps.getProperty(FSDRProperties.JOB_FREQUENCY.getName()))
        ));

        isSnapshotable = FSUtils.isDirectorySnapshottable(
                FSUtils.getFileSystem(fsSnapshotReplProps.getProperty(FSDRProperties.SOURCE_NN.getName()), new
                        Configuration(), false),
                FSUtils.getFileSystem(fsSnapshotReplProps.getProperty(FSDRProperties.TARGET_NN.getName()), new
                        Configuration(), false),
                sourceClusterProps.getPropertyIgnoreCase(
                        Cluster.ClusterFields.FSENDPOINT.getName()) + sourceDataset,
                targetClusterProps.getPropertyIgnoreCase(
                        Cluster.ClusterFields.FSENDPOINT.getName()) + targetDataset);

        Assert.assertEquals(isSnapshotable, false);

        customProps.setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false");
        nonSnapshotreplicationPolicy.setCustomProperties(customProps);
        Assert.assertEquals(PolicyHelper.getReplicationPolicyType(nonSnapshotreplicationPolicy), "FS");
    }

    @Test
    public void testPerformReplication() throws Exception {

        ReplicationJobDetails jobDetails = new ReplicationJobDetails();
        jobDetails.setProperties(fsSnapshotReplProps);

        DistributedFileSystem sourceFs = (DistributedFileSystem) FSUtils.getFileSystem(jobDetails.getProperties().
                getProperty(FSDRProperties.SOURCE_NN.getName()), new Configuration(), false);
        DistributedFileSystem targetFs = (DistributedFileSystem) FSUtils.getFileSystem(jobDetails.getProperties().
                getProperty(FSDRProperties.TARGET_NN.getName()), new Configuration(), false);

        FSDRImpl fsImpl = new FSDRImpl(jobDetails);
        fsImpl.init();
        CommandLine cmd = ReplicationOptionsUtils.getCommand(jobDetails.getProperties());
        String currentSnapshotName = FSUtils.SNAPSHOT_PREFIX + jobDetails.getName() + "-" + System.currentTimeMillis();
        // create dir1, create snapshot, invoke copy, check file in target, create snapshot on target
        Path dir1 = new Path(sourceSnapshotDir, "dir1");
        miniDfs.mkdir(dir1, fsPermission);
        miniDfs.createSnapshot(sourceSnapshotDir, "snapshot1");
        fsImpl.invokeCopy(cmd, sourceFs, targetFs, "snapshot1");
        miniDfs.createSnapshot(targetSnapshotDir, "snapshot1");
        Assert.assertTrue(miniDfs.exists(new Path(targetSnapshotDir, "dir1")));

        // create dir2, create snapshot, invoke copy, check dir in target, create snapshot on target
        Path dir2 = new Path(sourceSnapshotDir, "dir2");
        miniDfs.mkdir(dir2, fsPermission);
        miniDfs.createSnapshot(sourceSnapshotDir, "snapshot2");
        fsImpl.invokeCopy(cmd, sourceFs, targetFs, "snapshot2");
        miniDfs.createSnapshot(targetSnapshotDir, "snapshot2");
        Assert.assertTrue(miniDfs.exists(new Path(targetSnapshotDir, "dir1")));
        Assert.assertTrue(miniDfs.exists(new Path(targetSnapshotDir, "dir2")));

        // delete dir1, create snapshot, invoke copy, check file not in target
        miniDfs.delete(dir1, true);
        miniDfs.createSnapshot(sourceSnapshotDir, "snapshot3");
        fsImpl.invokeCopy(cmd, sourceFs, targetFs, "snapshot3");
        miniDfs.createSnapshot(targetSnapshotDir, "snapshot3");
        Assert.assertFalse(miniDfs.exists(new Path(targetSnapshotDir, "dir1")));
        Assert.assertTrue(miniDfs.exists(new Path(targetSnapshotDir, "dir2")));

    }

    private void createSnapshotsForEviction() throws Exception {
        for (int i = 0; i < NUM_FILES; i++) {
            miniDfs.createSnapshot(evictionDir, String.valueOf(i));
            Thread.sleep(10000);
        }
    }

    @Test
    public void testEvictSnapshots() throws Exception {
        miniDfs.mkdirs(evictionDir, fsPermission);
        miniDfs.allowSnapshot(evictionDir);

        createSnapshotsForEviction();

        ReplicationJobDetails jobDetails = new ReplicationJobDetails();
        jobDetails.setProperties(fsSnapshotReplProps);

        FSDRImpl fsImpl = new FSDRImpl(jobDetails);
        Path snapshotDir = new Path(evictionDir, ".snapshot");
        FileStatus[] fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES);

        FSUtils.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", NUM_FILES + 1);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES);

        FSUtils.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", NUM_FILES - 1);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES - 1);

        FSUtils.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", 2);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertTrue(fileStatuses.length >= 5);
    }

    @AfterClass
    public void cleanup() throws Exception {
        MiniHDFSClusterUtil.cleanupDfs(miniDFSCluster, baseDir);
        store.remove(EntityType.CLUSTER, SOURCE);
        store.remove(EntityType.CLUSTER, TARGET);
    }
}
