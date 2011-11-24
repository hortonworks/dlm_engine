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

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.tools.BeaconDBSetup;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.util.ReplicationType;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 *  Test class to test the FS Replication functionality.
 */
public class HDFSReplicationTest {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSReplicationTest.class);
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

    private PropertiesIgnoreCase sourceClusterProps = new PropertiesIgnoreCase();
    private PropertiesIgnoreCase targetClusterProps = new PropertiesIgnoreCase();
    private Properties fsSnapshotReplProps = new Properties();
    private Properties fsReplProps;
    private ClusterDao clusterDao = new ClusterDao();


    private String[][] sourceAttrs = {
            {Cluster.ClusterFields.FSENDPOINT.getName(), FS_ENDPOINT},
            {Cluster.ClusterFields.NAME.getName(), SOURCE},
            {Cluster.ClusterFields.DESCRIPTION.getName(), "source cluster"},
            {Cluster.ClusterFields.BEACONENDPOINT.getName(), BEACON_ENDPOINT},

    };

    private String[][] targetAttrs = {
            {Cluster.ClusterFields.FSENDPOINT.getName(), FS_ENDPOINT},
            {Cluster.ClusterFields.NAME.getName(), TARGET},
            {Cluster.ClusterFields.DESCRIPTION.getName(), "target cluster"},
            {Cluster.ClusterFields.BEACONENDPOINT.getName(), BEACON_ENDPOINT},

    };

    @BeforeClass
    public void init() throws Exception {
        RequestContext.setInitialValue();
        ServiceManager.getInstance().initialize(Collections.singletonList(BeaconStoreService.class.getName()), null);
        for (String[] sourceAttr : sourceAttrs) {
            sourceClusterProps.setProperty(sourceAttr[0], sourceAttr[1]);
        }

        for (String[] targetAttr : targetAttrs) {
            targetClusterProps.setProperty(targetAttr[0], targetAttr[1]);
        }

        // Empty table creation, not actual data is populated.
        createDBSchema();
        RequestContext.get().startTransaction();
        Cluster sourceCluster = ClusterBuilder.buildCluster(sourceClusterProps, SOURCE);
        clusterDao.submitCluster(sourceCluster);

        Cluster targetCluster = ClusterBuilder.buildCluster(targetClusterProps, TARGET);
        clusterDao.submitCluster(targetCluster);
        RequestContext.get().commitTransaction();

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
                {FSDRProperties.QUEUE_NAME.getName(), "default"},
                {FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false"},
                {FSDRProperties.JOB_TYPE.getName(), ReplicationType.FS.getName()},
                {FSDRProperties.SOURCE_CLUSTER_NAME.getName(), SOURCE},
                {FSDRProperties.TARGET_CLUSTER_NAME.getName(), TARGET},
        };

        for (String[] fsSnapshotReplAttr : fsSnapshotReplAttrs) {
            fsSnapshotReplProps.setProperty(fsSnapshotReplAttr[0], fsSnapshotReplAttr[1]);
        }

        fsReplProps = new Properties(fsSnapshotReplProps);
        fsReplProps.setProperty(FSDRProperties.JOB_NAME.getName(), "testFSReplication");
        fsReplProps.setProperty(FSDRProperties.SOURCE_DATASET.getName(), sourceDir.toString());
        fsReplProps.setProperty(FSDRProperties.TARGET_DATASET.getName(), targetDir.toString());

        try {
            baseDir = Files.createTempDirectory("test_snapshot-replication").toFile().getAbsoluteFile();
            miniDFSCluster = MiniHDFSClusterUtil.initMiniDfs(
                    MiniHDFSClusterUtil.SNAPSHOT_REPL_TEST_PORT1, baseDir);
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
    }

    @AfterClass
    public void teardown() {
        RequestContext.get().clear();
    }

    public static void createDBSchema() throws Exception {
        String currentDir = System.getProperty("user.dir");
        File hsqldbFile = new File(currentDir, "../src/sql/tables_hsqldb.sql");
        BeaconConfig.getInstance().getDbStore().setSchemaDirectory(hsqldbFile.getParent());
        BeaconDBSetup.setupDB();
    }

    @Test
    public void testSnapshotDirectory() throws Exception {
        String sourceDataset = FSUtils.getStagingUri(FS_ENDPOINT, sourceSnapshotDir.toString());
        String targetDataset = FSUtils.getStagingUri(FS_ENDPOINT, targetSnapshotDir + "_1");

        boolean isSourceDirSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(SOURCE, sourceDataset);
        Assert.assertEquals(isSourceDirSnapshottable, true);
        FileStatus fsStatus = miniDfs.getFileStatus(new Path(sourceDataset));
        Assert.assertEquals(miniDfs.exists(new Path(targetDataset)), false);
        Configuration conf = new Configuration();
        conf.set(BeaconConstants.FS_DEFAULT_NAME_KEY, FS_ENDPOINT);
        FSSnapshotUtils.createFSDirectory(miniDfs, fsStatus.getPermission(),
                fsStatus.getOwner(), fsStatus.getGroup(), targetDataset);
        if (isSourceDirSnapshottable) {
            FSSnapshotUtils.allowSnapshot(conf, targetDataset, miniDfs.getUri(),
                    ClusterHelper.getActiveCluster(TARGET));
        }
        Assert.assertEquals(miniDfs.exists(new Path(targetDataset)), true);
        isSourceDirSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(TARGET, targetDataset);
        Assert.assertEquals(isSourceDirSnapshottable, true);
        miniDfs.delete(new Path(targetDataset));
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
                Integer.parseInt(fsSnapshotReplProps.getProperty(FSDRProperties.JOB_FREQUENCY.getName()))
        ));

        Properties customProps = new Properties();
        customProps.setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false");
        replicationPolicy.setCustomProperties(customProps);

        boolean isSnapshotable = FSSnapshotUtils.isDirectorySnapshottable(
                SOURCE, TARGET,
                sourceClusterProps.getPropertyIgnoreCase(
                        Cluster.ClusterFields.FSENDPOINT.getName()) + sourceDataset,
                targetClusterProps.getPropertyIgnoreCase(
                        Cluster.ClusterFields.FSENDPOINT.getName()) + targetDataset);

        Assert.assertEquals(isSnapshotable, true);

        Assert.assertEquals(ReplicationUtils.getReplicationPolicyType(replicationPolicy), "FS_SNAPSHOT");

        customProps.setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
        replicationPolicy.setCustomProperties(customProps);
        Assert.assertEquals(ReplicationUtils.getReplicationPolicyType(replicationPolicy), "FS");

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
                Integer.parseInt(fsSnapshotReplProps.getProperty(FSDRProperties.JOB_FREQUENCY.getName()))
        ));

        isSnapshotable = FSSnapshotUtils.isDirectorySnapshottable(
                SOURCE, TARGET,
                sourceClusterProps.getPropertyIgnoreCase(
                        Cluster.ClusterFields.FSENDPOINT.getName()) + sourceDataset,
                targetClusterProps.getPropertyIgnoreCase(
                        Cluster.ClusterFields.FSENDPOINT.getName()) + targetDataset);

        Assert.assertEquals(isSnapshotable, false);

        customProps.setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false");
        nonSnapshotreplicationPolicy.setCustomProperties(customProps);
        Assert.assertEquals(ReplicationUtils.getReplicationPolicyType(nonSnapshotreplicationPolicy), "FS");
    }

    @Test
    public void testPerformReplication() throws Exception {
        String name = fsReplProps.getProperty(FSDRProperties.JOB_NAME.getName());
        String type = fsReplProps.getProperty(FSDRProperties.JOB_TYPE.getName());
        String identifier = name + "-" + type;
        ReplicationJobDetails jobDetails = new ReplicationJobDetails(identifier, name, type, fsReplProps);

        HDFSReplication fsImpl = new HDFSReplication(jobDetails);
        JobContext jobContext = new JobContext();
        jobContext.setJobInstanceId("/source/source/dummyRepl/0//00001@1");
        fsImpl.init(jobContext);
        List<String> directoryPath = Arrays.asList(".dir1", "_temporary", "test");
        // create dir(s), invoke copy, check file in target
        for (String dir: directoryPath) {
            Path path = new Path(sourceDir, dir);
            miniDfs.mkdir(path, fsPermission);
        }
        fsImpl.performCopy(jobContext, name, ReplicationMetrics.JobType.MAIN);
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "test")));
        Assert.assertFalse(miniDfs.exists(new Path(targetDir, ".dir1")));
        Assert.assertFalse(miniDfs.exists(new Path(targetDir, "_temporary")));

    }

    @Test
    public void testPerformSnapshotReplication() throws Exception {
        String name = fsSnapshotReplProps.getProperty(FSDRProperties.JOB_NAME.getName());
        String type = fsSnapshotReplProps.getProperty(FSDRProperties.JOB_TYPE.getName());
        String identifier = name + "-" + type;
        ReplicationJobDetails jobDetails = new ReplicationJobDetails(identifier, name, type, fsSnapshotReplProps);

        HDFSReplication fsImpl = new HDFSReplication(jobDetails);
        JobContext jobContext = new JobContext();
        jobContext.setJobInstanceId("/source/source/dummyRepl/0/1495688249800/00001@1");
        fsImpl.init(jobContext);
        // create dir1, create snapshot, invoke copy, check file in target, create snapshot on target
        Path dir1 = new Path(sourceSnapshotDir, "dir1");
        miniDfs.mkdir(dir1, fsPermission);
        miniDfs.createSnapshot(sourceSnapshotDir, "snapshot1");
        fsImpl.performCopy(jobContext, "snapshot1", ReplicationMetrics.JobType.MAIN);
        miniDfs.createSnapshot(targetSnapshotDir, "snapshot1");
        Assert.assertTrue(miniDfs.exists(new Path(targetSnapshotDir, "dir1")));

        // create dir2, create snapshot, invoke copy, check dir in target, create snapshot on target
        Path dir2 = new Path(sourceSnapshotDir, "dir2");
        miniDfs.mkdir(dir2, fsPermission);
        miniDfs.createSnapshot(sourceSnapshotDir, "snapshot2");
        fsImpl.performCopy(jobContext,  "snapshot2", ReplicationMetrics.JobType.MAIN);
        miniDfs.createSnapshot(targetSnapshotDir, "snapshot2");
        Assert.assertTrue(miniDfs.exists(new Path(targetSnapshotDir, "dir1")));
        Assert.assertTrue(miniDfs.exists(new Path(targetSnapshotDir, "dir2")));

        // delete dir1, create snapshot, invoke copy, check file not in target
        miniDfs.delete(dir1, true);
        miniDfs.createSnapshot(sourceSnapshotDir, "snapshot3");
        fsImpl.performCopy(jobContext,  "snapshot3", ReplicationMetrics.JobType.MAIN);
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
        Path snapshotDir = new Path(evictionDir, ".snapshot");
        FileStatus[] fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES);

        FSSnapshotUtils.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)",
                NUM_FILES + 1);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES);

        FSSnapshotUtils.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)",
                NUM_FILES - 1);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES - 1);

        FSSnapshotUtils.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)",
                2);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertTrue(fileStatuses.length >= 5);
    }

    @AfterClass
    public void cleanup() throws Exception {
        MiniHDFSClusterUtil.cleanupDfs(miniDFSCluster, baseDir);
    }
}
