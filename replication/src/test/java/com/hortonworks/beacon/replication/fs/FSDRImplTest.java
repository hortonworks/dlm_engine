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

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.utils.ReplicationDistCpOption;
import com.hortonworks.beacon.replication.utils.ReplicationOptionsUtils;
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


public class FSDRImplTest {

    private static final Logger LOG = LoggerFactory.getLogger(FSDRImplTest.class);

    private MiniDFSCluster miniDFSCluster;
    private DistributedFileSystem miniDfs;
    private File baseDir;
    private Path sourceDir = new Path("/apps/beacon/snapshot-replication/sourceDir/");
    private Path targetDir = new Path("/apps/beacon/snapshot-replication/targetDir/");
    private Path evictionDir = new Path("/apps/beacon/snapshot-eviction/");
    private static final int NUM_FILES = 7;
    private FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    private Properties fsProps = new Properties();
    ReplicationJobDetails jobDetails = new ReplicationJobDetails();

    private String[][] props = {
            { FSDRProperties.JOB_NAME.getName(), "snapshotJobName"},
            { FSDRProperties.DISTCP_MAX_MAPS.getName(), "1"},
            { FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(), "100"},
            { FSDRProperties.JOB_FREQUENCY.getName(), "3600"},
            { FSDRProperties.SOURCE_NN.getName(), "hdfs://localhost:54136"},
            { FSDRProperties.TARGET_NN.getName(), "hdfs://localhost:54136"},
            { FSDRProperties.SOURCE_DIR.getName(), "/apps/beacon/snapshot-replication/sourceDir/"},
            { FSDRProperties.TARGET_DIR.getName(), "/apps/beacon/snapshot-replication/targetDir/"},
            { ReplicationDistCpOption.DISTCP_OPTION_IGNORE_ERRORS.getName(), "false"},
            { ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_ACL.getName(), "false"},
            { FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false"},
            { FSDRProperties.JOB_TYPE.getName(), "FS",}
    };


    @BeforeClass
    public void init() {
        for (int i=0;i<props.length;i++) {
            fsProps.setProperty(props[i][0], props[i][1]);
        }

        jobDetails.setProperties(fsProps);
        try {
            baseDir = Files.createTempDirectory("test_snapshot-replication").toFile().getAbsoluteFile();
            miniDFSCluster = MiniHDFSClusterUtil.initMiniDfs(MiniHDFSClusterUtil.SNAPSHOT_REPL_TEST_PORT, baseDir);
            miniDfs = miniDFSCluster.getFileSystem();
            miniDfs.mkdirs(sourceDir);
            miniDfs.mkdirs(targetDir);

            miniDfs.allowSnapshot(sourceDir);
            miniDfs.allowSnapshot(targetDir);
        } catch (IOException ioe) {
            LOG.error("Exception occurred while creating directory on miniDFS : {} ", ioe);
        } catch (Exception e) {
            LOG.error("Exception occurred while initializing the miniDFS : {} ", e);
        }
    }

    @Test
    public void testPerformReplication() throws Exception {
        DistributedFileSystem sourceFs = FSUtils.getSourceFileSystem(jobDetails.getProperties().
                getProperty(FSDRProperties.SOURCE_NN.getName()), new Configuration());
        DistributedFileSystem targetFs = FSUtils.getTargetFileSystem(jobDetails.getProperties().
                        getProperty(FSDRProperties.TARGET_NN.getName()),
                new Configuration());

        FSDRImpl fsImpl = new FSDRImpl(jobDetails);
        fsImpl.establishConnection();
        CommandLine cmd = ReplicationOptionsUtils.getCommand(jobDetails.getProperties());
        String currentSnapshotName = FSUtils.SNAPSHOT_PREFIX + jobDetails.getName() + "-" + System.currentTimeMillis();
        // create dir1, create snapshot, invoke copy, check file in target, create snapshot on target
        Path dir1 = new Path(sourceDir, "dir1");
        miniDfs.mkdir(dir1, fsPermission);
        miniDfs.createSnapshot(sourceDir, "snapshot1");
        fsImpl.invokeCopy(cmd, sourceFs, targetFs, "snapshot1");
        miniDfs.createSnapshot(targetDir, "snapshot1");
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "dir1")));

        // create dir2, create snapshot, invoke copy, check dir in target, create snapshot on target
        Path dir2 = new Path(sourceDir, "dir2");
        miniDfs.mkdir(dir2, fsPermission);
        miniDfs.createSnapshot(sourceDir, "snapshot2");
        fsImpl.invokeCopy(cmd, sourceFs, targetFs, "snapshot2");
        miniDfs.createSnapshot(targetDir, "snapshot2");
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "dir1")));
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "dir2")));

        // delete dir1, create snapshot, invoke copy, check file not in target
        miniDfs.delete(dir1, true);
        miniDfs.createSnapshot(sourceDir, "snapshot3");
        fsImpl.invokeCopy(cmd, sourceFs, targetFs, "snapshot3");
        miniDfs.createSnapshot(targetDir, "snapshot3");
        Assert.assertFalse(miniDfs.exists(new Path(targetDir, "dir1")));
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "dir2")));

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

        FSDRImpl fsImpl = new FSDRImpl(jobDetails);
        Path snapshotDir = new Path(evictionDir, ".snapshot");
        FileStatus[] fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES);

        fsImpl.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", NUM_FILES + 1);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES);

        fsImpl.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", NUM_FILES - 1);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES - 1);

        fsImpl.evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", 2);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertTrue(fileStatuses.length >= 5);
    }

    @AfterClass
    public void cleanup() throws Exception {
        MiniHDFSClusterUtil.cleanupDfs(miniDFSCluster, baseDir);
    }
}