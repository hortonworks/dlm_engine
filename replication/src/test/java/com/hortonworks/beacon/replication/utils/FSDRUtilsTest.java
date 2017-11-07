/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hortonworks.beacon.XTestCase;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.fs.FSSnapshotUtils;
import com.hortonworks.beacon.replication.fs.MiniHDFSClusterUtil;

/**
 * FSDRUtils Test class to test FileSystem functionality.
 */
public class FSDRUtilsTest extends XTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(FSDRUtilsTest.class);

    private DistributedFileSystem miniDfs;
    private Path sourceDir = new Path("/apps/beacon/snapshot-replication/sourceDir/");
    private Path targetDir = new Path("/apps/beacon/snapshot-replication/targetDir/");

    @BeforeClass
    public void init() {
        try {
            initializeServices(null);
            File baseDir = Files.createTempDirectory("test_snapshot-replication").toFile().getAbsoluteFile();
            MiniDFSCluster miniDFSCluster = MiniHDFSClusterUtil.initMiniDfs(
                    MiniHDFSClusterUtil.SNAPSHOT_REPL_TEST_PORT, baseDir);
            miniDfs = miniDFSCluster.getFileSystem();
            miniDfs.mkdirs(sourceDir);
            miniDfs.mkdirs(new Path(sourceDir, "dir1"));
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
    public void testIsSnapShotsAvailable() throws Exception {
        boolean isSnapshotable = FSSnapshotUtils.isSnapShotsAvailable(miniDfs,
                new Path("hdfs://localhost:54136", sourceDir));
        Assert.assertTrue(isSnapshotable);
    }

    @Test
    public void testIsSnapShotsAvailableWithSubDir() throws Exception {
        Path subDirPath = new Path(sourceDir, "dir1");
        boolean isSnapshotable = FSSnapshotUtils.isSnapShotsAvailable(miniDfs,
                new Path("hdfs://localhost:54136", subDirPath));
        Assert.assertTrue(isSnapshotable);
    }

    @Test(expectedExceptions = BeaconException.class, expectedExceptionsMessageRegExp =
            "isSnapShotsAvailable: Path cannot be null or empty")
    public void testIsSnapShotsAvailableEmptyPath() throws Exception {
        FSSnapshotUtils.isSnapShotsAvailable(miniDfs, null);
    }

    @Test(expectedExceptions = BeaconException.class, expectedExceptionsMessageRegExp =
            "isSnapShotsAvailable: /apps/beacon/snapshot-replication/sourceDir is not fully qualified path")
    public void testIsSnapShotsAvailableNotFullPath() throws Exception {
        FSSnapshotUtils.isSnapShotsAvailable(miniDfs, sourceDir);
    }
}
