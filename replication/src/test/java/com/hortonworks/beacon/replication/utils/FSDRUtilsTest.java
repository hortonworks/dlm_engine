package com.hortonworks.beacon.replication.utils;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.fs.MiniHDFSClusterUtil;
import com.hortonworks.beacon.util.FSUtils;
import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FSDRUtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(FSDRUtilsTest.class);

    private MiniDFSCluster miniDFSCluster;
    private DistributedFileSystem miniDfs;
    private File baseDir;
    private Path sourceDir = new Path("/apps/beacon/snapshot-replication/sourceDir/");
    private Path targetDir = new Path("/apps/beacon/snapshot-replication/targetDir/");

    @BeforeClass
    public void init() {
        try {
            baseDir = Files.createTempDirectory("test_snapshot-replication").toFile().getAbsoluteFile();
            miniDFSCluster = MiniHDFSClusterUtil.initMiniDfs(MiniHDFSClusterUtil.SNAPSHOT_REPL_TEST_PORT, baseDir);
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
        BeaconConfig.getInstance().getEngine().setInTestMode(true);
        BeaconConfig.getInstance().getEngine().setInTestMode(true);
    }

    @Test
    public void testIsSnapShotsAvailable() throws Exception {
        boolean isSnapshotable = FSUtils.isSnapShotsAvailable(miniDfs, new Path("hdfs://localhost:54136", sourceDir));
        Assert.assertTrue(isSnapshotable);
    }

    @Test
    public void testIsSnapShotsAvailableWithSubDir() throws Exception {
        Path subDirPath = new Path(sourceDir, "dir1");
        boolean isSnapshotable = FSUtils.isSnapShotsAvailable(miniDfs,
                new Path("hdfs://localhost:54136", subDirPath));
        Assert.assertTrue(isSnapshotable);
    }

    @Test(expectedExceptions = BeaconException.class, expectedExceptionsMessageRegExp = "isSnapShotsAvailable: Path cannot be null or empty")
    public void testIsSnapShotsAvailableEmptyPath() throws Exception {
        FSUtils.isSnapShotsAvailable(miniDfs, null);
    }

    @Test(expectedExceptions = BeaconException.class, expectedExceptionsMessageRegExp =
            "isSnapShotsAvailable: /apps/beacon/snapshot-replication/sourceDir is not fully qualified path")
    public void testIsSnapShotsAvailableNotFullPath() throws Exception {
        FSUtils.isSnapShotsAvailable(miniDfs, sourceDir);
    }
}
