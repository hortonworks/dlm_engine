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

package com.hortonworks.beacon.replication.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.replication.fs.HDFSReplicationTest;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.fs.FSSnapshotUtils;
import com.hortonworks.beacon.replication.fs.MiniHDFSClusterUtil;
import com.hortonworks.beacon.service.ServiceManager;

/**
 * FSDRUtils Test class to test FileSystem functionality.
 */
public class FSDRUtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(FSDRUtilsTest.class);
    private static final String SOURCE = "source";
    private static final String FS_ENDPOINT = "hdfs://localhost:54137";
    private static final String BEACON_ENDPOINT = "http://localhost:55000";

    private DistributedFileSystem miniDfs;
    private Path sourceDir = new Path("/apps/beacon/snapshot-replication/sourceDir/");
    private Path targetDir = new Path("/apps/beacon/snapshot-replication/targetDir/");
    private File baseDir;
    private MiniDFSCluster miniDFSCluster;
    private Cluster cluster;
    private ClusterDao clusterDao = new ClusterDao();
    private String[][] sourceAttrs = {
            {Cluster.ClusterFields.FSENDPOINT.getName(), FS_ENDPOINT},
            {Cluster.ClusterFields.NAME.getName(), SOURCE},
            {Cluster.ClusterFields.DESCRIPTION.getName(), "source cluster"},
            {Cluster.ClusterFields.BEACONENDPOINT.getName(), BEACON_ENDPOINT},
    };
    private PropertiesIgnoreCase sourceClusterProps = new PropertiesIgnoreCase();

    @BeforeClass
    public void init() throws Exception {
        RequestContext.setInitialValue();
        ServiceManager.getInstance().initialize(Collections.singletonList(BeaconStoreService.class.getName()), null);
        HDFSReplicationTest.createDBSchema();
        for (String[] sourceAttr : sourceAttrs) {
            sourceClusterProps.setProperty(sourceAttr[0], sourceAttr[1]);
        }
        RequestContext.get().startTransaction();
        cluster = ClusterBuilder.buildCluster(sourceClusterProps, SOURCE);
        clusterDao.submitCluster(cluster);
        RequestContext.get().commitTransaction();
        try {
            ServiceManager.getInstance().initialize(null, null);
            baseDir = Files.createTempDirectory("test_snapshot-replication").toFile().getAbsoluteFile();
            miniDFSCluster = MiniHDFSClusterUtil.initMiniDfs(
                    MiniHDFSClusterUtil.SNAPSHOT_REPL_TEST_PORT2, baseDir);
            miniDfs = miniDFSCluster.getFileSystem();
            miniDfs.mkdirs(sourceDir);
            miniDfs.mkdirs(new Path(sourceDir, "dir1"));
            miniDfs.mkdirs(targetDir);

            miniDfs.allowSnapshot(sourceDir);
            miniDfs.allowSnapshot(targetDir);
        } catch (IOException ioe) {
            LOG.error("Exception occurred while creating directory on miniDFS : {} ", ioe);
        } catch (Exception e) {
            throw new Exception("Exception occurred while initializing the miniDFS", e);
        }
    }

    @Test
    public void testIsSnapShotsAvailable() throws Exception {
        boolean isSnapshotable = FSSnapshotUtils.isSnapShotsAvailable(cluster.getName(),
                new Path("hdfs://localhost:54137", sourceDir));
        Assert.assertTrue(isSnapshotable);
    }

    @Test
    public void testIsSnapShotsAvailableWithSubDir() throws Exception {
        Path subDirPath = new Path(sourceDir, "dir1");
        boolean isSnapshotable = FSSnapshotUtils.isSnapShotsAvailable(cluster.getName(),
                new Path("hdfs://localhost:54137", subDirPath));
        Assert.assertTrue(isSnapshotable);
    }

    @Test(expectedExceptions = BeaconException.class, expectedExceptionsMessageRegExp =
            "isSnapShotsAvailable: Path cannot be null or empty")
    public void testIsSnapShotsAvailableEmptyPath() throws Exception {
        FSSnapshotUtils.isSnapShotsAvailable(cluster.getName(), null);
    }

    @Test(expectedExceptions = BeaconException.class, expectedExceptionsMessageRegExp =
            "isSnapShotsAvailable: /apps/beacon/snapshot-replication/sourceDir is not fully qualified path")
    public void testIsSnapShotsAvailableNotFullPath() throws Exception {
        FSSnapshotUtils.isSnapShotsAvailable(cluster.getName(), sourceDir);
    }

    @AfterClass
    public void cleanup() throws Exception {
        MiniHDFSClusterUtil.cleanupDfs(miniDFSCluster, baseDir);
    }
}
