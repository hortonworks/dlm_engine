/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ClusterBuilder;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 *  Test class to test the FS TDE Replication functionality.
 */
public class FSTDEReplicationTest {

    private MiniDFSCluster srcDFSCluster;
    private DistributedFileSystem srcDFS;
    private DistributedFileSystem tgtDFS;
    private MiniDFSCluster tgtDFSCluster;
    private File srcBaseDir;
    private File tgtBaseDir;
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String SRC_FS_ENDPOINT = "hdfs://localhost:54138";
    private static final String TGT_FS_ENDPOINT = "hdfs://localhost:54139";
    private static final int SRC_PORT = 54138;
    private static final int TGT_PORT = 54139;
    private static final String BEACON_ENDPOINT = "http://localhost:55000";
    private Path replicationDir = new Path("/data/encrypt/");
    private String keyName = "default";
    private String jksFile = "test.jks";

    private PropertiesIgnoreCase sourceClusterProps = new PropertiesIgnoreCase();
    private PropertiesIgnoreCase targetClusterProps = new PropertiesIgnoreCase();
    private Properties fsReplProps = new Properties();
    private ClusterDao clusterDao = new ClusterDao();


    private String[][] sourceAttrs = {
            {Cluster.ClusterFields.FSENDPOINT.getName(), SRC_FS_ENDPOINT},
            {Cluster.ClusterFields.NAME.getName(), SOURCE},
            {Cluster.ClusterFields.DESCRIPTION.getName(), "source cluster"},
            {Cluster.ClusterFields.BEACONENDPOINT.getName(), BEACON_ENDPOINT},

    };

    private String[][] targetAttrs = {
            {Cluster.ClusterFields.FSENDPOINT.getName(), TGT_FS_ENDPOINT},
            {Cluster.ClusterFields.NAME.getName(), TARGET},
            {Cluster.ClusterFields.DESCRIPTION.getName(), "target cluster"},
            {Cluster.ClusterFields.BEACONENDPOINT.getName(), BEACON_ENDPOINT},

    };



    @BeforeClass
    public void init() throws Exception {
        ServiceManager.getInstance().initialize(Collections.singletonList(BeaconStoreService.SERVICE_NAME), null);
        for (String[] sourceAttr : sourceAttrs) {
            sourceClusterProps.setProperty(sourceAttr[0], sourceAttr[1]);
        }

        for (String[] targetAttr : targetAttrs) {
            targetClusterProps.setProperty(targetAttr[0], targetAttr[1]);
        }

        // Empty table creation, not actual data is populated.
        HDFSReplicationTest.createDBSchema();
        RequestContext.get().startTransaction();
        Cluster sourceCluster = ClusterBuilder.buildCluster(sourceClusterProps, SOURCE);
        clusterDao.submitCluster(sourceCluster);

        Cluster targetCluster = ClusterBuilder.buildCluster(targetClusterProps, TARGET);
        clusterDao.submitCluster(targetCluster);
        RequestContext.get().commitTransaction();

        String[][] fsTdeReplAttrs = {
                {FSDRProperties.JOB_NAME.getName(), "test-tde-fs"},
                {FSDRProperties.DISTCP_MAX_MAPS.getName(), "1"},
                {FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(), "10"},
                {FSDRProperties.JOB_FREQUENCY.getName(), "3600"},
                {FSDRProperties.SOURCE_NN.getName(), sourceClusterProps.getProperty(
                        Cluster.ClusterFields.FSENDPOINT.getName()), },
                {FSDRProperties.TARGET_NN.getName(), targetClusterProps.getProperty(
                        Cluster.ClusterFields.FSENDPOINT.getName()), },
                {FSDRProperties.SOURCE_DATASET.getName(), replicationDir.toString()},
                {FSDRProperties.TARGET_DATASET.getName(), replicationDir.toString()},
                {FSDRProperties.QUEUE_NAME.getName(), "default"},
                {FSDRProperties.JOB_TYPE.getName(), ReplicationType.FS.getName()},
                {FSDRProperties.SOURCE_CLUSTER_NAME.getName(), SOURCE},
                {FSDRProperties.TARGET_CLUSTER_NAME.getName(), TARGET},
                {FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true"},
        };

        for (String[] fsSnapshotReplAttr : fsTdeReplAttrs) {
            fsReplProps.setProperty(fsSnapshotReplAttr[0], fsSnapshotReplAttr[1]);
        }

        srcBaseDir = Files.createTempDirectory("src_tde-replication").toFile().getAbsoluteFile();
        tgtBaseDir = Files.createTempDirectory("tgt_tde-replication").toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, srcBaseDir.getAbsolutePath());
        conf.set("dfs.encryption.key.provider.uri", "jceks://file@" + this.getClass().getClassLoader()
                .getResource(jksFile).getPath());
        conf.set("hadoop.security.keystore.java-keystore-provider.password-file", "javakeystoreprovider.password");
        conf.setBoolean("dfs.namenode.delegation.token.always-use", true);
        srcDFSCluster = MiniHDFSClusterUtil.initMiniDfs(SRC_PORT, conf);
        srcDFS = srcDFSCluster.getFileSystem();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tgtBaseDir.getAbsolutePath());
        tgtDFSCluster = MiniHDFSClusterUtil.initMiniDfs(TGT_PORT, conf);
        tgtDFS = tgtDFSCluster.getFileSystem();
        srcDFS.mkdirs(replicationDir);
        tgtDFS.mkdirs(replicationDir);
        // As we are reusing the key, we don't need to create again.
        // DFSTestUtil.createKey(keyName, srcDFSCluster, conf);

        srcDFS.createEncryptionZone(replicationDir, keyName);
        tgtDFS.createEncryptionZone(replicationDir, keyName);
    }

    @Test
    public void testTDEReplication() throws Exception {
        Path srcfilePath = new Path(replicationDir, UUID.randomUUID().toString());
        DFSTestUtil.createFile(srcDFS, srcfilePath,
                1024, (short) 1, System.currentTimeMillis());
        String inputData = DFSTestUtil.readFile(srcDFS, srcfilePath);
        String name = fsReplProps.getProperty(FSDRProperties.JOB_NAME.getName());
        String type = fsReplProps.getProperty(FSDRProperties.JOB_TYPE.getName());
        String identifier = name + "-" + type;
        ReplicationJobDetails jobDetails = new ReplicationJobDetails(identifier, name, type, fsReplProps);

        HDFSReplication fsImpl = new HDFSReplication(jobDetails);
        JobContext jobContext = new JobContext();
        jobContext.setJobInstanceId("/source/source/dummyRepl/0//00001@1");
        fsImpl.init(jobContext);
        fsImpl.performCopy(jobContext, name, ReplicationMetrics.JobType.MAIN);
        String outputData = DFSTestUtil.readFile(tgtDFS, srcfilePath);
        Assert.assertEquals(inputData, outputData);
    }

    @Test
    public void testTDEReplicationWithReserved() throws Exception {
        fsReplProps.setProperty(FSDRProperties.TDE_SAMEKEY.getName(), "true");
        fsReplProps.setProperty("removeDeletedFiles", "false");

        Path srcfilePath = new Path(replicationDir, UUID.randomUUID().toString());
        DFSTestUtil.createFile(srcDFS, srcfilePath,
                1024, (short) 1, System.currentTimeMillis());
        String inputData = DFSTestUtil.readFile(srcDFS, srcfilePath);
        String name = fsReplProps.getProperty(FSDRProperties.JOB_NAME.getName());
        String type = fsReplProps.getProperty(FSDRProperties.JOB_TYPE.getName());
        String identifier = name + "-" + type;
        ReplicationJobDetails jobDetails = new ReplicationJobDetails(identifier, name, type, fsReplProps);

        HDFSReplication fsImpl = new HDFSReplication(jobDetails);
        JobContext jobContext = new JobContext();
        jobContext.setJobInstanceId("/source/source/dummyRepl/0//00001@1");
        fsImpl.init(jobContext);
        fsImpl.performCopy(jobContext, name, ReplicationMetrics.JobType.MAIN);
        String outputData = DFSTestUtil.readFile(tgtDFS, srcfilePath);
        Assert.assertEquals(inputData, outputData);
    }

    @AfterClass
    public void teardown() {
        RequestContext.get().clear();
    }

    @AfterClass
    public void cleanup() throws Exception {
        MiniHDFSClusterUtil.cleanupDfs(srcDFSCluster, srcBaseDir);
        MiniHDFSClusterUtil.cleanupDfs(tgtDFSCluster, tgtBaseDir);
    }
}
