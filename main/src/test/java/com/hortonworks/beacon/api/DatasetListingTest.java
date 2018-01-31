/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.replication.fs.SnapshotListing;
import com.hortonworks.beacon.util.FSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.notNull;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests DatasetListing.
 */
@PrepareForTest({EncryptionZoneListing.class, SnapshotListing.class, FSUtils.class, FileSystem.class})
public class DatasetListingTest extends PowerMockTestCase {

    private Cluster cluster;

    @Mock
    private EncryptionZoneListing encryptionZoneListing;

    @Mock
    private SnapshotListing snapshotListing;

    @Mock
    private FileSystem fs;

    @BeforeClass
    public void setUp() throws Exception {
        PowerMockito.mockStatic(EncryptionZoneListing.class);
        PowerMockito.mockStatic(SnapshotListing.class);
        PowerMockito.mockStatic(FSUtils.class);
        fs = Mockito.mock(FileSystem.class);

        cluster = new Cluster();
        cluster.setName("src");
        cluster.setFsEndpoint("hdfs://localhost:8020");

    }

    @Test
    public void testListFiles() throws Exception {
        // base path is encrypted
        FileStatus[] fileStatuses = new FileStatus[2];
        fileStatuses[0] = new FileStatus();
        fileStatuses[1] = new FileStatus();
        fileStatuses[0].setPath(new Path("/data/encrypt/raw/1"));
        fileStatuses[1].setPath(new Path("/data/encrypt/2"));
        String path = "/data/encrypt/";
        String keyName = "default";
        String baseEncryptedPath = path;
        when(FSUtils.getStagingUri(cluster.getFsEndpoint(), path)).thenReturn(path);
        when(FSUtils.getFileSystem((String) notNull(), (Configuration) notNull(), anyBoolean())).thenReturn(fs);
        when(fs.listStatus(new Path(path))).thenReturn(fileStatuses);

        when(EncryptionZoneListing.get()).thenReturn(encryptionZoneListing);
        when(encryptionZoneListing.getBaseEncryptedPath(cluster.getName(), cluster.getFsEndpoint(),
                path)).thenReturn(baseEncryptedPath);
        when(encryptionZoneListing.isEncrypted(baseEncryptedPath)).thenReturn(true);
        when(encryptionZoneListing.getEncryptionKeyName(cluster.getName(), baseEncryptedPath)).thenReturn(keyName);

        when(SnapshotListing.get()).thenReturn(snapshotListing);
        when(snapshotListing.isSnapshottable(cluster.getName(), cluster.getFsEndpoint(), "/data2/snapshot"))
                .thenReturn(true);

        DatasetListing datasetListing = new DatasetListing();
        FileListResult fileListResult = datasetListing.listFiles(cluster, path);
        Assert.assertEquals(fileListResult.fileList[0].isEncrypted, true);
        Assert.assertEquals(fileListResult.fileList[0].encryptionKeyName, keyName);
        Assert.assertEquals(fileListResult.fileList[0].snapshottable, false);

        // Sub path is encrypted
        path = "/data1/encrypt/";
        fileStatuses = new FileStatus[2];
        fileStatuses[0] = new FileStatus();
        fileStatuses[1] = new FileStatus();
        String subPath = "/data1/encrypt/1";
        String subPath1 = "/data1/encrypt/2";
        fileStatuses[0].setPath(new Path(subPath));
        fileStatuses[1].setPath(new Path(subPath1));

        when(FSUtils.getStagingUri(cluster.getFsEndpoint(), path)).thenReturn(path);
        when(fs.listStatus(new Path(path))).thenReturn(fileStatuses);
        when(encryptionZoneListing.getBaseEncryptedPath(cluster.getName(), cluster.getFsEndpoint(),
                subPath)).thenReturn(subPath);
        when(encryptionZoneListing.isEncrypted(subPath)).thenReturn(true);
        when(encryptionZoneListing.getEncryptionKeyName(cluster.getName(), subPath)).thenReturn(keyName);

        fileListResult = datasetListing.listFiles(cluster, path);
        Assert.assertEquals(fileListResult.fileList[0].isEncrypted, true);
        Assert.assertEquals(fileListResult.fileList[0].encryptionKeyName, "default");
        Assert.assertEquals(fileListResult.fileList[1].isEncrypted, false);
        Assert.assertEquals(fileListResult.fileList[1].encryptionKeyName, null);
        Assert.assertEquals(fileListResult.fileList[1].snapshottable, false);

        // path is Snapshotted
        path = "/data2/snapshot/";
        fileStatuses = new FileStatus[1];
        fileStatuses[0] = new FileStatus();
        fileStatuses[0].setPath(new Path(path));
        when(FSUtils.getStagingUri(cluster.getFsEndpoint(), path)).thenReturn(path);
        when(fs.listStatus(new Path(path))).thenReturn(fileStatuses);
        fileListResult = datasetListing.listFiles(cluster, path);
        Assert.assertEquals(fileListResult.fileList[0].isEncrypted, false);
        Assert.assertEquals(fileListResult.fileList[0].encryptionKeyName, null);
        Assert.assertEquals(fileListResult.fileList[0].snapshottable, true);
    }
}
