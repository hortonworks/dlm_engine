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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test File listing.
 */
public class FileListingTest extends ResourceBaseTest {

    private Cluster targetCluster;

    @Test
    public void testSnapshotListing() throws Exception {
        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        String pathStr = "/root/data";
        String dataSet = FSUtils.getStagingUri(targetCluster.getFsEndpoint(), pathStr);
        final FileStatus[] fileStatuses = new FileStatus[4];
        fileStatuses[0] = new FileStatus();
        fileStatuses[1] = new FileStatus();
        fileStatuses[2] = new FileStatus();
        fileStatuses[3] = new FileStatus();
        fileStatuses[0].setPath(new Path("/root"));
        fileStatuses[1].setPath(new Path("/root/data"));
        fileStatuses[2].setPath(new Path("/root/data/sub1"));
        fileStatuses[3].setPath(new Path("/root/data/sub1/sub2"));
        RemoteIterator<FileStatus> remoteItr = new RemoteIterator<FileStatus>() {
            private int idx = 0;
            @Override
            public boolean hasNext() throws IOException {
                return idx < fileStatuses.length;
            }

            @Override
            public FileStatus next() throws IOException {
                return fileStatuses[idx++];
            }
        };
        when(targetFs.listStatusIterator(new Path(dataSet))).thenReturn(remoteItr);
        DistributedFileSystem hdfs = (DistributedFileSystem) targetFs;

        SnapshottableDirectoryStatus ssds = mock(SnapshottableDirectoryStatus.class);
        when(hdfs.getSnapshottableDirListing()).thenReturn(new SnapshottableDirectoryStatus[]{ssds});
        when(ssds.getFullPath()).thenReturn(new Path(pathStr));

        FileListResult fileListResult = targetClient.listFiles(dataSet);
        FileListResult.FileList[] fileList = fileListResult.fileList;
        Assert.assertFalse(fileList[0].snapshottable);
        Assert.assertTrue(fileList[1].snapshottable);
        Assert.assertFalse(fileList[2].snapshottable);
        Assert.assertFalse(fileList[3].snapshottable);
        targetClient.deleteCluster(targetCluster.getName());

    }
}
