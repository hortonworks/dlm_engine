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

import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.PeerInfo;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.util.ClusterStatus;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.testng.annotations.Test;
import javax.ws.rs.core.Response;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Test for cluster APIs.
 */
public class ClusterResourceTest extends ResourceBaseTest {

    @Test
    public void testSubmitClusterInvalidLocal() throws Exception {
        Cluster cluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        cluster.setFsEndpoint("SomeRandomEndpoint:8020");
        boolean expected = false;
        try {
            targetClient.submitCluster(cluster.getName(), cluster.asProperties());
        } catch (BeaconClientException e) {
            if (e.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
                expected = true;
            }
        }
        assertTrue(expected);
    }

    @Test
    public void testMinimalCluster() throws Exception {
        Cluster cluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        cluster.setFsEndpoint(null);
        targetClient.submitCluster(cluster.getName(), cluster.asProperties());
        FileListResult files = targetClient.listFiles("/");
        assertNull(files.fileList);
        deleteClusters(cluster.getName());
    }

    @Test
    public void testSubmitClusterInvalidRemoteAndPairFail() throws Exception {
        Cluster targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        FileSystem fileSystem = mock(DistributedFileSystem.class);
        FileSystemClientFactory.setFileSystem(fileSystem);
        when(fileSystem.exists(new Path("/"))).thenThrow(ValidationException.class);
        Cluster sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        boolean expected = false;
        try {
            targetClient.pairClusters(sourceCluster.getName(), false);
        } catch (BeaconClientException e) {
            if (e.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
                expected = true;
            }
        }
        assertTrue(expected);
        deleteClusters(sourceCluster.getName(), targetCluster.getName());
        reset(fileSystem);
    }

    @Test
    public void testUpdateClusterInvalidRemoteAndPairFail() throws Exception {
        Cluster targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        Cluster sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);

        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());

        targetClient.pairClusters(sourceCluster.getName(), true);

        Cluster sourceClusterRetrieved = targetClient.getCluster(sourceCluster.getName());
        List<PeerInfo> peersInfo = sourceClusterRetrieved.getPeersInfo();
        assertEquals(peersInfo.get(0).getPairStatus(), ClusterStatus.PAIRED.name());

        FileSystem fileSystem = mock(DistributedFileSystem.class);
        FileSystemClientFactory.setFileSystem(fileSystem);
        when(fileSystem.exists(new Path("/"))).thenThrow(ValidationException.class);
        String oldSourceFsEndpoint = sourceCluster.getFsEndpoint();
        sourceCluster.setFsEndpoint("hdfs://SomeRandomInvalidFSEndpoint:8020");
        targetClient.updateCluster(sourceCluster.getName(), sourceCluster.asProperties());

        sourceClusterRetrieved = targetClient.getCluster(sourceCluster.getName());
        peersInfo = sourceClusterRetrieved.getPeersInfo();
        assertEquals(peersInfo.get(0).getPairStatus(), ClusterStatus.SUSPENDED.name());
        reset(fileSystem);

        sourceCluster.setFsEndpoint(oldSourceFsEndpoint);
        targetClient.updateCluster(sourceCluster.getName(), sourceCluster.asProperties());

        sourceClusterRetrieved = targetClient.getCluster(sourceCluster.getName());
        peersInfo = sourceClusterRetrieved.getPeersInfo();
        assertEquals(peersInfo.get(0).getPairStatus(), ClusterStatus.PAIRED.name());

        targetClient.unpairClusters(sourceCluster.getName(), true);
        deleteClusters(sourceCluster.getName(), targetCluster.getName());
    }

    @Test
    public void testSubmitAndPairCluster() throws Exception {
        Cluster sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        Cluster targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.pairClusters(sourceCluster.getName(), true);
        targetClient.unpairClusters(sourceCluster.getName(), true);
        //ClusterList list = targetClient.getClusterList("", "name", "asc", 0, 10);
        //System.out.println(list.getClusters()[0].getBeaconEndpoint());

        deleteClusters(sourceCluster.getName(), targetCluster.getName());
    }

    @Test
    public void testListClusterWithoutArguments() throws Exception {
        Cluster sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        try {
            targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
            BeaconConfig.getInstance().getEngine().setKnoxProxyEnabled(true);
            ClusterList list = targetClient.getClusterList("", "name", "asc", 0, 10);
            //getBeaconEndpoint shouldn't fail when custom properties are not loaded
            list.getClusters()[0].getBeaconEndpoint();
        } finally {
            BeaconConfig.getInstance().getEngine().setKnoxProxyEnabled(false);
            deleteClusters(sourceCluster.getName());
        }
    }

    @Test
    public void testListClustersBeforePairing() throws Exception{
        Cluster sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        Cluster targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        ClusterList clusterListWithAllDetails = targetClient.getClusterList("all", "name", "asc", 0, 10);
        Assert.assertEquals(2, clusterListWithAllDetails.getClusters().length);
        Assert.assertNotNull(clusterListWithAllDetails.getClusters()[0].getPeers());
        Assert.assertTrue(clusterListWithAllDetails.getClusters()[0].getPeers().size() == 0);
        Assert.assertNotNull(clusterListWithAllDetails.getClusters()[0].getPeersInfo());

        Cluster cluster = clusterListWithAllDetails.getClusters()[0];
        assertEquals(cluster.getClass().getName(), Cluster.class.getName());
        deleteClusters(sourceCluster.getName(), targetCluster.getName());
    }

    @Test
    public void testListClustersPostPairing() throws Exception {
        Cluster sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        Cluster targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.pairClusters(sourceCluster.getName(), true);
        ClusterList clusterListWithAllDetails = targetClient.getClusterList("all", "name", "asc", 0, 10);
        Assert.assertEquals(2, clusterListWithAllDetails.getClusters().length);
        Assert.assertTrue(clusterListWithAllDetails.getClusters()[0].getPeers().size() != 0);
        Cluster[] clusters = clusterListWithAllDetails.getClusters();
        Assert.assertEquals(targetCluster.getName(), clusters[1].getPeers().get(0));
        Assert.assertEquals(sourceCluster.getName(), clusters[0].getPeers().get(0));
        Assert.assertEquals(3, clusters[0].getCustomProperties().size());
        ClusterList clusterListWithJustName = targetClient.getClusterList("name", "name", "asc", 0, 10);
        clusters = clusterListWithJustName.getClusters();
        Assert.assertEquals(0, clusters[0].getCustomProperties().size());
        ClusterList clusterList = targetClient.getClusterList("peers,tags,peersInfo", "name", "asc", 0, 10);
        Assert.assertEquals(2, clusterList.getClusters().length);
        clusters = clusterList.getClusters();
        Assert.assertEquals(targetCluster.getName(), clusters[1].getPeers().get(0));
        Assert.assertEquals(sourceCluster.getName(), clusters[0].getPeers().get(0));
        Assert.assertEquals(0, clusters[0].getCustomProperties().size());
        ClusterList clusterListWithoutAnyFields = targetClient.getClusterList("", "name", "asc", 0, 10);
        Assert.assertEquals(2, clusterListWithoutAnyFields.getClusters().length);
        Assert.assertEquals(0, clusters[0].getCustomProperties().size());
        targetClient.unpairClusters(sourceCluster.getName(), true);
        deleteClusters(sourceCluster.getName(), targetCluster.getName());

    }

    @Test
    public void testGetClusterInfo() throws Exception {
        Cluster sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        Cluster cluster = targetClient.getCluster(sourceCluster.getName());
        Assert.assertEquals(sourceCluster.getName(), cluster.getName());
        Assert.assertEquals("testVal", cluster.getCustomProperties().getProperty("testKey"));
        deleteClusters(sourceCluster.getName());
    }

    @Test
    public void testClusterStatus() throws Exception {
        Cluster sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, true);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        Entity.EntityStatus statusResult = targetClient.getClusterStatus(sourceCluster.getName()).getStatus();
        Assert.assertEquals(Entity.EntityStatus.SUBMITTED, statusResult);
        deleteClusters(sourceCluster.getName());
    }

    private void deleteClusters(String... clusterNames) throws BeaconClientException {
        for (String cluster : clusterNames) {
            targetClient.deleteCluster(cluster);
        }
    }
}
