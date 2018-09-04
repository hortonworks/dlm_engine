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
import static org.testng.Assert.assertTrue;

/**
 * Test for cluster APIs.
 */
public class ClusterResourceTest extends ResourceBaseTest {

    private Cluster sourceCluster;

    private Cluster targetCluster;

    @Test
    public void testSubmitClusterInvalidLocal() throws Exception {
        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetCluster.setFsEndpoint("SomeRandomEndpoint:8020");
        boolean expected = false;
        try {
            targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        } catch (BeaconClientException e) {
            if (e.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
                expected = true;
            }
        }
        assertTrue(expected);
    }

    @Test(dependsOnMethods = "testSubmitClusterInvalidLocal")
    public void testSubmitClusterInvalidRemoteAndPairFail() throws Exception {
        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        FileSystem fileSystem = mock(DistributedFileSystem.class);
        FileSystemClientFactory.setFileSystem(fileSystem);
        when(fileSystem.exists(new Path("/"))).thenThrow(ValidationException.class);
        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
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
        targetClient.deleteCluster(sourceCluster.getName());
        targetClient.deleteCluster(targetCluster.getName());
        reset(fileSystem);
    }

    @Test(dependsOnMethods = "testSubmitClusterInvalidRemoteAndPairFail")
    public void testUpdateClusterInvalidRemoteAndPairFail() throws Exception {
        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);

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
        targetClient.deleteCluster(sourceCluster.getName());
        targetClient.deleteCluster(targetCluster.getName());

    }

    @Test(dependsOnMethods = "testUpdateClusterInvalidRemoteAndPairFail")
    public void testSubmitCluster() throws Exception {
        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, true);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());
        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, false);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testPairCluster() throws Exception {
        targetClient.pairClusters(targetCluster.getName(), true);
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testListWithoutArguments() throws Exception {
        try {
            BeaconConfig.getInstance().getEngine().setKnoxProxyEnabled(true);
            ClusterList list = targetClient.getClusterList("", "name", "asc", 0, 10);
            //getBeaconEndpoint shouldn't fail when custom properties are not loaded
            list.getClusters()[0].getBeaconEndpoint();
        } finally {
            BeaconConfig.getInstance().getEngine().setKnoxProxyEnabled(false);
        }
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testListClustersBeforePairing() throws Exception{
        ClusterList clusterListWithAllDetails = targetClient.getClusterList("all", "name", "asc", 0, 10);
        Assert.assertEquals(2, clusterListWithAllDetails.getClusters().length);
        Assert.assertNotNull(clusterListWithAllDetails.getClusters()[0].getPeers());
        Assert.assertTrue(clusterListWithAllDetails.getClusters()[0].getPeers().size() == 0);
        Assert.assertNotNull(clusterListWithAllDetails.getClusters()[0].getPeersInfo());
    }

    @Test(dependsOnMethods = {"testListClustersBeforePairing", "testPairCluster"})
    public void testListClustersPostPairing() throws Exception {
        ClusterList clusterListWithAllDetails = targetClient.getClusterList("all", "name", "asc", 0, 10);
        Assert.assertEquals(2, clusterListWithAllDetails.getClusters().length);
        Assert.assertTrue(clusterListWithAllDetails.getClusters()[0].getPeers().size() != 0);
        Cluster[] clusters = clusterListWithAllDetails.getClusters();
        Assert.assertEquals(sourceCluster.getName(), clusters[1].getPeers().get(0));
        Assert.assertEquals(targetCluster.getName(), clusters[0].getPeers().get(0));
        Assert.assertEquals(3, clusters[0].getCustomProperties().size());
        ClusterList clusterListWithJustName = targetClient.getClusterList("name", "name", "asc", 0, 10);
        clusters = clusterListWithJustName.getClusters();
        Assert.assertEquals(0, clusters[0].getCustomProperties().size());
        ClusterList clusterList = targetClient.getClusterList("peers,tags,peersInfo", "name", "asc", 0, 10);
        Assert.assertEquals(2, clusterList.getClusters().length);
        clusters = clusterList.getClusters();
        Assert.assertEquals(sourceCluster.getName(), clusters[1].getPeers().get(0));
        Assert.assertEquals(targetCluster.getName(), clusters[0].getPeers().get(0));
        Assert.assertEquals(0, clusters[0].getCustomProperties().size());
        ClusterList clusterListWithoutAnyFields = targetClient.getClusterList("", "name", "asc", 0, 10);
        Assert.assertEquals(2, clusterListWithoutAnyFields.getClusters().length);
        Assert.assertEquals(0, clusters[0].getCustomProperties().size());
    }

    @Test(dependsOnMethods = {"testPairCluster"})
    public void testGetClusterInfo() throws Exception {
        Cluster cluster = targetClient.getCluster(sourceCluster.getName());
        Assert.assertEquals(sourceCluster.getName(), cluster.getName());
        Assert.assertEquals("testVal", cluster.getCustomProperties().getProperty("testKey"));
    }

    @Test(dependsOnMethods = "testSubmitCluster")
    public void testClusterStatus() throws Exception {
        Entity.EntityStatus statusResult = targetClient.getClusterStatus(sourceCluster.getName()).getStatus();
        Assert.assertEquals(Entity.EntityStatus.SUBMITTED, statusResult);
    }


    @Test(dependsOnMethods = {"testPairCluster"})
    public void testUnpairCluster() throws Exception {
        targetClient.unpairClusters(targetCluster.getName(), true);
    }

    @Test(dependsOnMethods = {"testPairCluster", "testGetClusterInfo",
            "testClusterStatus", "testListClustersPostPairing" })
    public void testDeleteCluster() throws Exception {
        deleteClusters();
        ClusterList clusterList = targetClient.getClusterList("name", "name", "asc", 0, 10);
        Assert.assertEquals(0, clusterList.getClusters().length);
    }

    private void deleteClusters() throws BeaconClientException {
        targetClient.deleteCluster(sourceCluster.getName());
        targetClient.deleteCluster(targetCluster.getName());
    }
}
