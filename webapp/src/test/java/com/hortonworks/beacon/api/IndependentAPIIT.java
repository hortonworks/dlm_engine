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
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.test.BeaconIntegrationTest;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Integration test that starts new beacon service for every test.
 */
public class IndependentAPIIT extends BeaconIntegrationTest {
    public IndependentAPIIT() throws IOException {
        super();
    }

    @BeforeMethod
    public void setupBeaconServers(Method testMethod) throws Exception {
        super.setupBeaconServers(testMethod);
    }

    @AfterMethod
    public void teardownBeaconServers() throws Exception {
        super.teardownBeaconServers();
    }

    @Test
    public void testPairOnlyOneClusterKerberized() throws Exception {
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        Map<String, String> customProperties = new HashMap<>();
        String nnPricipal = "nnAdmin" + BeaconConstants.DOT_SEPARATOR + getTargetBeaconServerHostName();
        customProperties.put(BeaconConstants.NN_PRINCIPAL, nnPricipal);

        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, customProperties,
                false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, customProperties,
                true);
        pairClusterFailed(getSourceBeaconServer(), TARGET_CLUSTER);
    }

    @Test
    public void testUnpairClusters() throws Exception {
        String dataSet = "/tmp" + UUID.randomUUID().toString();
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        srcDfsCluster.getFileSystem().mkdirs(new Path(dataSet));
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        unpairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);

        // Pair, unpair and list
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        validateListClusterWithPeers(true);
        unpairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        validateListClusterWithPeers(false);
        // Same unpair operation again.
        unpairCluster(getSourceBeaconServer(), SOURCE_CLUSTER, TARGET_CLUSTER);
        validateListClusterWithPeers(false);

        unpairWrongClusters(getTargetBeaconServer(), OTHER_CLUSTER);


        // Pair cluster - submit policy - UnPair Cluster
        pairCluster(getTargetBeaconServer(), TARGET_CLUSTER, SOURCE_CLUSTER);
        String policyName = "policy";
        submitAndSchedule(policyName, 10, dataSet, null, new Properties());
        unpairClusterFailed(getTargetBeaconServer(), SOURCE_CLUSTER);
    }

    @Test
    public void testDeleteLocalCluster() throws Exception {
        String fsEndPoint = srcDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), fsEndPoint, true);
        String api = BASE_API + "cluster/delete/" + SOURCE_CLUSTER;
        deleteClusterAndValidate(api, getSourceBeaconServer(), SOURCE_CLUSTER);
    }

    @Test
    public void testDeleteCluster() throws Exception {
        String srcFsEndPoint = srcDfsCluster.getURI().toString();
        String tgtFsEndPoint = tgtDfsCluster.getURI().toString();
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getSourceBeaconServer(), srcFsEndPoint, true);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getSourceBeaconServer(), tgtFsEndPoint, false);
        submitCluster(SOURCE_CLUSTER, getSourceBeaconServer(), getTargetBeaconServer(), srcFsEndPoint, false);
        submitCluster(TARGET_CLUSTER, getTargetBeaconServer(), getTargetBeaconServer(), tgtFsEndPoint, true);

        //pair the clusters
        targetClient.pairClusters(SOURCE_CLUSTER, false);

        //Delete cluster fails if they are paired
        try {
            targetClient.deleteCluster(TARGET_CLUSTER);
            fail("Delete cluster should have failed");
        } catch (BeaconClientException e) {
            assertTrue(e.getMessage().contains("Can't delete cluster"));
            assertTrue(e.getMessage().contains("as its paired with"));
        }

        //Delete cluster succeeds when not paired
        targetClient.unpairClusters(SOURCE_CLUSTER, false);
        String api = BASE_API + "cluster/delete/" + TARGET_CLUSTER;
        deleteClusterAndValidate(api, getSourceBeaconServer(), TARGET_CLUSTER);
    }
}
