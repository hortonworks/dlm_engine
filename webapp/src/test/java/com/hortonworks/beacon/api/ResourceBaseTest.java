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

import com.hortonworks.beacon.TestDataGenerator;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

/**
 * Base class for tests.
 */
public abstract class ResourceBaseTest {

    protected static final String SOURCE_DIR = "/apps/beacon/replication/sourceDir/";

    private static final String SOURCE_CLUSTER_NAME = "cluster-src";
    private static final String TARGET_CLUSTER_NAME = "cluster-tgt";

    /**
     * Enum for source/target.
     */
    public enum ClusterType {
        SOURCE {
            @Override
            public String getClusterName(boolean isLocal) {
                if (isLocal) {
                    return "cluster-local";
                } else {
                    return SOURCE_CLUSTER_NAME;
                }
            }
        },
        TARGET {
            @Override
            public String getClusterName(boolean isLocal) {
                if (isLocal) {
                    return "cluster-local";
                } else {
                    return TARGET_CLUSTER_NAME;
                }
            }
        };

        public abstract String getClusterName(boolean isLocal);

    }

    protected TestDataGenerator testDataGenerator;

    protected BeaconClient sourceClient;
    protected BeaconClient targetClient;
    protected FileSystem sourceFs;
    protected FileSystem targetFs;

    @BeforeClass
    public void setup() throws Exception {
        System.setProperty("beacon.test.local", "true");
        testDataGenerator = TestDataGenerator.getTestDataGenerator();
        testDataGenerator.init();
        sourceClient = testDataGenerator.getClient(ClusterType.SOURCE);
        targetClient = testDataGenerator.getClient(ClusterType.TARGET);
        sourceFs = testDataGenerator.getFileSystem(ClusterType.SOURCE);
        targetFs = testDataGenerator.getFileSystem(ClusterType.TARGET);
    }

    /**
     * Interface for implementing any condition.
     */
    public interface Condition {
        boolean exit() throws BeaconClientException;
    }


    protected void waitOnCondition(long timeout, String message, Condition condition)
            throws InterruptedException, BeaconClientException {
        long currentTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < currentTime + timeout) {
            if (condition.exit()) {
                return;
            }
            Thread.sleep(100);
        }
        if (!condition.exit()) {
            Assert.fail("Timed out waiting for "+ message);
        }
    }

    protected PolicyInstanceList.InstanceElement getFirstInstance(BeaconClient client, String policyName)
            throws BeaconClientException {
        PolicyInstanceList policyInstanceList = client.listPolicyInstances(policyName);
        if (policyInstanceList.getElements().length > 0) {
            return policyInstanceList.getElements()[policyInstanceList.getElements().length - 1];
        }
        return null;
    }
}
