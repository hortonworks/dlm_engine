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

package com.hortonworks.beacon.api.cloud;

import com.hortonworks.beacon.client.entity.CloudCred;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertNotNull;

/**
 * Tests for GCS cloud replication.
 */
public class GCSCloudReplicationTest extends CloudReplicationTest {

    private static final String GCS_PRIVATE_KEY_ID = "test-private-key-id";
    private static final String GCS_PRIVATE_KEY = "test-private-key";
    private static final String GCS_CLIENT_EMAIL= "testemail@testdomain.com";

    @Override
    public Properties getPropertiesTargetHiveCloudCluster() {
        Properties customProps = new Properties();
        customProps.setProperty("hive.metastore.warehouse.dir",
                testDataGenerator.getRandomString("gcs://dummy/warehouse"));
        customProps.setProperty("hive.metastore.uris", "jdbc:hive2://local-" + ClusterType.TARGET);
        customProps.setProperty("hive.warehouse.subdir.inherit.perms", "false");
        customProps.setProperty("hive.repl.replica.functions.root.dir", "gcs://dummy/warehouse-root");
        return customProps;
    }

    @Override
    public String getCloudDataSet() {
        return testDataGenerator.getRandomString("gcs://gcs_test_bucket/test-folder/");
    }

    @Override
    public CloudCred getCloudCred(String cloudCredName) {
        Map<CloudCred.Config, String> configs = new HashMap<>();
        configs.put(CloudCred.Config.GCS_PRIVATE_KEY_ID, GCS_PRIVATE_KEY_ID);
        configs.put(CloudCred.Config.GCS_PRIVATE_KEY, GCS_PRIVATE_KEY);
        configs.put(CloudCred.Config.GCS_CLIENT_EMAIL, GCS_CLIENT_EMAIL);
        CloudCred cloudCred = testDataGenerator.buildCloudCred(cloudCredName, CloudCred.Provider.GCS,
                CloudCred.AuthType.GCS_PRIVATEKEY, configs);
        return cloudCred;
    }

    @Test
    public void testSubmitGCSCloudCred() throws Exception {
        CloudCred gcsCloudCred = getCloudCred(
                testDataGenerator.getRandomString("Submit-Cloud-Cred"));
        String gcsCloudCredId = targetClient.submitCloudCred(gcsCloudCred);
        assertNotNull(gcsCloudCredId);
    }
}
