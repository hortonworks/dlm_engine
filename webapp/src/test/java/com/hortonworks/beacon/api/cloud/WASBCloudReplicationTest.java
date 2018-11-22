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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Tests for wasb cloud replication.
 */
public class WASBCloudReplicationTest extends CloudReplicationTest {

    @Override
    public Properties getPropertiesTargetHiveCloudCluster() {
        Properties customProps = new Properties();
        customProps.setProperty("hive.metastore.warehouse.dir",
                testDataGenerator.getRandomString("wasb://dummy/warehouse"));
        customProps.setProperty("hive.metastore.uris", "jdbc:hive2://local-" + ClusterType.TARGET);
        customProps.setProperty("hive.warehouse.subdir.inherit.perms", "false");
        customProps.setProperty("hive.metastore.dml.events", "false");
        customProps.setProperty("hive.repl.replica.functions.root.dir", "wasb://dummy/warehouse-root");
        return customProps;
    }

    @Override
    public String getCloudDataSet() {
        return testDataGenerator.getRandomString("wasb://test-wasb/test-path");
    }

    @Override
    public CloudCred getCloudCred(String cloudCredName) {
        Map<CloudCred.Config, String> configs = new HashMap<>();
        configs.put(CloudCred.Config.WASB_ACCOUNT_NAME, "test-account");
        configs.put(CloudCred.Config.WASB_ACCESS_KEY, "test-access-key");
        CloudCred cloudCred = testDataGenerator.buildCloudCred(cloudCredName, CloudCred.Provider.WASB,
                CloudCred.AuthType.WASB_ACCESSKEY, configs);
        return cloudCred;
    }

    protected CloudCred createWasbCloudCredSasToken(String cloudCredName) {
        Map<CloudCred.Config, String> configs = new HashMap<>();
        configs.put(CloudCred.Config.WASB_CONTAINER_NAME, "test-container");
        configs.put(CloudCred.Config.WASB_ACCOUNT_NAME, "test-account-name");
        configs.put(CloudCred.Config.WASB_SAS_TOKEN, "test-sas-token");
        CloudCred cloudCred = testDataGenerator.buildCloudCred(cloudCredName, CloudCred.Provider.WASB,
                CloudCred.AuthType.WASB_SAS_TOKEN, configs);
        return cloudCred;
    }


    @Test
    public void testSubmitWasbSASCloudCred() throws Exception {
        CloudCred cloudCredSasToken = createWasbCloudCredSasToken(
                testDataGenerator.getRandomString("Submit-Cloud-Cred"));
        String cloudCredIdSas = targetClient.submitCloudCred(cloudCredSasToken);
        assertNotNull(cloudCredIdSas);
    }


    @Test
    public void testUpdateWasbCloudCred() throws Exception {
        String cloudCredName = testDataGenerator.getRandomString("Submit-Cloud-Cred");
        CloudCred cloudCred = getCloudCred(cloudCredName);
        String cloudCredIdAccessKey = targetClient.submitCloudCred(cloudCred);
        assertNotNull(cloudCredIdAccessKey);
        CloudCred savedCloudCred = targetClient.getCloudCred(cloudCredIdAccessKey);
        assertEquals(savedCloudCred.getName(), cloudCredName);
        assertEquals(savedCloudCred.getProvider().name(), CloudCred.Provider.WASB.name());
        assertEquals(savedCloudCred.getConfigs().get(CloudCred.Config.WASB_ACCOUNT_NAME), "test-account");

        CloudCred newCloudCred = createWasbCloudCredSasToken(cloudCredName);
        newCloudCred.getConfigs().put(CloudCred.Config.WASB_ACCOUNT_NAME, "updated-test-account");
        targetClient.updateCloudCred(cloudCredIdAccessKey, newCloudCred);
        CloudCred updatedCloudCred = targetClient.getCloudCred(cloudCredIdAccessKey);
        assertEquals(updatedCloudCred.getConfigs().get(CloudCred.Config.WASB_ACCOUNT_NAME), "updated-test-account");
    }
}
