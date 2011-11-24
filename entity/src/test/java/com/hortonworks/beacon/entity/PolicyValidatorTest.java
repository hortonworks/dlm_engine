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
package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.util.FSUtils;
import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Test Policy Validator.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ClusterHelper.class, FSUtils.class})
public class PolicyValidatorTest{

    @BeforeClass
    public static void setup() throws Exception {
        List<String> services = new ArrayList<>();
        ServiceManager.getInstance().initialize(services, null);
        BeaconConfig.getInstance().getEngine().setLocalClusterName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
    }

    @Before
    public void methodSetup() throws BeaconException, IOException, URISyntaxException {
        PowerMockito.mockStatic(ClusterHelper.class);
        PowerMockito.mockStatic(FSUtils.class);
        Cluster cluster = new Cluster();
        cluster.setName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
        PowerMockito.when(ClusterHelper.getLocalCluster()).thenReturn(cluster);
        PowerMockito.when(ClusterHelper.getActiveCluster(cluster.getName())).thenReturn(cluster);
        PowerMockito.when(FSUtils.isHCFS((Path) Mockito.any())).thenReturn(false);
    }

    @Test(expected = ValidationException.class)
    public void testValidatePolicyStartDateBeforeNow() throws Exception {
        final String name = "hdfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr", null, "backupCluster", "2016-11-26T23:54:50Z", null);
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);
        new PolicyValidator().validate(policy);
    }

    @Test(expected = ValidationException.class)
    public void testValidatePolicyEndDateBeforeStartDate() throws Exception {
        final String name = "hdfsPolicy-2";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                null, "backupCluster", "2017-11-26T23:54:50Z", "2015-11-26T23:54:50Z");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);
        new PolicyValidator().validate(policy);
    }

    @Test(expected = ValidationException.class)
    public void testValidatePolicyEndDateBeforeCurrent() throws Exception {
        final String name = "hdfsPolicy-1";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                null, "backupCluster", null, "2015-11-26T23:54:50Z");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);
        new PolicyValidator().validate(policy);
    }

    @Test
    public void testHS2ConnectionURL() throws Exception {
        String hs2URL = "jdbc:hive2://localhost:2181/;serviceDiscoveryMode=zooKeeper;"
                + "zooKeeperNamespace=hiveserver2";
        String queueName = "test";
        Properties properties = new Properties();
        properties.put(HiveDRProperties.QUEUE_NAME.getName(), queueName);
        String connString = HiveDRUtils.getHS2ConnectionUrl(hs2URL);
        Assert.assertEquals(connString, hs2URL);

        hs2URL = "hive2://localhost:10000";
        connString = HiveDRUtils.getHS2ConnectionUrl(hs2URL);
        Assert.assertEquals(connString, HiveDRUtils.JDBC_PREFIX + hs2URL);
    }
}
