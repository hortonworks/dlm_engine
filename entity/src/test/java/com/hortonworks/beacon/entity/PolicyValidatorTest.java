/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);
        new PolicyValidator().validate(policy);
    }

    @Test(expected = ValidationException.class)
    public void testValidatePolicyEndDateBeforeStartDate() throws Exception {
        final String name = "hdfsPolicy-2";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                null, "backupCluster", "2017-11-26T23:54:50Z", "2015-11-26T23:54:50Z");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);
        new PolicyValidator().validate(policy);
    }

    @Test(expected = ValidationException.class)
    public void testValidatePolicyEndDateBeforeCurrent() throws Exception {
        final String name = "hdfsPolicy-1";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                "hdfs://localhost:54136/apps/dr",
                null, "backupCluster", null, "2015-11-26T23:54:50Z");
        ReplicationPolicy policy = ReplicationPolicyBuilder.buildPolicy(policyProps, name);
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
