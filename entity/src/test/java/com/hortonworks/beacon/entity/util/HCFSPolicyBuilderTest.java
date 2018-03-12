/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.entity.util;


import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.PolicyBuilderTestUtil;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static com.hortonworks.beacon.entity.util.ReplicationPolicyBuilderTest.S3_TARGET_DATASET;
import static com.hortonworks.beacon.entity.util.ReplicationPolicyBuilderTest.SOURCE_DATASET;


/**
 * Test HCFS ReplicationPolicy builder.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ClusterHelper.class)
public class HCFSPolicyBuilderTest {

    private static final String CLOUD_CRED = "dummy";

    @BeforeClass
    public static void setup() throws Exception {
        List<String> services = new ArrayList<>();
        ServiceManager.getInstance().initialize(services, null);
        BeaconConfig.getInstance().getEngine().setLocalClusterName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
    }

    @Before
    public void methodSetup() throws BeaconException, IOException, URISyntaxException {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(ClusterHelper.class);
        Cluster cluster = new Cluster();
        cluster.setName(PolicyBuilderTestUtil.LOCAL_CLUSTER);
        PowerMockito.when(ClusterHelper.getLocalCluster()).thenReturn(cluster);
        PowerMockito.when(ClusterHelper.getActiveCluster(cluster.getName())).thenReturn(cluster);
    }

    @Test(expected = ValidationException.class)
    public void testValidateHCFSPolicyWithoutCloudCred() throws Exception {
        final String name = "hcfsPolicy";
        PropertiesIgnoreCase policyProps = PolicyBuilderTestUtil.buildPolicyProps(name,
                SOURCE_DATASET, S3_TARGET_DATASET, null);
        ReplicationPolicyBuilder.buildPolicy(policyProps, name, false);
    }
}
