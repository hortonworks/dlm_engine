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
