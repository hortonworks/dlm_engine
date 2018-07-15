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

package com.hortonworks.beacon;

import com.hortonworks.beacon.api.LocalBeaconClient;
import com.hortonworks.beacon.api.ResourceBaseTest;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.entity.util.hive.HiveServerClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.tools.BeaconDBSetup;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Test data generator for local testing.
 */
public class LocalTestDataGenerator extends TestDataGenerator {
    private BeaconClient client = new LocalBeaconClient();

    @Mock
    private FileSystem sourceFs;
    @Mock
    private FileSystem targetFs;

    @Mock
    private HiveMetadataClient hiveMetadataClient;

    @Mock
    private HiveServerClient hiveServerClient;

    @Override
    public void init() throws BeaconException {
        MockitoAnnotations.initMocks(this);
        FileSystemClientFactory.setFileSystem(sourceFs);
        HiveClientFactory.setHiveMetadataClient(hiveMetadataClient);
        HiveClientFactory.setHiveServerClient(hiveServerClient);
        BeaconConfig.getInstance();
        BeaconDBSetup.setupDB();
        List<String> defaultServices = Arrays.asList(BeaconStoreService.class.getName());
        List<String> dependentServices = Arrays.asList(BeaconQuartzScheduler.class.getName());
        ServiceManager.getInstance().initialize(defaultServices, dependentServices);

    }

    @Override
    public Cluster getCluster(ResourceBaseTest.ClusterType clusterType, boolean isLocal) {
        Cluster cluster = new Cluster();
        cluster.setLocal(isLocal);
        cluster.setName(clusterType.getClusterName(isLocal));
        cluster.setDescription(randomString("description"));
        if (isLocal) {
            cluster.setFsEndpoint("file:///");
        } else {
            cluster.setFsEndpoint("hdfs://local-" + clusterType);
        }
        cluster.setHsEndpoint("jdbc:hive2://local-" + clusterType);
        cluster.setBeaconEndpoint("http://beacon-" + cluster);
        cluster.setTags(Arrays.asList("test", "local", "IT"));
        Properties properties = new Properties();
        properties.put("testKey", "testVal");
        cluster.setCustomProperties(properties);
        return cluster;
    }

    @Override
    public BeaconClient getClient(ResourceBaseTest.ClusterType clusterType) {
        return client;
    }

    @Override
    public FileSystem getFileSystem(ResourceBaseTest.ClusterType clusterType) {
        switch (clusterType) {
            case SOURCE:
                return sourceFs;
            case TARGET:
                return targetFs;
            default:
                throw new IllegalStateException("Unhandled cluster type " + clusterType);
        }
    }

    private String randomString(String prefix) {
        return prefix + RandomStringUtils.randomAlphanumeric(10);
    }
}
