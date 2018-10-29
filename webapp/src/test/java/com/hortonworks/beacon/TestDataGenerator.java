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

import com.hortonworks.beacon.api.ResourceBaseTest;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * test data generator interface for local and ambari based cluster.
 */
public abstract class TestDataGenerator {

    protected BeaconClient localBeaconClient;

    protected BeaconClient targetBeaconClient;

    protected FileSystem sourceFs;
    protected FileSystem targetFs;

    protected HiveMetadataClient hiveMetadataClient;

    public static TestDataGenerator getTestDataGenerator() {
        if (System.getProperty("beacon.test.local", "true").equals("true")) {
            return new LocalTestDataGenerator();
        }
        return new AmbariBasedTestDataGenerator();
    }

    public abstract void init() throws Exception;

    public abstract Cluster getCluster(ResourceBaseTest.ClusterType clusterType, boolean isLocal);

    public abstract BeaconClient getClient(ResourceBaseTest.ClusterType clusterType);

    public abstract FileSystem getFileSystem(ResourceBaseTest.ClusterType clusterType);

    public abstract void createFSMocks(String path) throws IOException;

    public abstract void createHiveMocks(String dbName) throws BeaconException;

    private void populateCustomProperties(ReplicationPolicy policy) {
        policy.getCustomProperties().setProperty("distcpMaxMaps", "1");
        policy.getCustomProperties().setProperty("distcpMapBandwidth", "10");
        policy.getCustomProperties().setProperty("sourceSnapshotRetentionAgeLimit", "10");
        policy.getCustomProperties().setProperty("sourceSnapshotRetentionNumber", "1");
        policy.getCustomProperties().setProperty("targetSnapshotRetentionAgeLimit", "10");
        policy.getCustomProperties().setProperty("targetSnapshotRetentionNumber", "1");
        policy.getCustomProperties().setProperty("tags", "owner=producer@xyz.com,component=sales");
        policy.getCustomProperties().setProperty("plugins", "RANGER,ATLAS");
        policy.getCustomProperties().setProperty("retryAttempts", "0");
        policy.getCustomProperties().setProperty("retryDelay", "120");
        policy.getCustomProperties().setProperty("user", System.getProperty("user.name"));
    }

    public ReplicationPolicy getPolicy() {
        return getPolicy(getRandomString("Policy"), getRandomString("Path"),
                getRandomString("Path"), "FS", 120,
                getCluster(ResourceBaseTest.ClusterType.SOURCE, false).getName(),
                getCluster(ResourceBaseTest.ClusterType.TARGET, true).getName(),
                new HashMap<String, String>());
    }

    public ReplicationPolicy getPolicy(String policyName, String replicationPath) {
        return getPolicy(policyName, replicationPath, replicationPath, "FS", 120,
                getCluster(ResourceBaseTest.ClusterType.SOURCE, false).getName(),
                getCluster(ResourceBaseTest.ClusterType.TARGET, true).getName(),
                new HashMap<String, String>());
    }

    public ReplicationPolicy getPolicy(String policyName, String replicationPath, HashMap<String, String> custProps) {
        return getPolicy(policyName, replicationPath, replicationPath, "FS", 120,
                getCluster(ResourceBaseTest.ClusterType.SOURCE, false).getName(),
                getCluster(ResourceBaseTest.ClusterType.TARGET, true).getName(),
                custProps);
    }

    public ReplicationPolicy getPolicy(String policyName, String replicationPath, String type) {
        return getPolicy(policyName, replicationPath, replicationPath, type, 120,
                getCluster(ResourceBaseTest.ClusterType.SOURCE, false).getName(),
                getCluster(ResourceBaseTest.ClusterType.TARGET, true).getName(),
                new HashMap<String, String>());
    }

    public ReplicationPolicy getPolicy(String policyName, String sourceDataSet, String targetDataSet,
                                       String type, int frequency, String sourceCluster,
                                       String targetCluster, Map<String, String> extraProps) {
        ReplicationPolicy policy = new ReplicationPolicy();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, 1);
        policy.setStartTime(calendar.getTime());
        policy.setName(policyName);
        policy.setDescription("Beacon test policy");
        policy.setType(type);
        policy.setFrequencyInSec(frequency);
        policy.setSourceDataset(sourceDataSet);
        policy.setTargetDataset(targetDataSet);
        policy.setTargetCluster(targetCluster);
        policy.setSourceCluster(sourceCluster);
        populateCustomProperties(policy);
        for(Map.Entry<String, String> entry : extraProps.entrySet()) {
            policy.getCustomProperties().setProperty(entry.getKey(), entry.getValue());
        }
        return policy;
    }



    public CloudCred buildCloudCred(String name, CloudCred.Provider provider, CloudCred.AuthType authType,
                                     Map<CloudCred.Config, String> configs) {
        CloudCred cloudCred = new CloudCred();
        cloudCred.setName(name);
        cloudCred.setAuthType(authType);
        cloudCred.setProvider(provider);
        cloudCred.setConfigs(configs);
        return cloudCred;
    }

    public String getRandomString(String prefix) {
        return prefix + RandomStringUtils.randomAlphanumeric(15);
    }
}
