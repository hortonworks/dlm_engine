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
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.util.ConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Test class for testing replication on HA clusters.
 */
public class PolicyHAClustersTest extends ResourceBaseTest {

    private Cluster sourceCluster;

    private Cluster targetCluster;

    @BeforeClass
    public void initFs() throws Exception {
        sourceFs = testDataGenerator.getFileSystem(ClusterType.SOURCE);
        targetFs = testDataGenerator.getFileSystem(ClusterType.TARGET);
    }

    @Test
    public void testHAtoHAHdfsReplication() throws Exception {

        //Mock configuration
        Configuration configuration = new Configuration();
        configuration.set("dfs.nameservices", "mycluster0");
        ConfigurationFactory.getINSTANCE().setConfiguration(configuration);

        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        sourceCluster.setHsEndpoint(sourceCluster.getHsEndpoint()
                + "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        Properties currentSourceClusterCustomProps = sourceCluster.getCustomProperties();
        populateSrcClusterHAProps(currentSourceClusterCustomProps);
        sourceCluster.setCustomProperties(currentSourceClusterCustomProps);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());

        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetCluster.setHsEndpoint(targetCluster.getHsEndpoint()
                + "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        Properties currentTargetClusterCustomProps = targetCluster.getCustomProperties();
        populateTgtClusterHAProps(currentTargetClusterCustomProps);
        targetCluster.setCustomProperties(currentTargetClusterCustomProps);

        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.pairClusters(sourceCluster.getName(), true);

        // Unsetting the HA config
        configuration.unset("dfs.nameservices");

        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        submitAndSchedulePolicy(replicationPath, policyName);
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
        unpairAndDeleteClusters();
    }

    @Test
    public void testHAtoNonHAHdfsReplication() throws Exception {
        //Mock configuration
        Configuration configuration = new Configuration();
        configuration.set("dfs.nameservices", "mycluster0");
        ConfigurationFactory.getINSTANCE().setConfiguration(configuration);

        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        sourceCluster.setHsEndpoint(sourceCluster.getHsEndpoint()
                + "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        Properties currentSourceClusterCustomProps = sourceCluster.getCustomProperties();
        populateSrcClusterHAProps(currentSourceClusterCustomProps);
        sourceCluster.setCustomProperties(currentSourceClusterCustomProps);
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());

        // Unsetting the HA config
        configuration.unset("dfs.nameservices");

        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.pairClusters(sourceCluster.getName(), true);

        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        submitAndSchedulePolicy(replicationPath, policyName);
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
        unpairAndDeleteClusters();

    }


    @Test
    public void testNonHAtoHAHdfsReplication() throws Exception {
        sourceCluster = testDataGenerator.getCluster(ClusterType.SOURCE, false);
        sourceCluster.setHsEndpoint(sourceCluster.getHsEndpoint()
                + "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        targetClient.submitCluster(sourceCluster.getName(), sourceCluster.asProperties());

        //Mock configuration
        Configuration configuration = new Configuration();
        configuration.set("dfs.nameservices", "mycluster0");
        ConfigurationFactory.getINSTANCE().setConfiguration(configuration);

        targetCluster = testDataGenerator.getCluster(ClusterType.TARGET, true);
        targetCluster.setHsEndpoint(targetCluster.getHsEndpoint()
                + "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        Properties currentTargetClusterCustomProps = targetCluster.getCustomProperties();
        populateTgtClusterHAProps(currentTargetClusterCustomProps);
        targetCluster.setCustomProperties(currentTargetClusterCustomProps);
        targetClient.submitCluster(targetCluster.getName(), targetCluster.asProperties());
        targetClient.pairClusters(sourceCluster.getName(), true);

        // Unsetting the HA config
        configuration.unset("dfs.nameservices");

        final String policyName = testDataGenerator.getRandomString("FsPolicy");
        String replicationPath = SOURCE_DIR + policyName;
        submitAndSchedulePolicy(replicationPath, policyName);
        waitOnCondition(20000, "First Instance Success ", new Condition() {
            @Override
            public boolean exit() throws BeaconClientException {
                PolicyInstanceList.InstanceElement instanceElement = getFirstInstance(targetClient, policyName);
                return instanceElement != null && instanceElement.status.equals(JobStatus.SUCCESS.name());
            }
        });
        targetClient.deletePolicy(policyName, false);
        unpairAndDeleteClusters();
    }


    private void unpairAndDeleteClusters() throws BeaconClientException {
        targetClient.unpairClusters(sourceCluster.getName(), true);
        targetClient.deleteCluster(sourceCluster.getName());
        targetClient.deleteCluster(targetCluster.getName());
    }

    private void populateSrcClusterHAProps(Properties currentSourceClusterCustomProps) {
        currentSourceClusterCustomProps.setProperty("dfs.nameservices", "mycluster0");
        currentSourceClusterCustomProps.setProperty("dfs.ha.namenodes.mycluster0", "nn1,nn2");
        currentSourceClusterCustomProps.setProperty("dfs.namenode.rpc-address.mycluster0.nn1", "dummy");
        currentSourceClusterCustomProps.setProperty("dfs.namenode.rpc-address.mycluster0.nn2", "dummy");
        currentSourceClusterCustomProps.setProperty("dfs.internal.nameservices", "mycluster0");
    }

    private void populateTgtClusterHAProps(Properties currentTargetClusterCustomProps) {
        currentTargetClusterCustomProps.setProperty("dfs.nameservices", "mycluster1");
        currentTargetClusterCustomProps.setProperty("dfs.ha.namenodes.mycluster1", "nn1,nn2");
        currentTargetClusterCustomProps.setProperty("dfs.namenode.rpc-address.mycluster1.nn1", "dummy");
        currentTargetClusterCustomProps.setProperty("dfs.namenode.rpc-address.mycluster1.nn2", "dummy");
        currentTargetClusterCustomProps.setProperty("dfs.internal.nameservices", "mycluster1");
    }

}
