/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PolicyHelper {
    private PolicyHelper() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(PolicyHelper.class);
    public static final String INSTANCE_EXECUTION_TYPE = "INSTANCE_EXECUTION_TYPE";

    public static String getRemoteBeaconEndpoint(final String policyName) throws BeaconException {
        ReplicationPolicy policy = EntityHelper.getEntity(EntityType.REPLICATIONPOLICY, policyName);
        if (PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw new BeaconException("No remote beacon endpoint for HCFS policy:" + policyName);
        }
        String remoteClusterName = getRemoteClusterName(policyName);
        Cluster remoteCluster = EntityHelper.getEntity(EntityType.CLUSTER, remoteClusterName);
        return remoteCluster.getBeaconEndpoint();
    }

    public static String getRemoteClusterName(final String policyName) throws BeaconException {
        String localClusterName = BeaconConfig.getInstance().getEngine().getLocalClusterName();
        ReplicationPolicy policy = EntityHelper.getEntity(EntityType.REPLICATIONPOLICY, policyName);
        return localClusterName.equalsIgnoreCase(policy.getSourceCluster())
                ? policy.getTargetCluster() : policy.getSourceCluster();
    }

    public static boolean isPolicyHCFS(final String sourceDataset, final String targetDataset) throws BeaconException {
        if (StringUtils.isNotBlank(sourceDataset)) {
            Path sourceDatasetPath = new Path(sourceDataset);
            if (FSUtils.isHCFS(sourceDatasetPath)) {
                return true;
            }
        }

        if (StringUtils.isNotBlank(targetDataset)) {
            Path targetDatasetPath = new Path(targetDataset);
            if (FSUtils.isHCFS(targetDatasetPath)) {
                return true;
            }
        }

        return false;
    }

    public static String getReplicationPolicyType(ReplicationPolicy policy) throws BeaconException {
        String policyType = policy.getType().toUpperCase();

        Cluster sourceCluster;
        Cluster targetCluster;

        if (policyType.equalsIgnoreCase(ReplicationType.FS.getName())) {
            if (FSUtils.isHCFS(new Path(policy.getSourceDataset()))
                    || FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
                policyType = ReplicationType.FS + "_HCFS";
            } else {
                boolean tdeEncryptionEnabled = Boolean.parseBoolean(
                        policy.getCustomProperties().getProperty((FSUtils.TDE_ENCRYPTION_ENABLED), "false"));
                if (!tdeEncryptionEnabled) {
                    FileSystem sourceFs;
                    FileSystem targetFs;
                    boolean isSnapshot;

                    try {
                        String sourceDataset;
                        String targetDataset;

                        // HCFS check is already done, so need to check if clusters in policy is null
                        sourceCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getSourceCluster());
                        sourceFs = FSUtils.getFileSystem(sourceCluster.getFsEndpoint(), new Configuration(), false);
                        sourceDataset = FSUtils.getStagingUri(policy.getSourceDataset(), sourceCluster.getFsEndpoint());

                        targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getTargetCluster());
                        targetFs = FSUtils.getFileSystem(targetCluster.getFsEndpoint(), new Configuration(), false);
                        targetDataset = FSUtils.getStagingUri(policy.getTargetDataset(), targetCluster.getFsEndpoint());

                        isSnapshot = FSUtils.isDirectorySnapshottable(sourceFs, targetFs, sourceDataset, targetDataset);

                        if (isSnapshot) {
                            policyType = ReplicationType.FS + "_SNAPSHOT";
                        }

                    } catch (BeaconException e) {
                        LOG.error("Unable to get Policy details ", e);
                        throw new BeaconException(e);
                    }
                }
            }
        }

        LOG.info("PolicyType {} obtained for entity : {}", policyType, policy.getName());

        return policyType;
    }
}