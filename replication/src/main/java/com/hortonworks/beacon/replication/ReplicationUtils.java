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

package com.hortonworks.beacon.replication;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.replication.fs.FSDRProperties;
import com.hortonworks.beacon.replication.fs.FSSnapshotUtils;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * Replication utility class.
 */
public final class ReplicationUtils {
    private static final BeaconLog LOG = BeaconLog.getLog(ReplicationUtils.class);
    private static final String SEPARATOR = "/";

    private ReplicationUtils() {
    }

    public static String getReplicationPolicyType(ReplicationPolicy policy) throws BeaconException {
        String policyType = policy.getType().toUpperCase();
        ReplicationType type = ReplicationType.valueOf(policyType);

        switch (type) {
            case HIVE:
                policyType = ReplicationType.HIVE.name();
                break;
            case FS:
                policyType = getFSReplicationPolicyType(policy);
                break;
            default:
                throw new IllegalArgumentException("Policy Type "+policyType+" not supported");
        }

        LOG.info("PolicyType {} obtained for entity : {}", policyType, policy.getName());

        return policyType;
    }

    private static String getFSReplicationPolicyType(ReplicationPolicy policy) throws BeaconException {
        String policyType = policy.getType().toUpperCase();
        Cluster sourceCluster;
        Cluster targetCluster;

        if (FSUtils.isHCFS(new Path(policy.getSourceDataset()))
                || FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
            policyType = ReplicationType.FS + "_HCFS";
        } else {
            boolean tdeEncryptionEnabled = Boolean.parseBoolean(
                    policy.getCustomProperties().getProperty((FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()),
                            "false"));
            if (!tdeEncryptionEnabled) {
                FileSystem sourceFs;
                FileSystem targetFs;
                boolean isSnapshot;

                try {
                    String sourceDataset;
                    String targetDataset;

                    // HCFS check is already done, so need to check if clusters in policy is null
                    sourceCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getSourceCluster());
                    sourceFs = FSUtils.getFileSystem(sourceCluster.getFsEndpoint(),
                            new Configuration(), false);
                    sourceDataset = FSUtils.getStagingUri(sourceCluster.getFsEndpoint(), policy.getSourceDataset());

                    targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getTargetCluster());
                    targetFs = FSUtils.getFileSystem(targetCluster.getFsEndpoint(),
                            new Configuration(), false);
                    targetDataset = FSUtils.getStagingUri(targetCluster.getFsEndpoint(), policy.getTargetDataset());

                    isSnapshot = FSSnapshotUtils.isDirectorySnapshottable(sourceFs, targetFs,
                            sourceDataset, targetDataset);

                    if (isSnapshot) {
                        policyType = ReplicationType.FS + "_SNAPSHOT";
                    }

                } catch (BeaconException e) {
                    LOG.error("Unable to get Policy details ", e);
                    throw new BeaconException(e);
                }
            }
        }
        return policyType;
    }

    public static void storeTrackingInfo(JobContext jobContext, String id) throws BeaconException {
        try {
            String instanceId = jobContext.getJobInstanceId();
            PolicyInstanceBean bean = new PolicyInstanceBean(instanceId);
            bean.setTrackingInfo(id);
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
            executor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_TRACKING_INFO);
        } catch (Exception e) {
            LOG.error("Error while storing external id. Message: ", e.getMessage());
            throw new BeaconException(e);
        }
    }

    private static List<String> getReplicationPolicyDataset(String policyType) throws BeaconException {
        try {
            List<String> dataset = new ArrayList<>();
            PolicyBean bean = new PolicyBean();
            bean.setType(policyType);
            PolicyExecutor executor = new PolicyExecutor(bean);
            for(PolicyBean policyBean : executor.getAllPoliciesForType()) {
                dataset.add(policyBean.getSourceDataset());
            }
            return dataset;
        } catch (BeaconException e) {
            LOG.error("Error while obtaining Policy Bean : {}", e.getMessage());
            throw new BeaconException(e);
        }
    }

    public static boolean isDatasetConflicting(ReplicationType replType, String sourceDataset)
            throws BeaconException {
        boolean isConflicted;
        switch (replType) {
            case FS:
                isConflicted = checkFSDatasetConfliction(sourceDataset,
                        getReplicationPolicyDataset(replType.name()));
                break;
            case HIVE:
                isConflicted = checkHiveDatasetConfliction(sourceDataset,
                        getReplicationPolicyDataset(replType.name()));
                break;
            default:
                throw new IllegalArgumentException("Policy Type "+replType+" not supported");
        }
        return isConflicted;
    }

    public static boolean checkHiveDatasetConfliction(String sourceDataset, List<String> replicationDataset) {
        for (String dataset : replicationDataset) {
            if (dataset.equals(sourceDataset)) {
                return true;
            }
        }
        return false;
    }

    public static boolean checkFSDatasetConfliction(String sourceDataset, List<String> replicationDataset) {
        String childDataset;
        String parentDataset;
        int sourceDatasetLen = sourceDataset.split(SEPARATOR).length;
        for (String dataset : replicationDataset) {
            String sourceDatasetPrefix = SEPARATOR + sourceDataset.split(SEPARATOR)[1];
            if (!dataset.startsWith(sourceDatasetPrefix)) {
                continue;
            }

            if (dataset.equals(sourceDataset)) {
                return true;
            }

            if (sourceDatasetLen > dataset.split(SEPARATOR).length) {
                childDataset = sourceDataset;
                parentDataset = dataset;
            } else {
                childDataset = dataset;
                parentDataset = sourceDataset;
            }

            LOG.info("Identified Parent dataset : {} and child dataset : {}", parentDataset, childDataset);
            if (compareDataset(parentDataset, childDataset)) {
                return true;
            }
        }
        return false;
    }

    private static boolean compareDataset(String parentDataset, String childDataset) {
        String[] childDir = childDataset.split(SEPARATOR);
        String[] parendDir = parentDataset.split(SEPARATOR);
        int i = 0;
        while (i < parendDir.length) {
            if (!parendDir[i].equals(childDir[i])) {
                return false;
            }
            i++;
        }
        return true;
    }
}
