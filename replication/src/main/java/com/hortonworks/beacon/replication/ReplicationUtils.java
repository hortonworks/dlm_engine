/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication;

import com.hortonworks.beacon.Destination;
import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.replication.fs.FSSnapshotUtils;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyExecutor.PolicyQuery;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationType;
import com.hortonworks.beacon.util.StringFormat;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Replication utility class.
 */
public final class ReplicationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationUtils.class);
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
                throw new IllegalArgumentException(
                    StringFormat.format("Policy type {} is not supported", policyType));
        }

        LOG.info("PolicyType {} is obtained for entity: {}", policyType, policy.getName());

        return policyType;
    }

    private static String getFSReplicationPolicyType(ReplicationPolicy policy) throws BeaconException {
        String policyType = policy.getType().toUpperCase();

        if (FSUtils.isHCFS(new Path(policy.getSourceDataset()))
                || FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
            policyType = ReplicationType.FS + "_HCFS";
        } else {
            boolean tdeEncryptionEnabled = Boolean.parseBoolean(
                    policy.getCustomProperties().getProperty((FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()),
                            "false")
            );
            if (!tdeEncryptionEnabled) {
                // HCFS check is already done, so need to check if clusters in policy is null
                Cluster sourceCluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
                String sourceDataset = FSUtils.getStagingUri(sourceCluster.getFsEndpoint(),
                        policy.getSourceDataset());
                boolean isSnapshot = FSSnapshotUtils.checkSnapshottableDirectory(sourceCluster.getName(),
                        sourceDataset);
                isSnapshot = isSnapshot || Boolean.valueOf(policy.getSourceSetSnapshottable());
                if (isSnapshot) {
                    policyType = ReplicationType.FS + "_SNAPSHOT";
                }
            }
        }
        return policyType;
    }

    public static void storeTrackingInfo(JobContext jobContext, String details) throws BeaconException {
        try {
            RequestContext.get().startTransaction();
            String instanceId = jobContext.getJobInstanceId();
            PolicyInstanceBean bean = new PolicyInstanceBean(instanceId);
            bean.setTrackingInfo(details);
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
            executor.executeUpdate(PolicyInstanceQuery.UPDATE_INSTANCE_TRACKING_INFO);
            RequestContext.get().commitTransaction();
        } catch (Exception e) {
            LOG.error("Error while storing external id. Message: {}", e.getMessage());
            throw new BeaconException(e);
        } finally {
            RequestContext.get().rollbackTransaction();
            RequestContext.get().clear();
        }
    }

    public static String getInstanceTrackingInfo(String instanceId) throws BeaconException {
        LOG.info("Getting tracking info for instance id: [{}]", instanceId);
        PolicyInstanceBean instanceBean = new PolicyInstanceBean(instanceId);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
        List<PolicyInstanceBean> beanList = executor.executeSelectQuery(PolicyInstanceQuery.GET_INSTANCE_TRACKING_INFO);
        if (beanList == null || beanList.isEmpty()) {
            throw new BeaconException("No instance tracking info found for instance: {}", instanceId);
        }
        LOG.info("Getting tracking info completed for instance id: [{}], size: [{}]", instanceId, beanList.size());
        return beanList.get(0).getTrackingInfo();
    }

    private static List<String> getReplicationPolicyDataset(String policyType, Destination dest)
            throws BeaconException {
        try {
            List<String> dataset = new ArrayList<>();
            PolicyBean bean = new PolicyBean();
            bean.setType(policyType);
            PolicyExecutor executor = new PolicyExecutor(bean);
            for (PolicyBean policyBean : executor.getPolicies(PolicyQuery.GET_POLICIES_FOR_TYPE)) {
                if (dest == Destination.SOURCE) {
                    dataset.add(policyBean.getSourceDataset());
                } else {
                    dataset.add(policyBean.getTargetDataset());
                }
            }
            return dataset;
        } catch (BeaconException e) {
            LOG.error("Error while obtaining PolicyBean: {}", e.getMessage());
            throw new BeaconException(e);
        }
    }

    public static boolean isDatasetConflicting(ReplicationType replType, String dataset)
            throws BeaconException {
        boolean isConflicted;
        switch (replType) {
            case FS:
                isConflicted = checkFSDatasetConfliction(dataset,
                        getReplicationPolicyDataset(replType.name(), Destination.TARGET));
                break;
            case HIVE:
                isConflicted = checkHiveDatasetConfliction(dataset,
                        getReplicationPolicyDataset(replType.name(), Destination.TARGET));
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Policy type {} is not supported", replType));
        }
        return isConflicted;
    }

    public static boolean checkHiveDatasetConfliction(String dataset, List<String> replicationDatasetList) {
        for (String replicationDataset : replicationDatasetList) {
            if (replicationDataset.equals(dataset)) {
                return true;
            }
        }
        return false;
    }

    public static boolean checkFSDatasetConfliction(String dataset, List<String> replicationDatasetList) {
        String childDataset;
        String parentDataset;
        int sourceDatasetLen = dataset.split(SEPARATOR).length;
        for (String replicationDataset : replicationDatasetList) {
            String sourceDatasetPrefix = SEPARATOR + dataset.split(SEPARATOR)[1];
            if (!replicationDataset.startsWith(sourceDatasetPrefix)) {
                continue;
            }

            if (replicationDataset.equals(dataset)) {
                return true;
            }

            if (sourceDatasetLen > replicationDataset.split(SEPARATOR).length) {
                childDataset = dataset;
                parentDataset = replicationDataset;
            } else {
                childDataset = replicationDataset;
                parentDataset = dataset;
            }

            LOG.info("Identified parent dataset: {} and child dataset: {}", parentDataset, childDataset);
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

    public static int getReplicationMetricsInterval() {
        return BeaconConfig.getInstance().getScheduler().getReplicationMetricsInterval();
    }
}
