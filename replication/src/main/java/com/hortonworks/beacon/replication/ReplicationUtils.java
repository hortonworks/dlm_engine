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

package com.hortonworks.beacon.replication;

import com.google.gson.Gson;
import com.hortonworks.beacon.Destination;
import com.hortonworks.beacon.ExecutionType;
import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Replication utility class.
 */
public final class ReplicationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationUtils.class);
    private static final String SEPARATOR = "/";

    private static final Gson GSON = new Gson();

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
        boolean isCloud = false;
        ExecutionType executionType = ExecutionType.FS;

        if (FSUtils.isHCFS(new Path(policy.getSourceDataset()))
                || FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
            isCloud = true;
            executionType = ExecutionType.FS_HCFS;
        }
        boolean tdeEncryptionEnabled = Boolean.parseBoolean(
                policy.getCustomProperties().getProperty((FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()),
                        "false"));
        if (!tdeEncryptionEnabled) {
            // HCFS check is already done, so need to check if clusters in policy is null
            String clusterName, dataset;
            if (PolicyHelper.isDatasetHCFS(policy.getSourceDataset())) {
                clusterName = policy.getTargetCluster();
                dataset = policy.getTargetDataset();
            } else {
                clusterName = policy.getSourceCluster();
                dataset = policy.getSourceDataset();
            }
            Cluster cluster = ClusterHelper.getActiveCluster(clusterName);
            String stagingUri = FSUtils.getStagingUri(cluster.getFsEndpoint(), dataset);
            boolean isSnapshot = FSSnapshotUtils.checkSnapshottableDirectory(cluster.getName(), stagingUri);
            isSnapshot = isSnapshot || Boolean.valueOf(policy.getSourceSetSnapshottable());
            // FS_SNAPSHOT or FS_HCFS_SNAPSHOT
            if (isSnapshot && isCloud) {
                executionType = ExecutionType.FS_HCFS_SNAPSHOT;
            } else if (isSnapshot) {
                executionType = ExecutionType.FS_SNAPSHOT;
            }
        }
        return executionType.name();
    }

    public static void storeTrackingInfo(JobContext jobContext, String details) throws BeaconException {
        try {
            String instanceId = jobContext.getJobInstanceId();
            if (!isTrackingInfoLatest(instanceId, details)) {
                LOG.debug("Tracking info computed is stale. Not persisting: {}", details);
                return;
            }
            RequestContext.get().startTransaction();
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
        }
    }

    private static boolean isTrackingInfoLatest(String instanceId, String details) {
        try {
            String oldTrackingInfo = getInstanceTrackingInfo(instanceId);
            if (StringUtils.isNotEmpty(oldTrackingInfo)) {
                ReplicationMetrics oldMetrics = GSON.fromJson(oldTrackingInfo, ReplicationMetrics.class);
                ReplicationMetrics newMetrics = GSON.fromJson(details, ReplicationMetrics.class);
                return (oldMetrics.getProgress().getJobProgress() < newMetrics.getProgress().getJobProgress());
            }
        } catch (Throwable t) {
            return true;
        }
        return true;
    }

    public static String getInstanceTrackingInfo(String instanceId) throws BeaconException {
        LOG.info("Getting tracking info for instance id: [{}]", instanceId);
        PolicyInstanceBean instanceBean = new PolicyInstanceBean(instanceId);
        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(instanceBean);
        List<PolicyInstanceBean> beanList = executor.executeSelectQuery(PolicyInstanceQuery.GET_INSTANCE_TRACKING_INFO);
        if (beanList == null || beanList.isEmpty()) {
            return "";
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

    public static boolean isDatasetConflicting(ReplicationType replType, String dataset, Destination dest)
            throws BeaconException {
        boolean isConflicted;
        switch (replType) {
            case FS:
                isConflicted = checkFSDatasetConfliction(dataset,
                        getReplicationPolicyDataset(replType.name(), dest));
                break;
            case HIVE:
                isConflicted = checkHiveDatasetConfliction(dataset,
                        getReplicationPolicyDataset(replType.name(), dest));
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

    public static boolean checkFSDatasetConfliction(String dataset, List<String> replicationDatasetList)
            throws BeaconException {
        LOG.debug("Dataset to replicate: {}", dataset);
        boolean isConflicted = false;
        for (String replicationDataset : replicationDatasetList) {
            LOG.debug("Replication dataset: {}", replicationDataset);
            if (PolicyHelper.isDatasetHCFS(dataset)) {
                if (PolicyHelper.isDatasetHCFS(replicationDataset)) {
                    isConflicted = validateCloudDatasetAncestor(dataset, replicationDataset);
                }
            } else if (PolicyHelper.isDatasetHCFS(replicationDataset)) {
                continue;
            } else {
                String path;
                try {
                    path = new URI(dataset).getPath();
                } catch (URISyntaxException e) {
                    throw new BeaconException(e);
                }
                isConflicted = validateDatasetAncestor(path, replicationDataset);
            }
            if (isConflicted) {
                break;
            }
        }
        return isConflicted;
    }

    private static boolean validateCloudDatasetAncestor(String dataset, String replicationDataset)
            throws BeaconException {
        URI datasetURI;
        URI replicatedDatasetURI;
        try {
            datasetURI = new URI(dataset);
            replicatedDatasetURI = new URI(replicationDataset);
        } catch (URISyntaxException e) {
            throw new BeaconException(e);
        }
        String newDatasetScheme = datasetURI.getScheme();
        String replicatedDatasetScheme = replicatedDatasetURI.getScheme();
        if (newDatasetScheme.equalsIgnoreCase(replicatedDatasetScheme)) {
            String datasetWithoutScheme = datasetURI.getHost() + datasetURI.getPath();
            String replicatedDatasetWithoutScheme = replicatedDatasetURI.getHost() + replicatedDatasetURI.getPath();
            return validateDatasetAncestor(datasetWithoutScheme, replicatedDatasetWithoutScheme);
        }
        return false;
    }

    private static boolean validateDatasetAncestor(String dataset, String replicationDataset) {
        int sourceDatasetLen = dataset.split(SEPARATOR).length;
        String childDataset;
        String parentDataset;
        if (replicationDataset.equals(dataset)) {
            return true;
        }

        if (replicationDataset.startsWith(dataset)) {
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
