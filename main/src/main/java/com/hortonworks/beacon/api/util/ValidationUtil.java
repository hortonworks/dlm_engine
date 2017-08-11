/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */


package com.hortonworks.beacon.api.util;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.replication.fs.FSPolicyHelper;
import com.hortonworks.beacon.replication.hive.HivePolicyHelper;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;

/**
 * Utility class to validate API requests.
 */
public final class ValidationUtil {
    private static final BeaconLog LOG = BeaconLog.getLog(ValidationUtil.class);

    private ValidationUtil() {
    }

    public static void validateIfAPIRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        if (policy == null) {
            throw new BeaconException(MessageCode.COMM_010008.name(), "Policy");
        }

        isRequestAllowed(policy);
    }

    private static void isRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = BeaconConfig.getInstance().getEngine().getLocalClusterName();

        // If policy is HCFS then requests are allowed on source cluster
        if (localClusterName.equalsIgnoreCase(sourceClusterName)
                && !PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw BeaconWebException.newAPIException(MessageCode.MAIN_000005.name(), sourceClusterName,
                    targetClusterName);
        }
    }

    public static void validatePolicy(final ReplicationPolicy policy) throws BeaconException {
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                FSPolicyHelper.validateFSReplicationProperties(FSPolicyHelper.buildFSReplicationProperties(policy));
                break;
            case HIVE:
                HivePolicyHelper.validateHiveReplicationProperties(
                        HivePolicyHelper.buildHiveReplicationProperties(policy));
                break;
            default:
                throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.COMM_010007.name(), policy.getType()));
        }
    }

    public static void validateEntityDataset(final ReplicationPolicy policy) throws BeaconException {
        checkSameDataset(policy);
        checkDatasetConfliction(policy);
    }

    private static void checkSameDataset(ReplicationPolicy policy) throws BeaconException {
        String sourceDataset = policy.getSourceDataset();
        String targetDataset = policy.getTargetDataset();

        if (!targetDataset.equals(sourceDataset)) {
            LOG.error(MessageCode.MAIN_000031.name(), targetDataset, sourceDataset);
            throw new BeaconException(MessageCode.MAIN_000031.name(), targetDataset, sourceDataset);
        }
    }

    private static void checkDatasetConfliction(ReplicationPolicy policy) throws BeaconException {
        boolean isConflicted = ReplicationUtils.isDatasetConflicting(
                ReplicationHelper.getReplicationType(policy.getType()), policy.getSourceDataset());
        if (isConflicted) {
            LOG.error(MessageCode.MAIN_000032.name(), policy.getSourceDataset());
            throw new BeaconException(MessageCode.MAIN_000032.name(), policy.getSourceDataset());
        }
    }
}
