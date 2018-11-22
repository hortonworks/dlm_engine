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

package com.hortonworks.dlmengine;

import com.hortonworks.beacon.ExecutionType;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyDao;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.notification.BeaconNotification;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.util.StringFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import static com.hortonworks.beacon.constants.BeaconConstants.ONE_MIN;

/**
 * Main class that represents replication policy.
 * @param <S> source dataset
 * @param <T> target dataset
 */
public abstract class BeaconReplicationPolicy<S extends DataSet, T extends DataSet>
        extends ReplicationPolicy implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconReplicationPolicy.class);
    public static final String SNAPSHOT_PREFIX = "beacon-snapshot-";
    private static final PolicyDao POLICY_DAO = new PolicyDao();

    private S sourceDataset;
    private T targetDataset;

    /**
     *  Replication policy statuses.
     */
    public enum Status {
        SUCCEEDED,
        SUCCEEDEDWITHSKIPPED,
        FAILEDWITHSKIPPED,
        SUSPENDEDFORINTERVENTION;

    }

    public Status getStatusEnum() {
        return Status.valueOf(getStatus());
    }

    public abstract ExecutionType getExecutionTypeEnum() throws BeaconException;

    /**
     * Replication policy type - fs or hive.
     */
    public enum ReplicationPolicyType {
        FS,
        HIVE;

        public static ReplicationPolicyType fromReplicationPolicy(String replicationType) {
            for (ReplicationPolicyType type : values()) {
                if (type.name().equalsIgnoreCase(replicationType)) {
                    return type;
                }
            }
            throw new IllegalStateException("Unhandled replication type: {}" + replicationType);
        }
    }

    protected BeaconReplicationPolicy(ReplicationPolicy policyRequest, S srcDataset, T targetDataset)
            throws BeaconException {
        super(policyRequest);
        this.sourceDataset = srcDataset;
        this.targetDataset = targetDataset;
        this.setExecutionType(getExecutionTypeEnum().name());
        LOG.info("Created replication policy {} with source dataset {} and target dataset {}");
    }

    public static BeaconReplicationPolicy create(String policyName) throws BeaconException {
        return buildReplicationPolicy(new PolicyDao().getActivePolicy(policyName));
    }

    public static BeaconReplicationPolicy buildReplicationPolicy(ReplicationPolicy policy) throws BeaconException{
        DataPluginManagerService dataPluginManagerService = Services.get().getService(DataPluginManagerService.class);
        List<String> dataPlugins = dataPluginManagerService.getRegisteredPlugins();
        if (dataPlugins.isEmpty()) {
            throw new BeaconException("No Registered DataPlugin found");
        }
        for(String dataPluginName : dataPlugins) {
            DataPlugin dataPlugin = dataPluginManagerService.getPlugin(dataPluginName);
            BeaconReplicationPolicy beaconReplicationPolicy = dataPlugin.buildReplicationPolicy(policy);
            if (beaconReplicationPolicy != null) {
                return beaconReplicationPolicy;
            }
        }
        throw new BeaconException(StringFormat.format("Unable to build BeaconReplicationPolicy having "
                + "name {} and type {} ", policy.getName(), policy.getType()));
    }


    public S getSourceDatasetV2() {
        return sourceDataset;
    }

    public T getTargetDatasetV2() {
        return targetDataset;
    }

    public void validate() throws BeaconException {
        validateAPIAllowed();
        validatePolicyDoesNotExists();
        validateScheduleDate();
        validateClusters();
        validatePairing();
        validateDataSetConflict();
        validateSourceDatasetExists();
        deleteSourceSnapshots();
        validateEncryptionAndSnapshot();
        validateTargetExistsEmpty();
        validateTargetIsWritable();
    }

    protected abstract void validateClusters() throws BeaconException;

    protected void clusterExists(String name) throws BeaconException {
        new ClusterDao().getActiveCluster(name);
    }

    private void validateScheduleDate() throws ValidationException {
        if (this.getStartTime().before(new Date(System.currentTimeMillis() - ONE_MIN))) {
            throw new ValidationException("Start time cannot be earlier than current time.");
        }
        if (this.getEndTime() != null && this.getEndTime().before(this.getStartTime())) {
            throw new ValidationException("End time cannot be earlier than start time.");
        }
        if (this.getEndTime() != null && this.getEndTime().before(new Date())) {
            throw new ValidationException("End time cannot be earlier than current time.");
        }
    }

    private void validatePolicyDoesNotExists() throws BeaconException{
        try {
            ReplicationPolicy policy = POLICY_DAO.getActivePolicy(this.getName());
            throw new BeaconException("Policy already exists with name {}", policy.getName());
        } catch (NoSuchElementException ex) {
            //ignore the exception
        }
    }

    private void validateDataSetConflict() throws BeaconStoreException, ValidationException {
        BeaconNotification notification = new BeaconNotification();
        // Source dataset conflict validation.
        boolean sourceDataConflictAndTgtClusterConflict = isSourceDataConflictAndTgtClusterConflict();
        if (sourceDataConflictAndTgtClusterConflict) {
            notification.addError(StringFormat.format("Source dataset {} already in replication"
                            + " on same target cluster {}",
                    this.getSourceDataset(), this.getTargetCluster()));
        }

        // Target dataset conflict validation.
        List<String> targets = getTargetsForType(getReplicationType());
        for (String target: targets) {
            if (targetDataset.conflicts(target)) {
                notification.addError("Target dataset already in replication " + targetDataset.name);
            }
        }

        if (notification.hasErrors()) {
            throw new ValidationException(notification.errorMessage());
        }

    }


    private boolean isSourceDataConflictAndTgtClusterConflict() throws BeaconStoreException {
        PolicyBean policyBean = new PolicyBean();
        policyBean.setType(this.getType());
        policyBean.setSourceCluster(this.getSourceCluster());
        policyBean.setTargetCluster(this.getTargetCluster());
        policyBean.setSourceDataset(this.getSourceDataset());
        PolicyExecutor policyExecutor = new PolicyExecutor(policyBean);
        List<PolicyBean> existingPolicies = policyExecutor
                .getPolicies(PolicyExecutor.PolicyQuery.GET_POLICY_SAME_SOURCE_AND_TGT_CLUSTER);
        if (existingPolicies.size() > 0) {
            LOG.info(StringFormat.format("Source dataset {} already in replication on same target cluster {}",
                    this.getSourceDataset(), this.getTargetCluster()));
            return true;
        }
        return false;
    }

    private void validateTargetIsWritable() throws ValidationException {
        targetDataset.validateWriteAllowed();
    }

    private void validateTargetExistsEmpty() throws BeaconException {
        if (targetDataset.exists() && !targetDataset.isEmpty()) {
            throw new ValidationException("Target dataset directory {} is not empty", targetDataset.name);
        }
        /**
         * Creating the destination with same permissions as source.
         */
        targetDataset.create(sourceDataset.getFileStatus());
        targetDataset.validateEncryptionParameters();
    }

    private void validateEncryptionAndSnapshot() throws BeaconException {
        if (sourceDataset.isEncrypted() && isSnapshotBased()) {
            throw new ValidationException("Can not mark the source dataset snapshottable as it is TDE enabled");
        }
        if (sourceDataset.isEncrypted() || targetDataset.isEncrypted()) {
            getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
        }
        // Allowing replication from encrypted source to non-encrypted destination (BUG-110915).
        if (isSnapshotBased()) {
            sourceDataset.allowSnapshot();
            targetDataset.allowSnapshot();
        }
    }

    private void deleteSourceSnapshots() throws BeaconException {
        if (sourceDataset.isSnapshottable()) {
            LOG.info("Deleting existing snapshot(s) on source directory.");
            sourceDataset.deleteAllSnapshots(getSnapshotPrefix());
        }
    }

    public boolean isSnapshotBased() {
        return Boolean.valueOf(getEnableSnapshotBasedReplication());
    }

    public boolean isEncryptionBased() throws BeaconException {
        return sourceDataset.isEncrypted() || targetDataset.isEncrypted();
    }

    private void validateSourceDatasetExists() throws BeaconException {
        if (!sourceDataset.exists()) {
            throw new ValidationException("Source dataset {} doesn't exists", sourceDataset.name);
        }

        sourceDataset.validateEncryptionParameters();
    }

    private List<String> getTargetsForType(BeaconReplicationPolicy.ReplicationPolicyType type)
            throws BeaconStoreException {
        List<String> dataset = new ArrayList<>();
        PolicyBean bean = new PolicyBean();
        bean.setType(type.name());
        PolicyExecutor executor = new PolicyExecutor(bean);
        for (PolicyBean policyBean : executor.getPolicies(PolicyExecutor.PolicyQuery.GET_POLICIES_FOR_TYPE)) {
            dataset.add(policyBean.getTargetDataset());
        }
        return dataset;

    }

    public void validateAPIAllowed() throws BeaconException {
        String targetCluster = getSchedulableCluster().getName();
        String localCluster = ClusterHelper.getLocalCluster().getName();
        if (!localCluster.equalsIgnoreCase(targetCluster)) {
            throw new ValidationException("This operation is not allowed on cluster: {}. Try it on cluster: {}",
                    localCluster, targetCluster);
        }

    }

    protected abstract Cluster getSchedulableCluster();

    public ReplicationPolicyType getReplicationType() {
        return BeaconReplicationPolicy.ReplicationPolicyType.fromReplicationPolicy(getType());
    }

    public void validatePairing() {
        //do nothing
    }

    public String getSnapshotPrefix() {
        return SNAPSHOT_PREFIX + getName() + "-";
    }

    public String getSnapshotName() {
        return getSnapshotPrefix() + System.currentTimeMillis();
    }

    @Override
    public void close() {
        DataSet.close(sourceDataset);
        DataSet.close(targetDataset);
    }
}
