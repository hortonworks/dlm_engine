/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.replication.ReplicationDistCpOption;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.EvictionHelper;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.jsp.el.ELException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hortonworks.beacon.replication.InstanceReplication.getHAConfigs;

/**
 * FileSystem Replication Policy helper.
 */
public final class FSPolicyHelper {
    private static final BeaconLog LOG = BeaconLog.getLog(FSPolicyHelper.class);
    private FSPolicyHelper() {
    }

    // Should not have any properties coming cluster entity.
    // Cluster endpoints (properties) will be fetched by replication job.
    public static Properties buildFSReplicationProperties(final ReplicationPolicy policy) throws BeaconException {
        Map<String, String> map = new HashMap<>();
        map.put(FSDRProperties.SOURCE_CLUSTER_NAME.getName(), policy.getSourceCluster());
        map.put(FSDRProperties.TARGET_CLUSTER_NAME.getName(), policy.getTargetCluster());
        map.put(FSDRProperties.JOB_NAME.getName(), policy.getName());
        map.put(FSDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        map.put(FSDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        map.put(FSDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));

        map.put(FSDRProperties.SOURCE_DATASET.getName(), policy.getSourceDataset());
        map.put(FSDRProperties.TARGET_DATASET.getName(), policy.getTargetDataset());

        Properties customProp = policy.getCustomProperties();
        map.put(FSDRProperties.DISTCP_MAX_MAPS.getName(),
                customProp.getProperty(FSDRProperties.DISTCP_MAX_MAPS.getName(), "1"));
        map.put(FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),
                customProp.getProperty(FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(), "100"));
        map.put(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                customProp.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName(), "3"));
        map.put(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName(),
                customProp.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName(), "3"));
        map.put(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                customProp.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName(), "3"));
        map.put(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName(),
                customProp.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName(), "3"));

        map.put(FSDRProperties.QUEUE_NAME.getName(),
                customProp.getProperty(FSDRProperties.QUEUE_NAME.getName(), "default"));

        map.put(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false"));
        map.put(FSDRProperties.JOB_TYPE.getName(), policy.getType());
        map.put(FSDRProperties.RETRY_ATTEMPTS.getName(), String.valueOf(policy.getRetry().getAttempts()));
        map.put(FSDRProperties.RETRY_DELAY.getName(), String.valueOf(policy.getRetry().getDelay()));

        map.putAll(getDistcpOptions(policy.getCustomProperties()));

        Cluster sourceCluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
        if (ClusterHelper.isHighlyAvailableHDFS(sourceCluster.getCustomProperties())) {
            Cluster targetCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
            map.putAll(getHAConfigs(sourceCluster.getCustomProperties(), targetCluster.getCustomProperties()));
        }

        Properties prop = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            prop.setProperty(entry.getKey(), entry.getValue());
        }

        return prop;
    }


    public static void validateFSReplicationProperties(final Properties properties) throws BeaconException {
        for (FSDRProperties option : FSDRProperties.values()) {
            if (properties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException(
                        ResourceBundleService.getService()
                                .getString(MessageCode.REPL_000003.name(), option.getName()));
            }
        }

        validateRetentionAgeLimit(properties.getProperty(
                FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()));
        validateRetentionAgeLimit(properties.getProperty(
                FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()));
    }

    private static void validateRetentionAgeLimit(String ageLimit) throws BeaconException {
        try {
            if (StringUtils.isNotBlank(ageLimit)) {
                EvictionHelper.evalExpressionToMilliSeconds(ageLimit);
            }
        } catch (ELException e) {
            LOG.warn(MessageCode.COMM_010001.name(), ageLimit, e.getMessage());
            throw new BeaconException(MessageCode.COMM_010001.name(), e, ageLimit, e.getMessage());
        }
    }

    private static Map<String, String> getDistcpOptions(Properties properties) {
        Map<String, String> distcpOptionsMap = new HashMap<>();
        for(ReplicationDistCpOption options : ReplicationDistCpOption.values()) {
            if (properties.getProperty(options.getName())!=null) {
                distcpOptionsMap.put(options.getName(), properties.getProperty(options.getName()));
            }
        }
        return distcpOptionsMap;
    }
}
