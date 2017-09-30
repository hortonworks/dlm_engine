/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.entity.util.ReplicationDistCpOption;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Hive Replication Policy helper.
 */
public final class HivePolicyHelper {
    private HivePolicyHelper() {
    }
    public static Properties buildHiveReplicationProperties(final ReplicationPolicy policy) throws BeaconException {
        return  buildHiveReplicationProperties(policy, "");
    }

    // Should not have any properties coming cluster entity.
    // Cluster endpoints (properties) will be fetched by replication job.
    static Properties buildHiveReplicationProperties(final ReplicationPolicy policy,
                                                            String hiveActionType) throws BeaconException {
        Cluster sourceCluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
        Cluster targetCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());

        if (StringUtils.isBlank(sourceCluster.getHsEndpoint())
                || StringUtils.isBlank(targetCluster.getHsEndpoint())) {
            throw new BeaconException(MessageCode.MAIN_000154.name());
        }

        Properties customProp = policy.getCustomProperties();
        Map<String, String> map = new HashMap<>();
        map.put(HiveDRProperties.JOB_NAME.getName(), policy.getName());
        map.put(HiveDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        map.put(HiveDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        map.put(HiveDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));
        map.put(HiveDRProperties.SOURCE_DATASET.getName(), policy.getSourceDataset());
        map.put(HiveDRProperties.SOURCE_CLUSTER_NAME.getName(), policy.getSourceCluster());
        map.put(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName()));
        map.put(HiveDRProperties.TARGET_CLUSTER_NAME.getName(), policy.getTargetCluster());
        map.put(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName()));
        map.put(HiveDRProperties.MAX_EVENTS.getName(),
                customProp.getProperty(HiveDRProperties.MAX_EVENTS.getName(), String.valueOf(BeaconConfig.getInstance()
                        .getEngine().getMaxHiveEvents())));
        map.put(BeaconConstants.DISTCP_OPTIONS+"m",
                customProp.getProperty(HiveDRProperties.DISTCP_MAX_MAPS.getName(), "1"));
        map.put(BeaconConstants.DISTCP_OPTIONS+"bandwidth",
                customProp.getProperty(HiveDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(), "100"));
        map.put(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        map.put(HiveDRProperties.QUEUE_NAME.getName(),
                customProp.getProperty(HiveDRProperties.QUEUE_NAME.getName(), "default"));
        map.put(HiveDRProperties.JOB_TYPE.getName(), policy.getType());
        map.put(HiveDRProperties.RETRY_ATTEMPTS.getName(), String.valueOf(policy.getRetry().getAttempts()));
        map.put(HiveDRProperties.RETRY_DELAY.getName(), String.valueOf(policy.getRetry().getDelay()));
        if (StringUtils.isNotBlank(hiveActionType)) {
            map.put(HiveDRProperties.JOB_ACTION_TYPE.getName(), hiveActionType);
        }

        map.putAll(getDistcpOptions(policy.getCustomProperties()));
        Properties prop = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            prop.setProperty(entry.getKey(), entry.getValue());
        }

        return prop;
    }

    public static void validateHiveReplicationProperties(final Properties properties) {
        for (HiveDRProperties option : HiveDRProperties.values()) {
            if (properties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException(
                        ResourceBundleService.getService()
                                .getString(MessageCode.REPL_000020.name(), option.getName()));
            }
        }
    }

    private static Map<String, String> getDistcpOptions(Properties properties) {
        Map<String, String> distcpOptionsMap = new HashMap<>();
        // Setting default distcp options parameters to true.
        distcpOptionsMap.put(BeaconConstants.DISTCP_OPTIONS+ReplicationDistCpOption.
                DISTCP_OPTION_PRESERVE_USER.getSName(), "u");
        distcpOptionsMap.put(BeaconConstants.DISTCP_OPTIONS+ReplicationDistCpOption.
                DISTCP_OPTION_PRESERVE_GROUP.getSName(), "g");
        distcpOptionsMap.put(BeaconConstants.DISTCP_OPTIONS+ReplicationDistCpOption.
                DISTCP_OPTION_PRESERVE_PERMISSIONS.getSName(), "p");
        distcpOptionsMap.put(BeaconConstants.DISTCP_OPTIONS+ReplicationDistCpOption.
                DISTCP_OPTION_PRESERVE_XATTR.getSName(), "x");

        for(ReplicationDistCpOption options : ReplicationDistCpOption.values()) {
            if (properties.getProperty(options.getName())!=null) {
                distcpOptionsMap.put(BeaconConstants.DISTCP_OPTIONS+options.getSName(),
                        properties.getProperty(options.getName()));
            }
        }
        return distcpOptionsMap;
    }
}
