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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Cluster.ClusterFields;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
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
    public static Properties buildHiveReplicationProperties(final ReplicationPolicy policy,
                                                            String hiveActionType) throws BeaconException {
        Cluster sourceCluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
        Cluster targetCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());

        String dataLake = targetCluster.getCustomProperties().getProperty(ClusterFields.CLOUDDATALAKE.getName());
        Boolean isDataLake = Boolean.valueOf(dataLake);
        if (!isDataLake && (StringUtils.isBlank(sourceCluster.getHsEndpoint())
                                            || StringUtils.isBlank(targetCluster.getHsEndpoint()))) {
            throw new BeaconException("Hive server endpoint is not specified in cluster entity");
        }

        Properties customProp = policy.getCustomProperties();
        Map<String, String> map = new HashMap<>();
        map.put(HiveDRProperties.JOB_NAME.getName(), policy.getName());
        map.put(HiveDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        map.put(HiveDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        map.put(HiveDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));
        map.put(HiveDRProperties.SOURCE_DATASET.getName(), policy.getSourceDataset());
        map.put(HiveDRProperties.TARGET_DATASET.getName(), policy.getTargetDataset());
        map.put(HiveDRProperties.SOURCE_CLUSTER_NAME.getName(), policy.getSourceCluster());
        map.put(HiveDRProperties.SOURCE_HIVE_SERVER_AUTHENTICATION.getName(),
                sourceCluster.getHiveServerAuthentication());
        map.put(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName()));
        map.put(HiveDRProperties.SOURCE_HMS_KERBEROS_PRINCIPAL.getName(),
                sourceCluster.getCustomProperties().getProperty(
                        BeaconConstants.HMS_PRINCIPAL));
        map.put(HiveDRProperties.TARGET_CLUSTER_NAME.getName(), policy.getTargetCluster());
        map.put(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName()));
        map.put(HiveDRProperties.TARGET_HMS_KERBEROS_PRINCIPAL.getName(),
                targetCluster.getCustomProperties().getProperty(
                        BeaconConstants.HMS_PRINCIPAL));
        map.put(HiveDRProperties.TARGET_HIVE_SERVER_AUTHENTICATION.getName(),
                targetCluster.getHiveServerAuthentication());
        map.put(HiveDRProperties.MAX_EVENTS.getName(),
                customProp.getProperty(HiveDRProperties.MAX_EVENTS.getName(), String.valueOf(BeaconConfig.getInstance()
                        .getEngine().getMaxHiveEvents())));
        if (customProp.containsKey(HiveDRProperties.DISTCP_MAX_MAPS.getName())) {
            map.put(BeaconConstants.DISTCP_OPTIONS+"m",
                    customProp.getProperty(HiveDRProperties.DISTCP_MAX_MAPS.getName()));
        }
        if (customProp.containsKey(HiveDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())) {
            map.put(BeaconConstants.DISTCP_OPTIONS+"bandwidth",
                    customProp.getProperty(HiveDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName()));
        }
        if (customProp.containsKey(HiveDRProperties.QUEUE_NAME.getName())) {
            map.put(HiveDRProperties.QUEUE_NAME.getName(),
                    customProp.getProperty(HiveDRProperties.QUEUE_NAME.getName()));
        }

        map.put(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        map.put(HiveDRProperties.TDE_SAMEKEY.getName(),
                customProp.getProperty(HiveDRProperties.TDE_SAMEKEY.getName()));
        map.put(HiveDRProperties.JOB_TYPE.getName(), policy.getType());
        map.put(HiveDRProperties.RETRY_ATTEMPTS.getName(), String.valueOf(policy.getRetry().getAttempts()));
        map.put(HiveDRProperties.RETRY_DELAY.getName(), String.valueOf(policy.getRetry().getDelay()));
        map.put(ReplicationPolicy.ReplicationPolicyFields.CLOUDCRED.getName(),
                customProp.getProperty(ReplicationPolicy.ReplicationPolicyFields.CLOUDCRED.getName()));
        if (StringUtils.isNotBlank(hiveActionType)) {
            map.put(HiveDRProperties.JOB_ACTION_TYPE.getName(), hiveActionType);
        }
        if (ClusterHelper.isCloudEncryptionEnabled(targetCluster)) {
            map.put(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName(),
                    targetCluster.getHiveCloudEncryptionAlgorithm());
            map.put(FSDRProperties.CLOUD_ENCRYPTIONKEY.getName(), targetCluster.getHiveCloudEncryptionKey());
        } else if (PolicyHelper.isCloudEncryptionEnabled(policy)) {
            map.put(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName(), policy.getCloudEncryptionAlgorithm());
            map.put(FSDRProperties.CLOUD_ENCRYPTIONKEY.getName(), policy.getCloudEncryptionKey());
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
                throw new IllegalArgumentException("Missing DR property for Hive replication: " + option.getName());
            }
        }
    }

    static Map<String, String> getDistcpOptions(Properties properties) {
        Map<String, String> distcpOptionsMap = new HashMap<>();
        boolean isDataLake = Boolean.valueOf(properties.getProperty(ClusterFields.CLOUDDATALAKE.getName()));
        // Setting default distcp options parameters to true.
        setPreserveParameters(properties, distcpOptionsMap, isDataLake);

        String tdeEnabled = properties.getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName());
        String sameKey = properties.getProperty(FSDRProperties.TDE_SAMEKEY.getName());
        if (Boolean.valueOf(tdeEnabled) && !Boolean.valueOf(sameKey)) {
            distcpOptionsMap.put(BeaconConstants.DISTCP_OPTIONS + ReplicationDistCpOption
                    .DISTCP_OPTION_SKIP_CHECKSUM.getSName(), "");
            distcpOptionsMap.put(BeaconConstants.DISTCP_OPTIONS + ReplicationDistCpOption
                    .DISTCP_OPTION_UPDATE.getSName(), "");
        }
        return distcpOptionsMap;
    }

    private static void setPreserveParameters(Properties properties, Map<String, String> distcpOptionsMap,
                                              boolean isDataLake) {
        StringBuilder preserveParameters = new StringBuilder();
        String preserveFlag = "p";
        preserveParameters.append(BeaconConstants.DISTCP_OPTIONS)
            .append(preserveFlag)
            .append(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_USER.getSName())
            .append(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_GROUP.getSName())
            .append(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_PERMISSIONS.getSName())
            .append(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_BLOCK_SIZE.getSName());
        if (!isDataLake && StringUtils.isNotEmpty(properties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_ACL.getName()))) {
            preserveParameters.append(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_ACL.getSName());
        }
        if (!isDataLake && StringUtils.isNotEmpty(properties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_XATTR.getName()))) {
            preserveParameters.append(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_XATTR.getSName());
        }
        distcpOptionsMap.put(preserveParameters.toString(), "");
    }
}
