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

package com.hortonworks.beacon.replication.fs;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.servlet.jsp.el.ELException;

import com.hortonworks.beacon.Destination;
import com.hortonworks.beacon.SchemeType;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.entity.util.ReplicationPolicyBuilder;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ReplicationDistCpOption;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.EvictionHelper;
import com.hortonworks.beacon.util.StringFormat;
import com.hortonworks.beacon.config.BeaconConfig;

/**
 * FileSystem Replication Policy helper.
 */
public final class FSPolicyHelper {
    private static final Logger LOG = LoggerFactory.getLogger(FSPolicyHelper.class);
    private static BeaconConfig config = BeaconConfig.getInstance();
    private FSPolicyHelper() {
    }

    // Should not have any properties coming cluster entity.
    // Cluster endpoints (properties) will be fetched by replication job.
    public static Properties buildFSReplicationProperties(final ReplicationPolicy policy) throws BeaconException {
        Map<String, String> map = new HashMap<>();
        map.put(FSDRProperties.SOURCE_CLUSTER_NAME.getName(), policy.getSourceCluster());
        map.put(FSDRProperties.TARGET_CLUSTER_NAME.getName(), policy.getTargetCluster());
        if (FSUtils.isHCFS(new Path(policy.getSourceDataset()))
                || FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
            map.put(FSDRProperties.CLOUD_CRED.getName(),
                    policy.getCustomProperties().getProperty(ReplicationPolicyFields.CLOUDCRED.getName()));
        }
        map.put(FSDRProperties.EXECUTION_TYPE.getName(), policy.getExecutionType());
        map.put(FSDRProperties.JOB_NAME.getName(), policy.getName());
        map.put(FSDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        map.put(FSDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        map.put(FSDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));

        map.put(FSDRProperties.SOURCE_DATASET.getName(), getDatasetWithScheme(policy, Destination.SOURCE));
        map.put(FSDRProperties.TARGET_DATASET.getName(), getDatasetWithScheme(policy, Destination.TARGET));

        Properties customProp = policy.getCustomProperties();
        map.put(FSDRProperties.DISTCP_MAX_MAPS.getName(),
                customProp.getProperty(FSDRProperties.DISTCP_MAX_MAPS.getName()));

        map.put(FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),
                customProp.getProperty(FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName()));
        String defSnapshotRetCount = String.valueOf(config.getEngine().getSnapshotRetentionNumber());
        map.put(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),

                customProp.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName(), "3"));
        map.put(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName(),
                customProp.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName(), defSnapshotRetCount));
        map.put(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                customProp.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName(), "3"));
        map.put(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName(),
                customProp.getProperty(FSDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName(), defSnapshotRetCount));

        map.put(FSDRProperties.QUEUE_NAME.getName(),
                customProp.getProperty(FSDRProperties.QUEUE_NAME.getName()));

        map.put(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false"));
        map.put(FSDRProperties.TDE_SAMEKEY.getName(),
                customProp.getProperty(FSDRProperties.TDE_SAMEKEY.getName(), "false"));

        map.put(FSDRProperties.JOB_TYPE.getName(), policy.getType());
        map.put(FSDRProperties.RETRY_ATTEMPTS.getName(), String.valueOf(policy.getRetry().getAttempts()));
        map.put(FSDRProperties.RETRY_DELAY.getName(), String.valueOf(policy.getRetry().getDelay()));

        if (PolicyHelper.isCloudEncryptionEnabled(policy)) {
            map.put(FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName(), policy.getCloudEncryptionAlgorithm());
            map.put(FSDRProperties.CLOUD_ENCRYPTIONKEY.getName(), policy.getCloudEncryptionKey());
        }

        if (Boolean.valueOf(policy.getPreserveMeta())) {
            map.put(BeaconConstants.META_LOCATION, policy.getCustomProperties().getProperty(BeaconConstants
                    .META_LOCATION));
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


    public static void validateFSReplicationProperties(final Properties properties) throws BeaconException {
        for (FSDRProperties option : FSDRProperties.values()) {
            if (properties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException(
                    StringFormat.format("Missing DR property for FS replication: {}", option.getName()));
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
            LOG.warn("Unable to parse retention age limit: {} {}", ageLimit, e.getMessage());
            throw new BeaconException(e, "Unable to parse retention age limit: {} {}", e.getMessage(), ageLimit);
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

    private static String getDatasetWithScheme(ReplicationPolicy policy, Destination destination)
            throws BeaconException {
        String datasetWithScheme;
        if (destination == Destination.SOURCE) {
            datasetWithScheme = policy.getSourceDataset();
        } else {
            datasetWithScheme = policy.getTargetDataset();
        }
        if (PolicyHelper.isDatasetHCFS(datasetWithScheme)) {
            String cloudCred = policy.getCustomProperties().getProperty(ReplicationPolicyFields.CLOUDCRED.getName());
            datasetWithScheme = ReplicationPolicyBuilder.appendCloudSchema(cloudCred, datasetWithScheme, SchemeType
                    .HCFS_NAME);
        }
        return datasetWithScheme;
    }
}
