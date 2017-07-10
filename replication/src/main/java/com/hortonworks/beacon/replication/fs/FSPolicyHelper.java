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

package com.hortonworks.beacon.replication.fs;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.servlet.jsp.el.ELException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.EvictionHelper;
import com.hortonworks.beacon.util.FSUtils;

/**
 * FileSystem Replication Policy helper.
 */
public final class FSPolicyHelper {
    private static final BeaconLog LOG = BeaconLog.getLog(FSPolicyHelper.class);
    private FSPolicyHelper() {
    }

    public static Properties buildFSReplicationProperties(final ReplicationPolicy policy) throws BeaconException {
        Map<String, String> map = new HashMap<>();
        map.put(FSDRProperties.JOB_NAME.getName(), policy.getName());
        map.put(FSDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        map.put(FSDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        map.put(FSDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));

        if (!FSUtils.isHCFS(new Path(policy.getSourceDataset()))) {
            Cluster sourceCluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
            map.put(FSDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        } else {
            map.put(FSDRProperties.SOURCE_NN.getName(), policy.getSourceDataset());
        }
        map.put(FSDRProperties.SOURCE_DATASET.getName(), policy.getSourceDataset());

        if (!FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
            Cluster targetCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
            map.put(FSDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
        } else {
            map.put(FSDRProperties.TARGET_NN.getName(), policy.getTargetDataset());
        }
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

}
