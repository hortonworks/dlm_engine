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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * FileSystem Replication Policy helper.
 */
public final class FSPolicyHelper {
    private FSPolicyHelper() {
    }

    public static Properties buildFSReplicationProperties(final ReplicationPolicy policy) throws BeaconException {
        Map<String, String> map = new HashMap<>();
        map.put(FSDRProperties.JOB_NAME.getName(), policy.getName());
        map.put(FSDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        map.put(FSDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        map.put(FSDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));

        if (!FSUtils.isHCFS(new Path(policy.getSourceDataset()))) {
            Cluster sourceCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getSourceCluster());
            map.put(FSDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        } else {
            map.put(FSDRProperties.SOURCE_NN.getName(), policy.getSourceDataset());
        }
        map.put(FSDRProperties.SOURCE_DATASET.getName(), policy.getSourceDataset());

        if (!FSUtils.isHCFS(new Path(policy.getTargetDataset()))) {
            Cluster targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getTargetCluster());
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

        map.put(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "false"));
        map.put(FSDRProperties.JOB_TYPE.getName(), policy.getType());
        Properties prop = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            prop.setProperty(entry.getKey(), entry.getValue());
        }

        prop.setProperty(PolicyHelper.INSTANCE_EXECUTION_TYPE, policy.getExecutionType());
        return prop;
    }


    public static void validateFSReplicationProperties(final Properties properties) {
        for (FSDRProperties option : FSDRProperties.values()) {
            if (properties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException("Missing DR property for FS Replication : "
                        + option.getName());
            }
        }
    }

}
