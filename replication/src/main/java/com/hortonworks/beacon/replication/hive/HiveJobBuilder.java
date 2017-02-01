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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.DateUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HiveJobBuilder extends JobBuilder {

    public ReplicationJobDetails buildJob(ReplicationPolicy policy) throws BeaconException {
        ReplicationJobDetails job = new ReplicationJobDetails();
        Cluster sourceCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getSourceCluster());
        Cluster targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getTargetCluster());
        Properties customProp = policy.getCustomProperties();
        Map<String, String> map = new HashMap<>();
        map.put(HiveDRProperties.JOB_NAME.getName(), policy.getName());
        map.put(HiveDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        map.put(HiveDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        map.put(HiveDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));
        map.put(HiveDRProperties.SOURCE_HS2_URI.getName(), sourceCluster.getHsEndpoint());
        map.put(HiveDRProperties.SOURCE_DATABASE.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_DATABASE.getName()));
        map.put(HiveDRProperties.SOURCE_TABLES.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_TABLES.getName()));
        map.put(HiveDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        map.put(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName()));
        map.put(HiveDRProperties.TARGET_HS2_URI.getName(), targetCluster.getHsEndpoint());
        map.put(HiveDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
        map.put(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName()));
        map.put(HiveDRProperties.MAX_EVENTS.getName(),
                customProp.getProperty(HiveDRProperties.MAX_EVENTS.getName(), "100"));
        map.put(HiveDRProperties.DISTCP_MAX_MAPS.getName(),
                customProp.getProperty(HiveDRProperties.DISTCP_MAX_MAPS.getName(),"1"));
        map.put(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        map.put(HiveDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),
                customProp.getProperty(HiveDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),"100"));
        map.put(HiveDRProperties.JOB_TYPE.getName(), policy.getType());
        Properties prop = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            prop.setProperty(entry.getKey(), entry.getValue());
        }


        job.validateReplicationProperties(prop);
        job = job.setReplicationJobDetails(prop);
        return job;
    }
}
