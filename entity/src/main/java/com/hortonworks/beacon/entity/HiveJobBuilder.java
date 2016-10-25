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

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.hive.HiveDRProperties;
import com.hortonworks.beacon.replication.hive.HiveReplicationJobDetails;
import com.hortonworks.beacon.util.DateUtil;

import java.util.Properties;

public class HiveJobBuilder extends JobBuilder {

    public ReplicationJobDetails buildJob(ReplicationPolicy policy) throws BeaconException {
        HiveReplicationJobDetails job = new HiveReplicationJobDetails();
        Cluster sourceCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getSourceCluster());
        Cluster targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getTargetCluster());
        Properties customProp = policy.getCustomProperties();
        Properties prop = new Properties();
        prop.setProperty(HiveDRProperties.JOB_NAME.getName(), policy.getName());
        prop.setProperty(HiveDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        prop.setProperty(HiveDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        prop.setProperty(HiveDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));
        prop.setProperty(HiveDRProperties.SOURCE_HS2_URI.getName(), sourceCluster.getHsEndpoint());
        prop.setProperty(HiveDRProperties.SOURCE_DATABASE.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_DATABASE.getName()));
        prop.setProperty(HiveDRProperties.SOURCE_TABLES.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_TABLES.getName()));
        prop.setProperty(HiveDRProperties.STAGING_PATH.getName(),
                customProp.getProperty(HiveDRProperties.STAGING_PATH.getName()));
        prop.setProperty(HiveDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        prop.setProperty(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName()));
        prop.setProperty(HiveDRProperties.TARGET_HS2_URI.getName(), targetCluster.getHsEndpoint());
        prop.setProperty(HiveDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
        prop.setProperty(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HiveDRProperties.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName()));
        prop.setProperty(HiveDRProperties.MAX_EVENTS.getName(),
                customProp.getProperty(HiveDRProperties.MAX_EVENTS.getName()));
        prop.setProperty(HiveDRProperties.REPLICATION_MAX_MAPS.getName(),
                customProp.getProperty(HiveDRProperties.REPLICATION_MAX_MAPS.getName()));
        prop.setProperty(HiveDRProperties.DISTCP_MAX_MAPS.getName(),
                customProp.getProperty(HiveDRProperties.DISTCP_MAX_MAPS.getName()));
        prop.setProperty(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(HiveDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
        prop.setProperty(HiveDRProperties.DISTCP_MAP_BANDWIDTH.getName(),
                customProp.getProperty(HiveDRProperties.DISTCP_MAP_BANDWIDTH.getName()));
        job.validateReplicationProperties(prop);
        job = job.setReplicationJobDetails(prop);
        return job;
    }
}
