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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.util.EntityHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.hdfssnapshot.HDFSSnapshotDRProperties;
import com.hortonworks.beacon.replication.hdfssnapshot.HDFSSnapshotReplicationJobDetails;
import com.hortonworks.beacon.util.DateUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HDFSSnapshotJobBuilder extends JobBuilder {

    public ReplicationJobDetails buildJob(ReplicationPolicy policy) throws BeaconException {
        HDFSSnapshotReplicationJobDetails job = new HDFSSnapshotReplicationJobDetails();
        Cluster sourceCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getSourceCluster());
        Cluster targetCluster = EntityHelper.getEntity(EntityType.CLUSTER, policy.getTargetCluster());
        Properties customProp = policy.getCustomProperties();
        Map<String, String> map = new HashMap<>();
        map.put(HDFSSnapshotDRProperties.JOB_NAME.getName(), policy.getName());
        map.put(HDFSSnapshotDRProperties.JOB_FREQUENCY.getName(), String.valueOf(policy.getFrequencyInSec()));
        map.put(HDFSSnapshotDRProperties.START_TIME.getName(), DateUtil.formatDate(policy.getStartTime()));
        map.put(HDFSSnapshotDRProperties.END_TIME.getName(), DateUtil.formatDate(policy.getEndTime()));
        map.put(HDFSSnapshotDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
        map.put(HDFSSnapshotDRProperties.SOURCE_EXEC_URL.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_EXEC_URL.getName()));
        map.put(HDFSSnapshotDRProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName()));
        map.put(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_DIR.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_DIR.getName()));
        map.put(HDFSSnapshotDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
        map.put(HDFSSnapshotDRProperties.TARGET_EXEC_URL.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_EXEC_URL.getName()));
        map.put(HDFSSnapshotDRProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName()));
        map.put(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_DIR.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_DIR.getName()));
        map.put(HDFSSnapshotDRProperties.DISTCP_MAX_MAPS.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.DISTCP_MAX_MAPS.getName()));
        map.put(HDFSSnapshotDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName()));
        map.put(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName()));
        map.put(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
        map.put(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName()));
        map.put(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()));

        map.put(HDFSSnapshotDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                customProp.getProperty(HDFSSnapshotDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
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
