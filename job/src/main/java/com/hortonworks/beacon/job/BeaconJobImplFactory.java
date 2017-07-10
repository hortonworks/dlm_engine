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

package com.hortonworks.beacon.job;

import com.hortonworks.beacon.nodes.EndNode;
import com.hortonworks.beacon.nodes.StartNode;
import com.hortonworks.beacon.plugin.service.PluginJobManager;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.fs.FSReplication;
import com.hortonworks.beacon.replication.hive.HiveDRProperties;
import com.hortonworks.beacon.replication.hive.HiveExport;
import com.hortonworks.beacon.replication.hive.HiveImport;
import com.hortonworks.beacon.util.HiveActionType;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;

/**
 * Class to call and create actual Replication type.
 */
public final class BeaconJobImplFactory {

    private BeaconJobImplFactory() {
    }

    public static BeaconJob getBeaconJobImpl(ReplicationJobDetails details) {
        ReplicationType replType = ReplicationHelper.getReplicationType(details.getType());
        switch (replType) {
            case FS:
                return new FSReplication(details);
            case HIVE:
                return getHiveReplication(details);
            case PLUGIN:
                return new PluginJobManager(details);
            case START:
                return new StartNode(details);
            case END:
                return new EndNode(details);
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.COMM_010011.name(), details.getType()));
        }
    }

    private static BeaconJob getHiveReplication(ReplicationJobDetails details) {
        HiveActionType type = HiveActionType.valueOf(details.getProperties().getProperty(
                HiveDRProperties.JOB_ACTION_TYPE.getName()
        ));

        BeaconJob hiveJob;
        switch (type) {
            case EXPORT:
                hiveJob = new HiveExport(details);
                break;
            case IMPORT:
                hiveJob = new HiveImport(details);
                break;
            default:
                throw new IllegalArgumentException(
                        ResourceBundleService.getService().getString(MessageCode.COMM_010005.name(), type.name()));

        }
        return hiveJob;
    }

}
