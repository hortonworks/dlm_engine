/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.job;

import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.nodes.EndNode;
import com.hortonworks.beacon.nodes.StartNode;
import com.hortonworks.beacon.plugin.service.PluginJobManager;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.fs.HCFSReplication;
import com.hortonworks.beacon.replication.fs.HDFSReplication;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.replication.hive.HiveExport;
import com.hortonworks.beacon.replication.hive.HiveImport;
import com.hortonworks.beacon.util.HiveActionType;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;
import com.hortonworks.beacon.util.StringFormat;

import java.util.Properties;

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
                return getFSReplication(details);
            case HIVE:
                return getHiveReplication(details);
            case PLUGIN:
                return new PluginJobManager(details);
            case START:
                return new StartNode(details);
            case END:
                return new EndNode(details);
            default:
                throw new IllegalArgumentException("Invalid policy type: " + details.getType());
        }
    }

    private static BeaconJob getFSReplication(ReplicationJobDetails details) {
        Properties properties = details.getProperties();
        String executionType = properties.getProperty(FSDRProperties.EXECUTION_TYPE.getName());
        switch (executionType) {
            case "FS":
            case "FS_SNAPSHOT":
                return new HDFSReplication(details);
            case "FS_HCFS":
                return new HCFSReplication(details);
            default:
                throw new IllegalArgumentException(
                        StringFormat.format("FS execution type: {} is not supported.", executionType));
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
                    StringFormat.format("Hive action type: {} is not supported:", type.name()));

        }
        return hiveJob;
    }

}
