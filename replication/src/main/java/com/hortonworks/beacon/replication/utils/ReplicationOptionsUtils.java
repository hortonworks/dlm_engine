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


package com.hortonworks.beacon.replication.utils;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationType;
import com.hortonworks.beacon.replication.hdfs.HDFSDRProperties;
import com.hortonworks.beacon.replication.hdfssnapshot.HDFSSnapshotDRProperties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.Properties;

public final class ReplicationOptionsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationOptionsUtils.class);

    public static CommandLine getCommand(final Properties properties) throws BeaconException {
        System.out.println("in getcommand properties size :"+properties.size());
        String[] args = new String[properties.size()*2];
        int i=0;
        for (Enumeration e = properties.propertyNames(); e.hasMoreElements();) {
            String option = e.nextElement().toString();
            args[i++] = "-"+option;
            String value = properties.getProperty(option);
            args[i++] = value;
        }

        Options options = getDROptions(properties.getProperty("type"));

        Option opt = new Option("type", true, "Replication Job type");
        opt.setRequired(true);
        options.addOption(opt);

         opt = new Option(HDFSDRProperties.JOB_NAME.getName(),
                true, "Replication instance job name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HDFSDRProperties.JOB_FREQUENCY.getName(),
                true, "Replication Job Frequency");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HDFSDRProperties.START_TIME.getName(),
                true, "Replication Policy start time");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(HDFSDRProperties.END_TIME.getName(),
                true, "Replication Policy end time");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(HDFSDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                true, "Is TDE encryption enabled on dirs being replicated?");
        opt.setRequired(false);
        options.addOption(opt);


        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_OVERWRITE.getName(), true, "option to force overwrite");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_IGNORE_ERRORS.getName(), true, "abort on error");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_SKIP_CHECKSUM.getName(), true, "skip checksums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_REMOVE_DELETED_FILES.getName(), true,
                "remove deleted files - should there be files in the target directory that"
                        + "were removed from the source directory");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_BLOCK_SIZE.getName(), true,
                "preserve block size");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER.getName(), true,
                "preserve replication count");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_PERMISSIONS.getName(), true,
                "preserve permissions");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_USER.getName(), true,
                "preserve user");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_GROUP.getName(), true,
                "preserve group");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE.getName(), true,
                "preserve checksum type");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_ACL.getName(), true,
                "preserve ACL");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_XATTR.getName(), true,
                "preserve XATTR");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_TIMES.getName(), true,
                "preserve access and modification times");
        opt.setRequired(false);
        options.addOption(opt);

        try {
            return new GnuParser().parse(options, args);
        } catch (ParseException pe) {
            LOG.info("Unable to parse commad line arguments for HDFS Mirroring " + pe.getMessage());
            throw  new BeaconException(pe.getMessage());
        }
    }


    private static Options getDROptions(final String type) {
        Options options = new Options();
        LOG.info("Replication type :"+type);
        ReplicationType replType = ReplicationType.valueOf(type);
        if (ReplicationType.HDFS.equals(replType)) {
            Option opt = new Option(HDFSDRProperties.DISTCP_MAX_MAPS.getName(),
                    true, "max number of maps to use for distcp");
            opt.setRequired(false);
            options.addOption(opt);
            opt = new Option(HDFSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),
                    true, "Bandwidth in MB/s used by each mapper during replication");
            opt.setRequired(false);
            options.addOption(opt);

            opt = new Option(HDFSDRProperties.SOURCE_CLUSTER_FS_READ_ENDPOINT.getName(), true, "Source NN");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option(HDFSDRProperties.TARGET_CLUSTER_FS_WRITE_ENDPOINT.getName(), true, "Target NN");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option(HDFSDRProperties.SOURCE_DIR.getName(),
                    true, "Source directory to replicate");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option(HDFSDRProperties.TARGET_DIR.getName(),
                    true, "Target directory to replicate");
            opt.setRequired(true);
            options.addOption(opt);

        } else if (ReplicationType.HDFSSNAPSHOT.equals(replType)) {

            LOG.info("Replication type is HDFS Snapshot");
            Option opt = new Option(HDFSSnapshotDRProperties.DISTCP_MAX_MAPS.getName(),
                    true, "max number of maps to use for distcp");
            opt.setRequired(false);
            options.addOption(opt);
            opt = new Option(HDFSSnapshotDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(),
                    true, "Bandwidth in MB/s used by each mapper during replication");
            opt.setRequired(false);
            options.addOption(opt);

            opt = new Option(HDFSSnapshotDRProperties.SOURCE_NN.getName(), true, "Source NN");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option(HDFSSnapshotDRProperties.TARGET_NN.getName(), true, "Target NN");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_DIR.getName(),
                    true, "Source snapshot-able dir to replicate");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_DIR.getName(),
                    true, "Target snapshot-able dir to replicate");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),true,
                    "Delete source snapshots older than this age");
            opt.setRequired(false);
            options.addOption(opt);

            opt = new Option(HDFSSnapshotDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName(),true,
                    "Delete source snapshots older than this age");
            opt.setRequired(false);
            options.addOption(opt);

            opt = new Option(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),true,
                    "Delete source snapshots older than this age");
            opt.setRequired(false);
            options.addOption(opt);

            opt = new Option(HDFSSnapshotDRProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName(),true,
                    "Delete source snapshots older than this age");
            opt.setRequired(false);
            options.addOption(opt);


        }

        return options;
    }

}
