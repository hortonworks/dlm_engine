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

package com.hortonworks.beacon.replication.hdfs;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.utils.DistCPOptionsUtil;
import com.hortonworks.beacon.replication.utils.ReplicationOptionsUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class HDFSDRImpl implements DRReplication {


    private static final Logger LOG = LoggerFactory.getLogger(HDFSDRImpl.class);

    HDFSReplicationJobDetails details;
    protected CommandLine cmd;
    private Properties properties;
    private String sourceStagingUri = null;
    private String targetStagingUri = null;

    public HDFSDRImpl() {
    }

    public HDFSDRImpl(ReplicationJobDetails details) {
        this.details = (HDFSReplicationJobDetails)details;
    }

    @Override
    public void establishConnection() {
        //Validate connection to Source NN and target NN;
        sourceStagingUri = new Path(details.getSourceClusterFS(), details.getSourceDir()).toString();
        targetStagingUri = new Path(details.getTargetClusterFS(), details.getTargetDir()).toString();
    }


    @Override
    public void performReplication()  {
        Configuration conf = new Configuration();
        DistCpOptions options = null;
        try {
            cmd = ReplicationOptionsUtils.getCommand(details.getProperties());
            options = getDistCpOptions(cmd, conf);
            options.setMaxMaps(Integer.parseInt(cmd.getOptionValue(HDFSDRProperties.DISTCP_MAX_MAPS.getName())));
            options.setMapBandwidth(Integer.parseInt(cmd.getOptionValue(HDFSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName())));
        } catch (Exception e) {
            LOG.error("Error occurred while parsing distcp options: {}", e);
        }

        try {
            LOG.info("Started DistCp with source Path: {} \t target path: {}", sourceStagingUri, targetStagingUri);
            DistCp distCp = new DistCp(conf, options);
            Job job = distCp.execute();
            LOG.info("Distp Hadoop job: {}", job.getJobID().toString());
            LOG.info("Completed DistCp");
        } catch (Exception e) {
            System.out.println("Exception occurred while invoking distcp : "+e);
        }
    }

    public DistCpOptions getDistCpOptions(CommandLine cmd, Configuration conf) throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths
        List<Path> sourceUris = new ArrayList<>();
        sourceUris.add(new Path(sourceStagingUri));
        return DistCPOptionsUtil.getDistCpOptions(cmd, sourceUris, new Path(targetStagingUri), false, null, null, conf);

    }

    private List<Path> getPaths(String[] paths) {
        List<Path> listPaths = new ArrayList<>();
        for (String path : paths) {
            listPaths.add(new Path(path));
        }
        return listPaths;
    }

   /* public CommandLine getCommand(final Properties properties) throws BeaconException {

        System.out.println(properties.size());
        String[] args = new String[properties.size()*2];
        int i=0;
        for (Enumeration e = properties.propertyNames(); e.hasMoreElements();) {
            String option = e.nextElement().toString();
            args[i++] = "-"+option;
            String value = properties.getProperty(option);
            args[i++] = value;
        }

        Options options = new Options();

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
                true, "Target snapshot-able dir to replicate");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HDFSDRProperties.TARGET_DIR.getName(),
                true, "Target snapshot-able dir to replicate");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HDFSDRProperties.TDE_ENCRYPTION_ENABLED.getName(),
                true, "Is TDE encryption enabled on dirs being replicated?");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(HDFSDRProperties.JOB_NAME.getName(),
                true, "Replication instance job name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HDFSDRProperties.JOB_FREQUENCY.getName(),
                true, "Replication Job Frequency");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("type", true, "Replication Job type");
        opt.setRequired(true);
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
    }*/

}
