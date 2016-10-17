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

}
