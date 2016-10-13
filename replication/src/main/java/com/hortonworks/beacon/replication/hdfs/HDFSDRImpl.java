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

import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class HDFSDRImpl implements DRReplication {


    private static final Logger LOG = LoggerFactory.getLogger(HDFSDRImpl.class);

    HDFSReplicationJobDetails details;
    private String sourceStagingUri = null;
    private String targetStagingUri = null;

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
        DistCpOptions options = getDistCpOptions();

        try {
            LOG.info("Started DistCp with source Path: {} \t target path: {}", sourceStagingUri, targetStagingUri);
            DistCp distCp = new DistCp(new Configuration(), options);
            Job job = distCp.execute();
            LOG.info("Distp Hadoop job: {}", job.getJobID().toString());
            LOG.info("Completed DistCp");
        } catch (Exception e) {
            System.out.println("Exception occurred while invoking distcp : "+e);
        }
    }

    public DistCpOptions getDistCpOptions() {
        // DistCpOptions expects the first argument to be a file OR a list of Paths
        List<Path> sourceUris = new ArrayList<>();
        sourceUris.add(new Path(sourceStagingUri));
        DistCpOptions distcpOptions = new DistCpOptions(sourceUris, new Path(targetStagingUri));
        distcpOptions.setSyncFolder(true); //ensures directory structure is maintained when source is copied to target

        if (details.isTdeEncryptionEnabled()) {
            distcpOptions.setSkipCRC(true);
        }

        distcpOptions.setBlocking(true);
        distcpOptions.setMaxMaps(details.getDistcpMaxMaps());
        distcpOptions.setMapBandwidth(details.getDistcpMapBandwidth());
        return distcpOptions;
    }

    private List<Path> getPaths(String[] paths) {
        List<Path> listPaths = new ArrayList<>();
        for (String path : paths) {
            listPaths.add(new Path(path));
        }
        return listPaths;
    }
}
