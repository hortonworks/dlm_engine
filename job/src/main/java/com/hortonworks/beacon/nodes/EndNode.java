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

package com.hortonworks.beacon.nodes;

import com.hortonworks.beacon.job.BeaconJob;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End node implementation.
 */
public class EndNode implements BeaconJob {

    private static final Logger LOG = LoggerFactory.getLogger(EndNode.class);

    private ReplicationJobDetails jobDetails;

    public EndNode(ReplicationJobDetails jobDetails) {
        this.jobDetails = jobDetails;
    }

    @Override
    public void init(JobContext jobContext) throws BeaconException {

    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException {
        LOG.info("Starting the replication job for [{}], type [{}]",
                jobContext.getJobInstanceId(), jobDetails.getType());
    }

    @Override
    public void cleanUp(JobContext jobContext) throws BeaconException {

    }

    @Override
    public String getJobExecutionContextDetails() throws BeaconException {
        InstanceExecutionDetails details = new InstanceExecutionDetails();
        details.setJobStatus(JobStatus.SUCCESS.name());
        details.setJobMessage("SUCCESS");
        details.setJobId("N/A");
        details.setJobExecutionType("N/A");
        return details.toJsonString();

    }
}
