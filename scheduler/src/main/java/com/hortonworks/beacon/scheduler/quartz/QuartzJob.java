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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.ReplicationImplFactory;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class QuartzJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzJob.class);
    ReplicationJobDetails details;

    public void setDetails(ReplicationJobDetails details) {
        this.details = details;
    }

    public void execute(JobExecutionContext context) {
        JobKey jobKey = context.getJobDetail().getKey();
        details = (ReplicationJobDetails) context.getJobDetail().getJobDataMap().get(QuartzDataMapEnum.DETAILS.getValue());
        LOG.info("Job [key: {}] [type: {}] execution started.", jobKey, details.getType());
        DRReplication drReplication = ReplicationImplFactory.getReplicationImpl(details);
        if (drReplication!=null) {
            drReplication.establishConnection();
            try {
                drReplication.performReplication();
                drReplication.updateJobExecutionDetails(context);
            } catch (BeaconException e) {
                LOG.error("Exception occurred while doing perform replication :"+e);
            }

            LOG.info("Job [key: {}] [type: {}] execution finished.", jobKey, details.getType());
        }
    }
}
