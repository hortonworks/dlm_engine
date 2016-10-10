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

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.utils.SchedulerUtils;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class QuartzJobDetailFactory {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzJobDetailFactory.class);

    public JobDetail createJobDetail(ReplicationJobDetails job, boolean recovery) {
        String jobKey = SchedulerUtils.getUUID();
        JobDetail jobDetail = JobBuilder.newJob(QuartzJob.class )
                .withIdentity(jobKey)
                .storeDurably(true)
                .requestRecovery(recovery)
                .usingJobData(getJobDataMap("Details", job))
                .build();
        LOG.info("JobDetail [key: {}] is created.", jobKey);
        return jobDetail;
    }

    public List<JobDetail> createJobDetailList(List<ReplicationJobDetails> jobDetailses, boolean recovery) {
        List<JobDetail> jobDetails = new ArrayList<>();
        for (ReplicationJobDetails replicationJobDetails : jobDetailses) {
            jobDetails.add(createJobDetail(replicationJobDetails, recovery));
        }
        return jobDetails;
    }

    private JobDataMap getJobDataMap(String key, Object data) {
        JobDataMap dataMap = new JobDataMap();
        dataMap.put(key, data);
        return dataMap;
    }
}
