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
import com.hortonworks.beacon.replication.ReplicationType;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class QuartzJobDetailBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzJobDetailBuilder.class);

    public JobDetail createJobDetail(ReplicationJobDetails job, boolean recovery) {
        return createJobDetail(job, recovery, false);
    }

    public JobDetail createJobDetail(ReplicationJobDetails job, boolean recovery, boolean isChained) {
        JobDetail jobDetail = JobBuilder.newJob(QuartzJob.class )
                .withIdentity(job.getName(), ReplicationType.valueOf(job.getType().toUpperCase()).getName())
                .storeDurably(true)
                .requestRecovery(recovery)
                .usingJobData(getJobDataMap(QuartzDataMapEnum.DETAILS.getValue(), job))
                .usingJobData(QuartzDataMapEnum.COUNTER.getValue(), 0)
                .usingJobData(QuartzDataMapEnum.ISCHAINED.getValue(), isChained)
                .build();
        LOG.info("JobDetail [key: {}] is created. isChained: {}", jobDetail.getKey(), isChained);
        return jobDetail;
    }

    public List<JobDetail> createJobDetailList(List<ReplicationJobDetails> jobs, boolean recovery) {
        List<JobDetail> jobDetails = new ArrayList<>();
        int i = 0;
        for (; i < jobs.size()-1; i++) {
            jobDetails.add(createJobDetail(jobs.get(i), recovery, true));
        }
        jobDetails.add(createJobDetail(jobs.get(i), recovery, false));
        return jobDetails;
    }

    private JobDataMap getJobDataMap(String key, Object data) {
        JobDataMap dataMap = new JobDataMap();
        dataMap.put(key, data);
        return dataMap;
    }
}
