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

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.utils.SchedulerUtils;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BeaconJobDetailsFactory {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconJobDetailsFactory.class);

    public static JobDetail createJobDetail(Class<? extends BeaconJob> job, Map<String, Object> jobData, boolean recovery) {
        String jobKey = SchedulerUtils.getUUID();
        JobDetail jobDetail = JobBuilder.newJob(job)
                .withIdentity(jobKey)
                .storeDurably(true)
                .requestRecovery(recovery)
                .usingJobData(SchedulerUtils.prepareJobData(jobData))
                .build();
        LOG.info("JobDetail [key: {}] is created.", jobKey);
        return jobDetail;
    }

    public static JobDetail createJobDetail(BeaconJob job, boolean recovery) {
        String jobKey = SchedulerUtils.getUUID();
        JobDetail jobDetail = JobBuilder.newJob(job.getClass())
                .withIdentity(jobKey)
                .storeDurably(true)
                .requestRecovery(recovery)
                .build();
        LOG.info("JobDetail [key: {}] is created.", jobKey);
        return jobDetail;
    }
}
