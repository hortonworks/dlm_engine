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

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.internal.AdminJob;
import com.hortonworks.beacon.scheduler.internal.SchedulableAdminJob;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;

import java.util.ArrayList;
import java.util.List;

/**
 * Create JobDetail instance for Quartz from ReplicationJob.
 */
public final class QuartzJobDetailBuilder {

    private static final BeaconLog LOG = BeaconLog.getLog(QuartzJobDetailBuilder.class);

    private QuartzJobDetailBuilder() {
    }

    private static JobDetail createJobDetail(ReplicationJobDetails job, boolean recovery, boolean isChained,
                                      String policyId, String group) {
        JobDetail jobDetail = JobBuilder.newJob(QuartzJob.class)
                .withIdentity(policyId, group)
                .storeDurably(true)
                .requestRecovery(recovery)
                .usingJobData(getJobDataMap(QuartzDataMapEnum.DETAILS.getValue(), job))
                .usingJobData(QuartzDataMapEnum.CHAINED.getValue(), isChained)
                .build();
        LOG.info(MessageCode.SCHD_000040.name(), jobDetail.getKey(), isChained);
        return jobDetail;
    }

    static List<JobDetail> createJobDetailList(List<ReplicationJobDetails> jobs,
                                               boolean recovery, String policyId) {
        List<JobDetail> jobDetails = new ArrayList<>();
        int i = 0;
        for (; i < jobs.size() - 1; i++) {
            jobDetails.add(createJobDetail(jobs.get(i), recovery, true, policyId, String.valueOf(i)));
        }
        jobDetails.add(createJobDetail(jobs.get(i), recovery, false, policyId, String.valueOf(i)));
        // Add the number of jobs, which is used for inserting the job instance.
        JobDetail jobDetail = jobDetails.get(0);
        jobDetail.getJobDataMap().put(QuartzDataMapEnum.NO_OF_JOBS.getValue(), jobs.size());
        jobDetail.getJobDataMap().put(QuartzDataMapEnum.COUNTER.getValue(), 0);
        JobDetail lastJobDetail = jobDetails.get(jobDetails.size() - 1);
        lastJobDetail.getJobDataMap().put(QuartzDataMapEnum.IS_END_JOB.getValue(), true);
        return jobDetails;
    }

    private static JobDataMap getJobDataMap(String key, Object data) {
        JobDataMap dataMap = new JobDataMap();
        dataMap.put(key, data);
        return dataMap;
    }

    public static JobDetail createAdminJobDetail(AdminJob adminJob, String name, String group) {
        JobDetail jobDetail = JobBuilder.newJob(SchedulableAdminJob.class)
                .withIdentity(name, group)
                .storeDurably(true)
                .usingJobData(getJobDataMap(QuartzDataMapEnum.ADMIN_JOB.getValue(), adminJob))
                .build();
        LOG.info(MessageCode.SCHD_000041.name(), jobDetail.getKey());
        return jobDetail;
    }
}
