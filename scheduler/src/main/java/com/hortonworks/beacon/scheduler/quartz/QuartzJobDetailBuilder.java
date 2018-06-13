/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.internal.AdminJob;
import com.hortonworks.beacon.scheduler.internal.SchedulableAdminJob;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Create JobDetail instance for Quartz from ReplicationJob.
 */
public final class QuartzJobDetailBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzJobDetailBuilder.class);

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
        LOG.info("JobDetail [key: {}] is created. isChained: {}", jobDetail.getKey(), isChained);
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
        LOG.info("JobDetail [key: {}] is created.", jobDetail.getKey());
        return jobDetail;
    }
}
