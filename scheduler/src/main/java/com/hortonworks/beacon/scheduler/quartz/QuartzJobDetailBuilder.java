/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
