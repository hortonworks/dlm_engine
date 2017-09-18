/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler.internal;

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.scheduler.quartz.QuartzDataMapEnum;
import org.quartz.InterruptableJob;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.UnableToInterruptJobException;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Quartz based implementation of AdminJob.
 */
public class SchedulableAdminJob implements InterruptableJob {

    private AtomicReference<Thread> runningThread = new AtomicReference<>();
    private static final BeaconLog LOG = BeaconLog.getLog(SchedulableAdminJob.class);
    private AdminJob adminJob;

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        LOG.info(MessageCode.SCHD_000021.name(),
                adminJob != null ? adminJob.getClass().getSimpleName() : "SchedulableAdminJob");
        Thread thread = runningThread.get();
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        this.runningThread.set(Thread.currentThread());
        JobDetail jobDetail = context.getJobDetail();
        adminJob = (AdminJob) jobDetail.getJobDataMap().get(QuartzDataMapEnum.ADMIN_JOB.getValue());
        JobKey jobKey = jobDetail.getKey();
        try {
            boolean result = adminJob.perform();
            if (result) {
                LOG.info(MessageCode.SCHD_000022.name(), adminJob.getClass().getSimpleName());
                Scheduler scheduler = context.getScheduler();
                scheduler.deleteJob(jobKey);
            }
        } catch (Throwable e) {
            LOG.error(MessageCode.SCHD_000023.name(), adminJob.getClass().getSimpleName(), e.getMessage(), e);
        }
    }
}
