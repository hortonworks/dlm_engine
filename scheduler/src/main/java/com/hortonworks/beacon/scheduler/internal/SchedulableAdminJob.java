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

import java.util.concurrent.atomic.AtomicReference;

import org.quartz.InterruptableJob;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.scheduler.quartz.QuartzDataMapEnum;

/**
 * Quartz based implementation of AdminJob.
 */
public class SchedulableAdminJob implements InterruptableJob {

    private AtomicReference<Thread> runningThread = new AtomicReference<>();
    private static final Logger LOG = LoggerFactory.getLogger(SchedulableAdminJob.class);
    private AdminJob adminJob;

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        LOG.info("Interrupt received for job [{}].",
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
            adminJob.perform();
            LOG.info("AdminJob [{}] is completed successfully. Removing the scheduled job.",
                adminJob.getClass().getSimpleName());
            Scheduler scheduler = context.getScheduler();
            scheduler.deleteJob(jobKey);
        } catch (Throwable e) {
            LOG.error("AdminJob [{}] error message: {}", adminJob.getClass().getSimpleName(), e.getMessage(), e);
        }
    }
}
