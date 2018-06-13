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

package com.hortonworks.beacon.scheduler.internal;

import java.util.concurrent.atomic.AtomicReference;

import org.quartz.InterruptableJob;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
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
    public void interrupt() {
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
            LOG.error("AdminJob failed", e);
        }
    }
}
