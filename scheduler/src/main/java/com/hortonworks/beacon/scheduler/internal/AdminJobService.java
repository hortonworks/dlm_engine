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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.scheduler.quartz.QuartzJobDetailBuilder;
import com.hortonworks.beacon.scheduler.quartz.QuartzScheduler;
import com.hortonworks.beacon.scheduler.quartz.QuartzTriggerBuilder;
import com.hortonworks.beacon.service.BeaconService;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admin job scheduler.
 */
public final class AdminJobService implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(AdminJobService.class);
    private static final AdminJobService INSTANCE = new AdminJobService();

    private QuartzScheduler scheduler;

    private AdminJobService() {
    }

    /**
     * This should not be used in the code except by {@link com.hortonworks.beacon.service.ServiceManager}.
     * @return an instance of {@link AdminJobService}
     */
    public static AdminJobService get() {
        return INSTANCE;
    }

    /**
     * Schedule a {@link AdminJob} indefinitely.
     * @param adminJob adminJob to be scheduled.
     * @param frequency frequency in minute.
     * @param maxRetry max retry count.
     * @throws BeaconException thrown if any error occurs.
     */
    private void schedule(AdminJob adminJob, int frequency, int maxRetry) throws BeaconException {
        String name = adminJob.getName();
        String group = adminJob.getGroup();
        JobDetail jobDetail = QuartzJobDetailBuilder.createAdminJobDetail(adminJob, name, group);
        frequency = frequency * 60; // frequency in seconds.
        Trigger trigger = QuartzTriggerBuilder.createTrigger(name, group, frequency, maxRetry);
        LOG.info("Scheduling admin job: [{}], group: [{}], policy name: [{}] with frequency: [{} sec].",
                adminJob.getClass().getSimpleName(), group, name, frequency);
        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e);
        }
    }

    public void checkAndSchedule(AdminJob adminJob, int frequency, int maxRetry) throws BeaconException {
        boolean checkAndDelete = checkAndDelete(adminJob);
        if (checkAndDelete) {
            schedule(adminJob, frequency, maxRetry);
        }
    }

    public boolean checkAndDelete(AdminJob adminJob) throws BeaconException {
        String name = adminJob.getName();
        String group = adminJob.getGroup();
        try {
            boolean checkExists = scheduler.checkExists(name, group);
            if (checkExists) {
                LOG.info("Admin job: [{}], group: [{}], policy name: [{}] is deleted successfully.",
                        adminJob.getClass().getSimpleName(), group, name);
                return scheduler.deleteJob(name, group);
            } else {
                LOG.info("Admin job: [{}], group: [{}], policy name: [{}] does not exist.",
                        adminJob.getClass().getSimpleName(), group, name);
                return true;
            }
        } catch (SchedulerException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void init() throws BeaconException {
        scheduler = QuartzScheduler.get();
    }

    @Override
    public void destroy() throws BeaconException {

    }
}
