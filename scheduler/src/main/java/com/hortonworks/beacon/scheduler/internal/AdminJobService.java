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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.scheduler.quartz.QuartzJobDetailBuilder;
import com.hortonworks.beacon.scheduler.quartz.QuartzScheduler;
import com.hortonworks.beacon.scheduler.quartz.QuartzTriggerBuilder;
import com.hortonworks.beacon.service.BeaconService;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

/**
 * Admin job scheduler.
 */
public final class AdminJobService implements BeaconService {

    private static final BeaconLog LOG = BeaconLog.getLog(AdminJobService.class);
    public static final String SERVICE_NAME = AdminJobService.class.getName();
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
     * @throws BeaconException thrown if any error occurs.
     */
    private void schedule(AdminJob adminJob, int frequency) throws BeaconException {
        String name = adminJob.getName();
        String group = adminJob.getGroup();
        JobDetail jobDetail = QuartzJobDetailBuilder.createAdminJobDetail(adminJob, name, group);
        frequency = frequency * 60; // frequency in seconds.
        Trigger trigger = QuartzTriggerBuilder.createTrigger(name, group, null, null, frequency);
        LOG.info(MessageCode.SCHD_000018.name(),
                adminJob.getClass().getSimpleName(), group, name, frequency);
        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e);
        }
    }

    public void checkAndSchedule(AdminJob adminJob, int frequency) throws BeaconException {
        boolean checkAndDelete = checkAndDelete(adminJob);
        if (checkAndDelete) {
            schedule(adminJob, frequency);
        }
    }

    public boolean checkAndDelete(AdminJob adminJob) throws BeaconException {
        String name = adminJob.getName();
        String group = adminJob.getGroup();
        try {
            boolean checkExists = scheduler.checkExists(name, group);
            if (checkExists) {
                LOG.info(MessageCode.SCHD_000019.name(),
                        adminJob.getClass().getSimpleName(), group, name);
                return scheduler.deleteJob(name, group);
            } else {
                LOG.info(MessageCode.SCHD_000020.name(),
                        adminJob.getClass().getSimpleName(), group, name);
                return true;
            }
        } catch (SchedulerException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws BeaconException {
        scheduler = QuartzScheduler.get();
    }

    @Override
    public void destroy() throws BeaconException {

    }
}
