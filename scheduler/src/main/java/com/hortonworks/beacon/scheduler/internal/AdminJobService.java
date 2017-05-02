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
        LOG.info("Scheduling admin job: [{}], group: [{}], policy name: [{}] with frequency: [{} sec].",
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
                LOG.info("Admin job: [{}], group: [{}], policy name: [{}] is deleted successfully.",
                        adminJob.getClass().getSimpleName(), group, name);
                return scheduler.deleteJob(name, group);
            } else {
                LOG.info("Admin job: [{}], group: [{}], policy name: [{}] does not exits.",
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
