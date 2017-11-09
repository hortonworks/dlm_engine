/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.BeaconService;
import com.hortonworks.beacon.service.Services;

import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beacon scheduler service.
 * The service depends on the DB to be setup with Quartz tables.
 */
public final class SchedulerStartService implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerStartService.class);
    public static final String SERVICE_NAME = SchedulerStartService.class.getName();
    private static final SchedulerStartService INSTANCE = new SchedulerStartService();

    private BeaconQuartzScheduler scheduler;

    private SchedulerStartService() {
        scheduler = BeaconQuartzScheduler.get();
    }

    public static SchedulerStartService get() {
        return INSTANCE;
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws BeaconException {
        try {
            Services.get().getService(SchedulerInitService.SERVICE_NAME);
            scheduler.startScheduler();
        } catch (NoSuchElementException e) {
            LOG.error("{} is not initialized. Error: {}", SchedulerInitService.SERVICE_NAME, e.getMessage());
            throw new BeaconException("{} is not initialized. Error: {}", e, SchedulerInitService.SERVICE_NAME,
                e.getMessage());
        }
    }

    /**
     * no-op. scheduler is stopped by {@link SchedulerInitService}
     * @throws BeaconException
     */
    @Override
    public void destroy() throws BeaconException {
    }
}
