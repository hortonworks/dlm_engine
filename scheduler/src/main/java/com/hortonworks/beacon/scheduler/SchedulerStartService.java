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
            throw new BeaconException(e, "{} is not initialized. Error: {}", SchedulerInitService.SERVICE_NAME,
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
