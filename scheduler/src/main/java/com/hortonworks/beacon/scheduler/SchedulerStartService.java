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

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.BeaconService;
import com.hortonworks.beacon.service.Services;

import java.util.NoSuchElementException;

/**
 * Beacon scheduler service.
 * The service depends on the DB to be setup with Quartz tables.
 */
public final class SchedulerStartService implements BeaconService {

    private static final BeaconLog LOG = BeaconLog.getLog(SchedulerStartService.class);
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
            LOG.error(MessageCode.SCHD_000017.name(), SchedulerInitService.SERVICE_NAME, e.getMessage());
            throw new BeaconException(MessageCode.SCHD_000017.name(), e);
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
