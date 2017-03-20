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
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.BeaconService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beacon scheduler service.
 * The service depends on the DB to be setup with Quartz tables.
 */
public final class BeaconSchedulerService implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconSchedulerService.class);

    private BeaconQuartzScheduler scheduler;
    private static final BeaconSchedulerService INSTANCE = new BeaconSchedulerService();

    private BeaconSchedulerService() {
        scheduler = BeaconQuartzScheduler.get();
    }

    public static BeaconSchedulerService get() {
        return INSTANCE;
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public void init() throws BeaconException {
        LOG.info("Initializing beacon scheduler service.");
        if (scheduler != null && !scheduler.isStarted()) {
            scheduler.startScheduler();
        }
    }

    @Override
    public void destroy() throws BeaconException {
        LOG.info("Destroying beacon scheduler service.");
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.stopScheduler();
        }
    }
}
