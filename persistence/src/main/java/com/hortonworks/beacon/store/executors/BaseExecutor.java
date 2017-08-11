/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.store.BeaconStoreService;

abstract class BaseExecutor {
    static final BeaconStoreService STORE = Services.get().getService(BeaconStoreService.SERVICE_NAME);
}
