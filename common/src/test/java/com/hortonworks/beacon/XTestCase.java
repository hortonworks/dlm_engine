/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.service.Services;

/**
 * Abstract Class to initialize the Services required for test cases.
 *
 */
public abstract class XTestCase {
    private static String beaconTestBaseDir;
    private static final String LOG_DIR;

    static {
        beaconTestBaseDir = System.getProperty("beacon.test.dir",
            System.getProperty("user.dir")) + "/target/";
        LOG_DIR = beaconTestBaseDir + "log/";
        System.setProperty("beacon.log.dir", LOG_DIR);
        System.setProperty("beacon.hostname", System.getProperty("beacon.hostname", "localhost"));
    }

    private static final List<String> DEFAULTSERVICES = new ArrayList<String>() {
        {
            // ResourceBundleService is to access the resourceBundle which is
            // accessed by all modules for i18n. This should be the last entry.
            if (!Services.get().isRegistered(ResourceBundleService.SERVICE_NAME)) {
                add(ResourceBundleService.SERVICE_NAME);
            }
        }
    };

    protected static void initializeServices(List<String> services) throws BeaconException {
        List<String> beaconServices = services != null ? new ArrayList<>(services) : new ArrayList<String>();
        try {
            if (CollectionUtils.isEmpty(services)) {
                if (!Services.get().isRegistered(ResourceBundleService.SERVICE_NAME)) {
                    beaconServices = DEFAULTSERVICES;
                }
            } else if (!(services.contains(ResourceBundleService.SERVICE_NAME)
                    && Services.get().isRegistered(ResourceBundleService.SERVICE_NAME))) {
                beaconServices.add(ResourceBundleService.SERVICE_NAME);
            }
            ServiceManager.getInstance().initialize(beaconServices, null);
        } catch (BeaconException e) {
            throw e;
        }
    }
}
