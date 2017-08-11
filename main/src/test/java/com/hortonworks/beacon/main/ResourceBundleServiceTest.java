/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.main;

import org.testng.annotations.Test;

import com.hortonworks.beacon.XTestCase;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.service.ServiceManager;

/**
 * Test Resource Bundle.
 */
public class ResourceBundleServiceTest extends XTestCase{

    private void setup(String language) throws BeaconException {
        ServiceManager.getInstance().destroy();
        BeaconConfig.getInstance().getEngine().setLocale(language);
        initializeServices(null);
    }

    @Test(expectedExceptions = BeaconException.class,
            expectedExceptionsMessageRegExp = "Missing parameter: en")
    public void testEnglish() throws Exception {
        setup("en");
        throw new BeaconException(MessageCode.COMM_010002.name(), "en");
    }

    @Test(expectedExceptions = BeaconException.class,
            expectedExceptionsMessageRegExp = "Missing parameter: Default")
    public void testDefault() throws Exception {
        setup("de");
        throw new BeaconException(MessageCode.COMM_010002.name(), "Default");
    }
}
