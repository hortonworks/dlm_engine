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


import com.hortonworks.beacon.plugin.service.PluginManagerService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.service.Services;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test class for ServiceInitializer.
 */
public class ServiceManagerTest {
    private static final List<String> SERVICES = Arrays.asList(PluginManagerService.SERVICE_NAME);

    @Test(enabled = false)
    public void testServiceInitEnabled() throws Exception {
        ServiceManager serviceInitializer = ServiceManager.getInstance();
        serviceInitializer.initialize(null, null);
        for (String service : SERVICES) {
            boolean isRegistered = Services.get().isRegistered(service);
            Assert.assertTrue(isRegistered);
        }
    }

    @Test
    public void testServiceInit() throws Exception {
        ServiceManager serviceInitializer = ServiceManager.getInstance();
        serviceInitializer.initialize(null, null);
        for (String service : SERVICES) {
            boolean isRegistered = Services.get().isRegistered(service);
            Assert.assertTrue(!isRegistered);
        }
    }

}
