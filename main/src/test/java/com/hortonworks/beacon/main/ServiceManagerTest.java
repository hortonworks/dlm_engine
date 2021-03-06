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
    private static final List<String> SERVICES = Arrays.asList(PluginManagerService.class.getName());

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
