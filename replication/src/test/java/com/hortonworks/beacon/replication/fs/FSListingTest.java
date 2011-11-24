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

package com.hortonworks.beacon.replication.fs;

import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.notNull;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test FSlisting method(s).
 */
@PrepareForTest(FSListing.class)
public class FSListingTest extends PowerMockTestCase {

    @Mock
    private FSListing fsListing;

    @BeforeClass
    public void setup() {
        PowerMockito.mockStatic(FSListing.class);
    }

    @Test
    public void testGetBaseListing() throws Exception {
        String clusterName = "src";
        String fsEndPoint = "hdfs://localhost:8020";

        when(fsListing.contains(clusterName, "/data/encrypt/")).thenReturn(true);
        when(fsListing.getBaseListing((String) notNull(), (String) notNull(), (String) notNull())).thenCallRealMethod();
        doNothing().when(fsListing).updateListing((String) notNull(), (String) notNull(), (String) notNull());
        Assert.assertEquals(fsListing.getBaseListing(clusterName, fsEndPoint, "/data/encrypt"), "/data/encrypt/");
        Assert.assertEquals(fsListing.getBaseListing(clusterName, fsEndPoint, "/data/encrypt"), "/data/encrypt/");
        Assert.assertEquals(fsListing.getBaseListing(clusterName, fsEndPoint, "/data/encrypt/media"),
                "/data/encrypt/");
        Assert.assertNull(fsListing.getBaseListing(clusterName, fsEndPoint, "/data/encryptZone/media"));
        Assert.assertNull(fsListing.getBaseListing(clusterName, fsEndPoint, "/"));
        Assert.assertNull(fsListing.getBaseListing(clusterName, fsEndPoint, "/data"));
        Assert.assertNull(fsListing.getBaseListing(clusterName, fsEndPoint, ""));
    }
}
