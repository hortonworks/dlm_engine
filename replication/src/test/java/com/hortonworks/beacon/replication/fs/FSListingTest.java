/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
