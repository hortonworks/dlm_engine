/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api.util;

import com.hortonworks.beacon.api.EncryptionZoneListing;
import com.hortonworks.beacon.client.entity.Cluster;
import org.apache.hadoop.fs.Path;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.when;

/**
 * Test class for EncryptionZoneListing.
 */
@PrepareForTest(EncryptionZoneListing.class)
public class EncryptionZoneListingTest extends PowerMockTestCase {

    private EncryptionZoneListing encryptionZoneListing;

    @BeforeClass
    public void setup() {
        encryptionZoneListing = PowerMockito.mock(EncryptionZoneListing.class);
    }

    @Test
    public void testIsPathEncrypted() throws Exception {
        Set<String> encryptionZones = new HashSet<>();
        encryptionZones.add("/data/encrypt/");
        Cluster cluster = new Cluster();
        when(encryptionZoneListing.getEncryptionZones((Cluster)notNull(), (Path)notNull())).thenReturn(
                encryptionZones);
        when(encryptionZoneListing.isPathEncrypted((Cluster) notNull(), (Path)notNull())).thenCallRealMethod();
        Assert.assertTrue(encryptionZoneListing.isPathEncrypted(cluster, (new Path("/data/encrypt"))));
        Assert.assertTrue(encryptionZoneListing.isPathEncrypted(cluster, (new Path("/data/encrypt/media"))));
        Assert.assertFalse(encryptionZoneListing.isPathEncrypted(cluster, (new Path("/data/encryptZone/media")
        )));
        Assert.assertFalse(encryptionZoneListing.isPathEncrypted(cluster, (new Path("/"))));
        Assert.assertFalse(encryptionZoneListing.isPathEncrypted(cluster, (new Path("/data"))));
    }
}
