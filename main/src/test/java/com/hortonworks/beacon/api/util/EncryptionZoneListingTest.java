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

import java.util.HashMap;
import java.util.Map;

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
        Map<String, String> encryptionZones = new HashMap<>();
        encryptionZones.put("/data/encrypt/", "default");
        Cluster cluster = new Cluster();
        cluster.setName("src");
        when(encryptionZoneListing.getEncryptionZones((Cluster)notNull(), (Path)notNull())).thenReturn(
                encryptionZones);
        when(encryptionZoneListing.getEncryptionKeyName(cluster, "/data/encrypt/")).thenReturn(
                "default");
        when(encryptionZoneListing.getBaseEncryptedPath((Cluster) notNull(), (Path)notNull())).thenCallRealMethod();
        when(encryptionZoneListing.getBaseEncryptedPath((Cluster) notNull(), (String)notNull())).thenCallRealMethod();
        Assert.assertEquals(encryptionZoneListing.getBaseEncryptedPath(cluster, (new Path("/data/encrypt"))),
                "/data/encrypt/");
        Assert.assertEquals(encryptionZoneListing.getBaseEncryptedPath(cluster, (new Path("/data/encrypt/media"))),
                "/data/encrypt/");
        Assert.assertNull(encryptionZoneListing.getBaseEncryptedPath(cluster, (new Path("/data/encryptZone/media")
        )));
        Assert.assertNull(encryptionZoneListing.getBaseEncryptedPath(cluster, (new Path("/"))));
        Assert.assertNull(encryptionZoneListing.getBaseEncryptedPath(cluster, (new Path("/data"))));
        Assert.assertNull(encryptionZoneListing.getBaseEncryptedPath(cluster, ""));
    }
}
