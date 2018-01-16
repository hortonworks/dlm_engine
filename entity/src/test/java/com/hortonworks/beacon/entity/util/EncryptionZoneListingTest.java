/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import org.apache.hadoop.fs.Path;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.notNull;
import static org.powermock.api.mockito.PowerMockito.when;


/**
 * Test class for EncryptionZoneListing.
 */
@PrepareForTest(EncryptionZoneListing.class)
public class EncryptionZoneListingTest extends PowerMockTestCase {

    @Mock
    private EncryptionZoneListing encryptionZoneListing;

    @BeforeClass
    public void setup() {
        PowerMockito.mockStatic(EncryptionZoneListing.class);
    }

    @Test
    public void testIsPathEncrypted() throws Exception {
        Map<String, String> encryptionZones = new HashMap<>();
        encryptionZones.put("/data/encrypt/", "default");
        String clusterName = "src";
        String fsEndPoint = "hdfs://localhost:8020";
        when(encryptionZoneListing.getEncryptionZones((String) notNull(), (String) notNull(), (Path) notNull()))
                .thenReturn(encryptionZones);
        when(encryptionZoneListing.getEncryptionKeyName(clusterName, "/data/encrypt/")).thenReturn(
                "default");
        when(encryptionZoneListing.getBaseEncryptedPath((String) notNull(), (String) notNull(), (Path) notNull()))
                .thenCallRealMethod();
        when(encryptionZoneListing.getBaseEncryptedPath((String) notNull(), (String) notNull(), (String) notNull()))
                .thenCallRealMethod();
        Assert.assertEquals(encryptionZoneListing.getBaseEncryptedPath(clusterName, fsEndPoint,
                new Path("/data/encrypt")), "/data/encrypt/");
        Assert.assertEquals(encryptionZoneListing.getBaseEncryptedPath(clusterName, fsEndPoint,
                new Path("/data/encrypt/media")), "/data/encrypt/");
        Assert.assertNull(encryptionZoneListing.getBaseEncryptedPath(clusterName, fsEndPoint,
                new Path("/data/encryptZone/media")));
        Assert.assertNull(encryptionZoneListing.getBaseEncryptedPath(clusterName, fsEndPoint, new Path("/")));
        Assert.assertNull(encryptionZoneListing.getBaseEncryptedPath(clusterName, fsEndPoint, new Path("/data")));
        Assert.assertNull(encryptionZoneListing.getBaseEncryptedPath(clusterName, fsEndPoint, ""));
    }
}
