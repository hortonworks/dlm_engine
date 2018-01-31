/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.api;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.fs.FSListing;

/**
 * Cache of HDFS encryption zones.
 */
public final class EncryptionZoneListing extends FSListing<Map> {

    private static final EncryptionZoneListing INSTANCE = new EncryptionZoneListing();

    private EncryptionZoneListing() {
    }

    public static EncryptionZoneListing get() {
        return INSTANCE;
    }

    public String getEncryptionKeyName(String clusterName, String baseEncryptedPath) {
        Map listing = listingMap.get(clusterName);
        if (listing != null) {
            return (String) listing.get(baseEncryptedPath);
        }
        return null;
    }

    /**
     * Returns the root encrypted path for a give path. Example, if /data/app1 is encrypted, getBaseEncryptedPath
     * ("/data/app1/file") returns /data/app1
     * @param clusterName
     * @param fsEndPoint
     * @param path
     * @return the base encrypted path if available. Else, null
     * @throws BeaconException
     */
    public String getBaseEncryptedPath(String clusterName, String fsEndPoint, String path) throws BeaconException {
        return getBaseListing(clusterName, fsEndPoint, path);
    }

    public boolean isEncrypted(String baseEncryptionPath) {
        return StringUtils.isNotEmpty(baseEncryptionPath);
    }

    @Override
    protected Map getListing(String clusterName, String fsEndPoint) throws BeaconException {
        Map<String, String> encryptionZonesLocal = new HashMap<>();
        try {
            HdfsAdmin hdfsAdmin = new HdfsAdmin(new URI(fsEndPoint),
                    ClusterHelper.getHAConfigurationOrDefault(clusterName));
            RemoteIterator<EncryptionZone> iterator = hdfsAdmin.listEncryptionZones();
            while (iterator.hasNext()) {
                EncryptionZone encryptionZone = iterator.next();
                String encryptionZonePath = encryptionZone.getPath().endsWith(BeaconConstants.FORWARD_SLASH)
                        ? encryptionZone.getPath() : encryptionZone.getPath() + BeaconConstants.FORWARD_SLASH;
                encryptionZonesLocal.put(encryptionZonePath, encryptionZone.getKeyName());
            }
        } catch (IOException | URISyntaxException e) {
            throw new BeaconException(e);
        }
        return encryptionZonesLocal;
    }

    @Override
    protected boolean contains(String clusterName, String path) {
        if (listingMap.containsKey(clusterName)) {
            return listingMap.get(clusterName).containsKey(path);
        }
        return false;
    }

    @Override
    protected int getRefreshFrequency() {
        return BeaconConfig.getInstance().getEngine().getRefreshEncryptionZones();
    }
}
