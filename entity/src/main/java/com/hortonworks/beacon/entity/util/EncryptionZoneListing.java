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

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class on HDFS Dataset.
 */
public final class EncryptionZoneListing {

    private static final EncryptionZoneListing INSTANCE = new EncryptionZoneListing();

    private static Map<String, Map<String, String>> encryptionZones = new ConcurrentHashMap<>();
    private static Map<String, Long> lastUpdated = new ConcurrentHashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(EncryptionZoneListing.class);

    private EncryptionZoneListing() {
    }

    public static EncryptionZoneListing get() {
        return INSTANCE;
    }

    private void setEncryptedZones(String clusterName, String fsEndPoint) throws URISyntaxException, IOException,
            BeaconException {
        HdfsAdmin hdfsAdmin = new HdfsAdmin(new URI(fsEndPoint),
                ClusterHelper.getHAConfigurationOrDefault(clusterName));
        RemoteIterator<EncryptionZone> iterator = hdfsAdmin.listEncryptionZones();
        Map<String, String> encryptionZonesLocal = new HashMap<>();
        while (iterator.hasNext()) {
            EncryptionZone encryptionZone = iterator.next();
            String encryptionZonePath = encryptionZone.getPath().endsWith(BeaconConstants.FORWARD_SLASH)
                    ? encryptionZone.getPath() :encryptionZone.getPath() + BeaconConstants.FORWARD_SLASH;
            encryptionZonesLocal.put(encryptionZonePath, encryptionZone.getKeyName());
        }
        encryptionZones.put(clusterName, encryptionZonesLocal);
        lastUpdated.put(clusterName, System.currentTimeMillis());
    }

    public Map<String, String> getEncryptionZones(String clusterName, String fsEndPoint, Path path) throws
            URISyntaxException, IOException, BeaconException {
        if (!encryptionZones.containsKey(clusterName) || path.toString().equals(BeaconConstants.FORWARD_SLASH)
                || !isEncryptionZoneListValid(clusterName)) {
            setEncryptedZones(clusterName, fsEndPoint);
        }
        return encryptionZones.get(clusterName);
    }

    public String getBaseEncryptedPath(String clusterName, String fsEndPoint, String path) throws IOException,
            URISyntaxException,
            BeaconException {
        String encryptionKey = null;
        if (StringUtils.isNotEmpty(path)) {
            encryptionKey = getBaseEncryptedPath(clusterName, fsEndPoint, new Path(path));
        }
        return encryptionKey;
    }

    public String getEncryptionKeyName(String clusterName, String path) {
        String encryptionKeyName = "";
        if (encryptionZones.containsKey(clusterName)) {
            encryptionKeyName = encryptionZones.get(clusterName).get(path);
        }
        return encryptionKeyName;
    }

    /**
     * This function returns the base path of a path if it is encrypted or else returns null.
     * @param clusterName
     * @param fsEndPoint
     * @param path
     * @return the base encrypted path, if available.
     * @throws IOException
     * @throws URISyntaxException
     * @throws BeaconException
     */
    public String getBaseEncryptedPath(String clusterName, String fsEndPoint, Path path) throws IOException,
            URISyntaxException,
            BeaconException {
        Map<String, String> currentEncryptionZones = getEncryptionZones(clusterName, fsEndPoint, path);
        String pathToCheck = path.toUri().getPath();
        pathToCheck = pathToCheck.endsWith(BeaconConstants.FORWARD_SLASH)
                ? pathToCheck : pathToCheck + BeaconConstants.FORWARD_SLASH;
        boolean pathEncrypted = false;
        int lastIndex = 0;
        String tmpPathToCheck = BeaconConstants.FORWARD_SLASH;
        LOG.debug("Path to check: {}", pathToCheck);
        while (true) {
            lastIndex = pathToCheck.indexOf(BeaconConstants.FORWARD_SLASH, lastIndex) + 1;
            if (lastIndex == -1) {
                break;
            }
            tmpPathToCheck = pathToCheck.substring(0, lastIndex);
            if (StringUtils.isEmpty(tmpPathToCheck)) {
                break;
            }
            if (currentEncryptionZones.containsKey(tmpPathToCheck)) {
                pathEncrypted = true;
                break;
            }
        }
        if (pathEncrypted) {
            return tmpPathToCheck;
        } else {
            return null;
        }
    }

    private boolean isEncryptionZoneListValid(String clusterName) {
        boolean isValid = false;
        long elapsedTime = (System.currentTimeMillis() - lastUpdated.get(clusterName))/1000;
        if (lastUpdated.containsKey(clusterName) && elapsedTime
                < BeaconConfig.getInstance().getEngine().getRefreshEncryptionZones()) {
            isValid = true;
        }
        LOG.debug("Encryption zone list last refreshed {} seconds ago. Valid: {}", elapsedTime, isValid);
        return isValid;
    }
}
