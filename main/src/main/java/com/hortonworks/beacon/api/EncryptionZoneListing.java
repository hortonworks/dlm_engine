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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
    private static final Logger LOG = LoggerFactory.getLogger(DatasetListing.class);

    private EncryptionZoneListing() {
    }

    public static EncryptionZoneListing get() {
        return INSTANCE;
    }

    private void setEncryptedZones(Cluster cluster) throws URISyntaxException, IOException {
        HdfsAdmin hdfsAdmin = new HdfsAdmin(new URI(cluster.getFsEndpoint()), new Configuration());
        RemoteIterator<EncryptionZone> iterator = hdfsAdmin.listEncryptionZones();
        Map<String, String> encryptionZonesLocal = new HashMap<>();
        while (iterator.hasNext()) {
            EncryptionZone encryptionZone = iterator.next();
            String encryptionZonePath = encryptionZone.getPath().endsWith(BeaconConstants.FORWARD_SLASH)
                    ? encryptionZone.getPath() :encryptionZone.getPath() + BeaconConstants.FORWARD_SLASH;
            encryptionZonesLocal.put(encryptionZonePath, encryptionZone.getKeyName());
        }
        encryptionZones.put(cluster.getName(), encryptionZonesLocal);
        lastUpdated.put(cluster.getName(), System.currentTimeMillis());
    }

    public Map<String, String> getEncryptionZones(Cluster cluster, Path path) throws URISyntaxException,
            IOException {
        if (!encryptionZones.containsKey(cluster.getName()) || path.toString().equals(BeaconConstants.FORWARD_SLASH)
                || isEncryptionZoneListValid(cluster.getName())) {
            setEncryptedZones(cluster);
        }
        return encryptionZones.get(cluster.getName());
    }

    public String getBaseEncryptedPath(Cluster cluster, String path) throws IOException, URISyntaxException,
            BeaconException {
        String encryptionKey = null;
        if (StringUtils.isNotEmpty(path)) {
            encryptionKey = getBaseEncryptedPath(cluster, new Path(path));
        }
        return encryptionKey;
    }

    public String getEncryptionKeyName(Cluster cluster, String path) {
        String encryptionKeyName = "";
        if (encryptionZones.containsKey(cluster.getName())) {
            encryptionKeyName = encryptionZones.get(cluster.getName()).get(path);
        }
        return encryptionKeyName;
    }

    /**
     * This function returns the base path of a path if it is encrypted or else returns null.
     * @param cluster
     * @param path
     * @return
     * @throws IOException
     * @throws URISyntaxException
     * @throws BeaconException
     */

    public String getBaseEncryptedPath(Cluster cluster, Path path) throws IOException, URISyntaxException,
            BeaconException {
        Map<String, String> currentEncryptionZones = getEncryptionZones(cluster, path);
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
        if (lastUpdated.containsKey(clusterName) && (System.currentTimeMillis() - lastUpdated.get(clusterName))/1000
                < BeaconConfig.getInstance().getEngine().getRefreshEncryptionZones()) {
            isValid = true;
        }
        return isValid;
    }
}
