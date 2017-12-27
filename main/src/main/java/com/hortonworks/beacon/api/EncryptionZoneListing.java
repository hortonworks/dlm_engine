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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class on HDFS Dataset.
 */
public final class EncryptionZoneListing {

    private static final EncryptionZoneListing INSTANCE = new EncryptionZoneListing();

    private static Map<String, Set<String>> encryptionZones = new ConcurrentHashMap<>();
    private static Map<String, Long> lastUpdated = new ConcurrentHashMap<>();

    private EncryptionZoneListing() {
    }

    public static EncryptionZoneListing get() {
        return INSTANCE;
    }

    private void setEncryptedZones(Cluster cluster) throws URISyntaxException, IOException {
        HdfsAdmin hdfsAdmin = new HdfsAdmin(new URI(cluster.getFsEndpoint()), new Configuration());
        RemoteIterator<EncryptionZone> iterator = hdfsAdmin.listEncryptionZones();
        Set<String> encryptionZonesLocal = new HashSet<>();
        while (iterator.hasNext()) {
            EncryptionZone encryptionZone = iterator.next();
            String encryptionZonePath = encryptionZone.getPath().endsWith(BeaconConstants.FORWARD_SLASH)
                    ? encryptionZone.getPath() :encryptionZone.getPath() + BeaconConstants.FORWARD_SLASH;
            encryptionZonesLocal.add(encryptionZonePath);
        }
        encryptionZones.put(cluster.getName(), encryptionZonesLocal);
        lastUpdated.put(cluster.getName(), System.currentTimeMillis());
    }

    public Set<String> getEncryptionZones(Cluster cluster, Path path) throws URISyntaxException,
            IOException {
        if (!encryptionZones.containsKey(cluster.getName()) || path.toString().equals(BeaconConstants.FORWARD_SLASH)
                || isEncryptionZoneListValid(cluster.getName())) {
            setEncryptedZones(cluster);
        }
        return encryptionZones.get(cluster.getName());
    }

    public boolean isPathEncrypted(Cluster cluster, String path) throws IOException, URISyntaxException,
            BeaconException {
        return isPathEncrypted(cluster, new Path(path));
    }

    public boolean isPathEncrypted(Cluster cluster, Path path) throws IOException, URISyntaxException,
            BeaconException {
        Set<String> currentEncryptionZones = getEncryptionZones(cluster, path);
        String pathToCheck = path.toString();
        pathToCheck = pathToCheck.endsWith("/") ? pathToCheck : pathToCheck + BeaconConstants.FORWARD_SLASH;
        boolean pathEncrypted = false;
        while (true) {
            pathToCheck = pathToCheck.substring(0, pathToCheck.lastIndexOf(BeaconConstants.FORWARD_SLASH));
            if (StringUtils.isEmpty(pathToCheck)) {
                break;
            }
            if (currentEncryptionZones.contains(pathToCheck + BeaconConstants.FORWARD_SLASH)) {
                pathEncrypted = true;
                break;
            }
        }
        return pathEncrypted;
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
