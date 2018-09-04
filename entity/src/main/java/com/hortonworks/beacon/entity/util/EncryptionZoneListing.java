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
package com.hortonworks.beacon.entity.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;

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
            HdfsAdmin hdfsAdmin = HdfsAdminFactory.getInstance().createHDFSAdmin(fsEndPoint, clusterName);
            RemoteIterator<EncryptionZone> iterator = hdfsAdmin.listEncryptionZones();
            while (iterator.hasNext()) {
                EncryptionZone encryptionZone = iterator.next();
                String encryptionZonePath = encryptionZone.getPath().endsWith(File.separator)
                        ? encryptionZone.getPath() : encryptionZone.getPath() + File.separator;
                encryptionZonesLocal.put(encryptionZonePath, encryptionZone.getKeyName());
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        return encryptionZonesLocal;
    }

    @Override
    protected String getBaseListing(String clusterName, String pathToCheck) {
        int lastIndex = 0;
        String tmpPathToCheck;

        while (true) {
            lastIndex = pathToCheck.indexOf(File.separator, lastIndex) + 1;
            if (lastIndex == -1) {
                break;
            }
            tmpPathToCheck = pathToCheck.substring(0, lastIndex);
            if (StringUtils.isEmpty(tmpPathToCheck)) {
                break;
            }
            if (contains(clusterName, tmpPathToCheck)) {
                return tmpPathToCheck;
            }
        }
        return null;
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
