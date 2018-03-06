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


import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * class on various HDFS Listing like snapshot listingMap, encryption zone listingMap.
 */
public abstract class FSListing<T> {

    private Map<String, Long> lastUpdated = new ConcurrentHashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(FSListing.class);
    protected Map<String, T> listingMap = new ConcurrentHashMap<>();

    public void updateListing(String clusterName, String fsEndPoint, String path) throws BeaconException {
        String rootPath = Path.SEPARATOR;
        if (path.equals(rootPath) || !isListingValid(clusterName)) {
            LOG.debug("Updating the cache for cluster: {}", clusterName);
            synchronized(this) {
                if (path.equals(rootPath) || !isListingValid(clusterName)) {
                    T listing = getListing(clusterName, fsEndPoint);
                    listingMap.put(clusterName, listing);
                    lastUpdated.put(clusterName, System.currentTimeMillis());
                }
            }
        }
    }

    protected abstract T getListing(String clusterName, String fsEndPoint) throws BeaconException;

    protected String getBaseListing(String clusterName, String fsEndPoint, String path) throws BeaconException {
        if (StringUtils.isNotEmpty(path)) {
            String decodedPath = Path.getPathWithoutSchemeAndAuthority(new Path(path)).toString();
            String pathToCheck = decodedPath.endsWith(File.separator)
                    ? decodedPath : decodedPath + File.separator;
            LOG.debug("Path to check: {}", pathToCheck);
            updateListing(clusterName, fsEndPoint, pathToCheck);

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
        }
        return null;
    }

    protected abstract boolean contains(String clusterName, String tmpPathToCheck);

    protected boolean isListingValid(String clusterName) {
        if (lastUpdated.containsKey(clusterName)
                && (System.currentTimeMillis() - lastUpdated.get(clusterName))/1000 < getRefreshFrequency()) {
            return true;
        }
        return false;
    }

    protected abstract int getRefreshFrequency();
}
