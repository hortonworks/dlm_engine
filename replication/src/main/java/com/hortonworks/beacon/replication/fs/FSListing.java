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


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * class on various HDFS Listing like snapshot listingMap, encryption zone listingMap.
 */
public abstract class FSListing<T> {

    private Map<String, Long> lastUpdated = new ConcurrentHashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(FSListing.class);
    protected Map<String, T> listingMap = new ConcurrentHashMap<>();

    protected void updateListing(String clusterName, String fsEndPoint, String path) throws BeaconException {
        if (path.toString().equals(BeaconConstants.FORWARD_SLASH)|| !isListingValid(clusterName)) {
            synchronized(this) {
                if (!isListingValid(clusterName)) {
                    T listing = getListing(clusterName, fsEndPoint);
                    listingMap.put(clusterName, listing);
                    lastUpdated.put(clusterName, System.currentTimeMillis());
                }
            }
        }
    }

    protected abstract T getListing(String clusterName, String fsEndPoint) throws BeaconException;

    protected String getBaseListing(String clusterName, String fsEndPoint, String path) throws BeaconException {
        LOG.debug("Path to check: {}", path);
        String pathToCheck = path.endsWith(BeaconConstants.FORWARD_SLASH)
                ? path : path + BeaconConstants.FORWARD_SLASH;

        updateListing(clusterName, fsEndPoint, pathToCheck);

        int lastIndex = 0;
        String tmpPathToCheck;

        while (true) {
            lastIndex = pathToCheck.indexOf(BeaconConstants.FORWARD_SLASH, lastIndex) + 1;
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
