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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;

/**
 * Cache of HDFS snapshot list.
 */
public final class SnapshotListing extends FSListing<Set> {

    private static final SnapshotListing INSTANCE = new SnapshotListing();

    private SnapshotListing() {
    }

    public static SnapshotListing get() {
        return INSTANCE;
    }

    public boolean isSnapshottable(String clusterName, String fsEndPoint, String path) throws BeaconException {
        String baseSnapshotPath = getBaseListing(clusterName, fsEndPoint, path);
        return StringUtils.isNotEmpty(baseSnapshotPath);
    }

    @Override
    protected Set<String> getListing(String clusterName, String fsEndPoint) throws BeaconException {
        try {
            DistributedFileSystem hdfs = (DistributedFileSystem) FSUtils.getFileSystem(fsEndPoint,
                    ClusterHelper.getHAConfigurationOrDefault(clusterName), false);
            SnapshottableDirectoryStatus[] snapshottableDirListing = hdfs.getSnapshottableDirListing();
            Set<String> listing = new HashSet<>();
            if (ArrayUtils.isNotEmpty(snapshottableDirListing)) {
                for (SnapshottableDirectoryStatus dir : snapshottableDirListing) {
                    Path snapshotDirPath = dir.getFullPath();
                    String snapshotPath = snapshotDirPath.toString().endsWith(BeaconConstants.FORWARD_SLASH)
                            ? snapshotDirPath.toString() : snapshotDirPath.toString() + BeaconConstants.FORWARD_SLASH;
                    listing.add(snapshotPath);
                }
            }
            return listing;
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    protected boolean contains(String clusterName, String path) {
        if (listingMap.containsKey(clusterName)) {
            return listingMap.get(clusterName).contains(path);
        }
        return false;
    }

    @Override
    protected int getRefreshFrequency() {
        return BeaconConfig.getInstance().getEngine().getRefreshSnapshotDirs();
    }
}
