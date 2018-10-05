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
package com.hortonworks.dlmengine.fs.hdfs;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * Cache of HDFS snapshot list.
 */
public final class SnapshotListing extends FSListing<Set> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotListing.class);
    private static final SnapshotListing INSTANCE = new SnapshotListing();

    private SnapshotListing() {
    }

    public static SnapshotListing get() {
        return INSTANCE;
    }

    public boolean isSnapshottable(BeaconCluster cluster, String path) throws BeaconException {
        updateListing(cluster, path);
        String clusterName = cluster.getName();
        return listingMap.containsKey(clusterName) && listingMap.get(clusterName).contains(path);
    }

    @Override
    protected Set<String> getListing(BeaconCluster cluster) throws BeaconException {
        try {
            FileSystem fs = cluster.getFileSystem();
            Set<String> listing = new HashSet<>();
            if (fs instanceof DistributedFileSystem) {
                DistributedFileSystem hdfs = (DistributedFileSystem) fs;
                SnapshottableDirectoryStatus[]snapshottableDirListing = hdfs.getSnapshottableDirListing();
                if (ArrayUtils.isNotEmpty(snapshottableDirListing)) {
                    for (SnapshottableDirectoryStatus dir : snapshottableDirListing) {
                        Path snapshotDirPath = dir.getFullPath();
                        String decodedPath = new URI(snapshotDirPath.toString()).getPath();
                        String snapshotPath = decodedPath.endsWith(File.separator)
                                ? decodedPath : decodedPath + File.separator;
                        listing.add(snapshotPath);
                    }
                }
            }
            return listing;
        } catch (IOException | URISyntaxException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    protected int getRefreshFrequency() {
        return BeaconConfig.getInstance().getEngine().getRefreshSnapshotDirs();
    }
}
