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
package com.hortonworks.beacon.replication.fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public boolean isSnapshottable(String clusterName, String fsEndPoint, String path) throws BeaconException {
        String baseSnapshotPath = getBaseListing(clusterName, fsEndPoint, path);
        return StringUtils.isNotEmpty(baseSnapshotPath);
    }

    @Override
    protected Set<String> getListing(String clusterName, String fsEndPoint) throws BeaconException {
        try {
            FileSystem fs = FSUtils.getFileSystem(fsEndPoint, ClusterHelper.getHAConfigurationOrDefault(clusterName));
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
    protected boolean contains(String clusterName, String path) {
        LOG.debug("Snapshot listing set: {}", listingMap.entrySet());
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
