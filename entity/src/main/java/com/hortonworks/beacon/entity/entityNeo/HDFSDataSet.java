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

package com.hortonworks.beacon.entity.entityNeo;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.entity.util.EncryptionZoneListing;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.hortonworks.beacon.constants.BeaconConstants.SNAPSHOT_DIR_PREFIX;

/**
 * On-Premise hdfs data set.
 */
public class HDFSDataSet extends FSDataSet {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSDataSet.class);
    private BeaconCluster beaconCluster;

    public HDFSDataSet(String path, String clusterName) throws BeaconException {
        super(path, new BeaconCluster(clusterName).getHadoopConfiguration());
        this.beaconCluster = new BeaconCluster(clusterName);
    }

    public HDFSDataSet(FileSystem fileSystem, String path) throws BeaconException {
        super(fileSystem, path);
    }

    @Override
    protected Configuration getHadoopConf(String path, ReplicationPolicy policy) throws BeaconException {
        throw new IllegalStateException();
    }

    @Override
    public boolean isSnapshottable() throws IOException {
        return fileSystem.exists(new Path(path, BeaconConstants.SNAPSHOT_DIR_PREFIX));
    }

    @Override
    public void deleteAllSnapshots(final String snapshotNamePrefix) throws IOException {
        String dirName = StringUtils.removeEnd(path, Path.SEPARATOR);
        String snapshotDir = dirName + Path.SEPARATOR + SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
        FileStatus[] snapshots = fileSystem.listStatus(new Path(snapshotDir), new PathFilter() {
            @Override
            public boolean accept(Path p) {
                return p.getName().startsWith(snapshotNamePrefix);
            }
        });
        if (snapshots != null) {
            for (FileStatus snapshot : snapshots) {
                LOG.info("Deleting Snapshot: {}", snapshot.getPath().getName());
                fileSystem.deleteSnapshot(new Path(dirName), snapshot.getPath().getName());
            }
        }
    }

    @Override
    public void allowSnapshot() throws IOException {
        DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
        dfs.allowSnapshot(new Path(path));
    }

    @Override
    public void disallowSnapshot() throws IOException {
        DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
        dfs.disallowSnapshot(new Path(path));
    }

    @Override
    public boolean isEncrypted() throws BeaconException {
        String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(beaconCluster.getName(),
                beaconCluster.getFsEndpoint(), path);
        return StringUtils.isNotEmpty(baseEncryptedPath);
    }
}
