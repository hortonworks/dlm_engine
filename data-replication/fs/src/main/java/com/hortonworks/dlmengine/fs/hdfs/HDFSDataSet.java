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

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.dlmengine.fs.FSDataSet;
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
    public String resolvePath(String path, ReplicationPolicy policy) {
        return path;
    }

    @Override
    public Configuration getHadoopConf() {
        return beaconCluster.getHadoopConfiguration();
    }

    @Override
    protected Configuration getHadoopConf(String path, ReplicationPolicy policy) throws BeaconException {
        throw new IllegalStateException();
    }

    @Override
    public void validateWriteAllowed() throws ValidationException {
        //do nothing
    }

    @Override
    public boolean isSnapshottable() throws BeaconException {
        return SnapshotListing.get().isSnapshottable(beaconCluster, getPathPart().toString());
    }

    public void createSnapshot(String snapshotName) throws BeaconException {
        LOG.info("Creating snapshot {} in {}", snapshotName, path);

        //Delete old one if it exists
        deleteSnapshot(snapshotName);
        try {
            fileSystem.createSnapshot(path, snapshotName);
        } catch (IOException e) {
            throw new BeaconException(e);
        }

    }

    public void deleteSnapshot(String snapshotName) throws BeaconException {
        try {
            if (fileSystem.exists(new Path(new Path(path, SNAPSHOT_DIR_PREFIX), snapshotName))) {
                LOG.debug("Deleting snapshot {} in {}", snapshotName, path);
                fileSystem.deleteSnapshot(path, snapshotName);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    private Path getPathPart(Path myPath) {
        return new Path(myPath.toUri().getPath());
    }

    private Path getPathPart() {
        return getPathPart(path);
    }

    @Override
    public void deleteAllSnapshots(final String snapshotNamePrefix) throws BeaconException {
        try {
            Path snapshottableDirectory = getPathPart();
            Path snapshotDir = new Path(path, SNAPSHOT_DIR_PREFIX);
            FileStatus[] snapshots = fileSystem.listStatus(snapshotDir, new PathFilter() {
                @Override
                public boolean accept(Path p) {
                    return p.getName().startsWith(snapshotNamePrefix);
                }
            });
            if (snapshots != null) {
                for (FileStatus snapshot : snapshots) {
                    LOG.info("Deleting Snapshot: {} in {}", snapshot.getPath().getName(), snapshottableDirectory);
                    fileSystem.deleteSnapshot(snapshottableDirectory, snapshot.getPath().getName());
                }
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void allowSnapshot() throws BeaconException {
        try {
            LOG.info("Making directory {} snapshottable", path);
            DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
            dfs.allowSnapshot(path);
            SnapshotListing.get().updateListing(beaconCluster, Path.SEPARATOR);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public void disallowSnapshot() throws BeaconException {
        DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
        try {
            LOG.info("Making directory {} non-snapshottable", path);
            dfs.disallowSnapshot(path);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        SnapshotListing.get().updateListing(beaconCluster, Path.SEPARATOR);
    }

    @Override
    public boolean isEncrypted(Path path) throws BeaconException {
        EncryptionZoneListing encryptionZoneListing = EncryptionZoneListing.get();
        String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(beaconCluster,
                getPathPart(path).toString());
        return encryptionZoneListing.isEncrypted(baseEncryptedPath);
    }

    @Override
    public void close() {
        //do nothing
    }

    public BeaconCluster getCluster() {
        return beaconCluster;
    }
}
