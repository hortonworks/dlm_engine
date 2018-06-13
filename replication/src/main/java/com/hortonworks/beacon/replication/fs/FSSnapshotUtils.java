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

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.EvictionHelper;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.hortonworks.beacon.constants.BeaconConstants.SNAPSHOT_DIR_PREFIX;
import static com.hortonworks.beacon.constants.BeaconConstants.SNAPSHOT_PREFIX;

/**
 * FS Snapshotutils Methods.
 */
public final class FSSnapshotUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FSSnapshotUtils.class);

    public static final String TEMP_REPLICATION_SNAPSHOT = "tempReplicationSnapshot";

    private FSSnapshotUtils() {
    }

    public static boolean isSnapShotsAvailable(String clusterName, Path path) throws BeaconException {
        if (path == null) {
            throw new BeaconException("isSnapShotsAvailable: Path cannot be null or empty");
        }
        LOG.debug("Validating if dir: {} is snapshotable.", path.toString());
        URI pathUri = path.toUri();
        if (pathUri.getAuthority() == null) {
            LOG.error("{} is not fully qualified path", path);
            throw new BeaconException("isSnapShotsAvailable: {} is not fully qualified path", path);
        }
        Cluster cluster = ClusterHelper.getActiveCluster(clusterName);
        return SnapshotListing.get().isSnapshottable(cluster.getName(), cluster.getFsEndpoint(),
                path.toUri().getPath());
    }

    public static boolean isDirectorySnapshottable(String sourceClusterName, String targetClusterName,
                                                   String sourceStagingUri, String targetStagingUri)
            throws BeaconException {

        boolean sourceSnapshottableDirectory = checkSnapshottableDirectory(sourceClusterName, sourceStagingUri);
        boolean targetSnapshottableDirectory =  false;

        if (sourceSnapshottableDirectory) {
            targetSnapshottableDirectory = checkSnapshottableDirectory(targetClusterName, targetStagingUri);
        }
        return sourceSnapshottableDirectory && targetSnapshottableDirectory;
    }

    public static boolean checkSnapshottableDirectory(String clusterName, String stagingUri) throws BeaconException {
        if (FSUtils.isHCFS(new Path(stagingUri)) || FSUtils.isHCFS(new Path(stagingUri))) {
            return false;
        }
        return isSnapShotsAvailable(clusterName, new Path(stagingUri));
    }

    static String findLatestReplicatedSnapshot(DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                                               String sourceDir, String targetDir) throws BeaconException {
        try {
            FileStatus[] sourceSnapshots = sourceFs.listStatus(new Path(getSnapshotDir(sourceDir)));
            Set<String> sourceSnapshotNames = new HashSet<>();
            for (FileStatus snapshot : sourceSnapshots) {
                sourceSnapshotNames.add(snapshot.getPath().getName());
            }

            FileStatus[] targetSnapshots = targetFs.listStatus(new Path(getSnapshotDir(targetDir)));
            if (targetSnapshots.length > 0) {
                //sort target snapshots in desc order of creation time.
                Arrays.sort(targetSnapshots, new Comparator<FileStatus>() {
                    @Override
                    public int compare(FileStatus f1, FileStatus f2) {
                        return Long.compare(f2.getModificationTime(), f1.getModificationTime());
                    }
                });

                // get most recent snapshot name that exists in source.
                for (FileStatus targetSnapshot : targetSnapshots) {
                    String name = targetSnapshot.getPath().getName();
                    if (sourceSnapshotNames.contains(name)) {
                        return name;
                    }
                }
                // If control reaches here,
                // there are snapshots on target, but none are replicated from source. Return null.
            }
            return null;
        } catch (IOException e) {
            LOG.error("Unable to find latest snapshot on targetDir {} {}", targetDir, e.getMessage());
            throw new BeaconException(e, "Unable to find latest snapshot on targetDir {}", targetDir);
        }
    }

    /**
     * Create a new snapshot. If snapshot already exists, delete and re-create.
     * @param fileSystem HDFS file system
     * @param stagingUri snapshot path
     * @param snapshotName snapshot name
     * @throws BeaconException throws if any error.
     */
    static void checkAndCreateSnapshot(FileSystem fileSystem, String stagingUri, String snapshotName)
            throws BeaconException {
        try {
            checkAndDeleteSnapshot(fileSystem, stagingUri, snapshotName);
            fileSystem.createSnapshot(new Path(stagingUri), snapshotName);
        } catch (Exception e) {
            LOG.error(
                "Exception occurred while checking and create recovery snapshot. stagingUri: {}, snapshotName: {}",
                stagingUri, snapshotName);
            throw new BeaconException(e, "Unable to create snapshot {}", snapshotName);
        }
    }

    /**
     * Delete snapshot if exists.
     * @param fileSystem HDFS file system
     * @param stagingUri snapshot path
     * @param snapshotName snapshot name
     * @throws BeaconException throws if any error.
     */
    static void checkAndDeleteSnapshot(FileSystem fileSystem, String stagingUri, String snapshotName)
            throws BeaconException {
        String parent = stagingUri + Path.SEPARATOR + SNAPSHOT_DIR_PREFIX;
        Path snapshotPath = new Path(parent, snapshotName);
        try {
            boolean exists = fileSystem.exists(snapshotPath);
            if (exists) {
                fileSystem.deleteSnapshot(new Path(stagingUri), snapshotName);
            }
        } catch (Exception e) {
            LOG.error(
                "Exception occurred while checking and delete recovery snapshot. stagingUri: {}, snapshotName: {}",
                stagingUri, snapshotName);
            throw new BeaconException(e,
                    "Exception occurred while checking and delete recovery snapshot. stagingUri: {}, snapshotName: {}",
                    stagingUri, snapshotName);
        }
    }

    static void checkAndRenameSnapshot(FileSystem fileSystem, String path, String oldSnapshot, String newSnapshot)
            throws BeaconException {
        try {
            boolean exists = fileSystem.exists(new Path(getSnapshotDir(path), oldSnapshot));
            if (exists) {
                fileSystem.renameSnapshot(new Path((path)), oldSnapshot, newSnapshot);
            } else {
                throw new BeaconException("No snapshot exists with name: [{}] path: [{}]", oldSnapshot, path);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    private static void createSnapshotInFileSystem(String dirName, String snapshotName,
                                                   FileSystem fs) throws BeaconException {
        try {
            LOG.info("Creating snapshot {} in directory {}", snapshotName, dirName);
            fs.createSnapshot(new Path(dirName), snapshotName);
        } catch (IOException e) {
            LOG.error("Unable to create snapshot {} in filesystem {}. Exception is {}", snapshotName,
                fs.getConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY), e.getMessage());
            throw new BeaconException(e, "Unable to create snapshot {}", snapshotName);
        }
    }

    static void evictSnapshots(DistributedFileSystem fs, String dirName, String ageLimit, int numSnapshots)
            throws BeaconException {
        try {
            LOG.info("Started evicting snapshots on dir {}, agelimit {}, numSnapshot {}", dirName, ageLimit,
                numSnapshots);

            long evictionTime = System.currentTimeMillis() - EvictionHelper.evalExpressionToMilliSeconds(ageLimit);

            dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
            String snapshotDir = dirName + Path.SEPARATOR + SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
            FileStatus[] snapshots = fs.listStatus(new Path(snapshotDir));
            if (snapshots.length <= numSnapshots) {
                LOG.info("No eviction required as number of snapshots: {} is less than numSnapshots: {}",
                    snapshots.length, numSnapshots);
                // no eviction needed
                return;
            }

            // Sort by last modified time, ascending order.
            Arrays.sort(snapshots, new Comparator<FileStatus>() {
                @Override
                public int compare(FileStatus f1, FileStatus f2) {
                    return Long.compare(f1.getModificationTime(), f2.getModificationTime());
                }
            });

            for (int i = 0; i < (snapshots.length - numSnapshots); i++) {
                // delete if older than ageLimit while retaining numSnapshots
                if (snapshots[i].getModificationTime() < evictionTime) {
                    LOG.info("Deleting snapshots with path: {} and snapshot path: {}", new Path(dirName),
                        snapshots[i].getPath().getName());
                    synchronized (FSSnapshotUtils.class) {
                        if (fs.exists(new Path(snapshotDir, snapshots[i].getPath().getName()))) {
                            fs.deleteSnapshot(new Path(dirName), snapshots[i].getPath().getName());
                        }
                    }
                }
            }

        } catch (ELException ele) {
            LOG.warn("Unable to parse retention age limit: {} {}", ageLimit, ele.getMessage());
            throw new BeaconException(ele, "Unable to parse retention age limit: {} {}", ele.getMessage(), ageLimit);
        } catch (IOException ioe) {
            LOG.warn("Unable to evict snapshots from dir {} {}", dirName, ioe);
            throw new BeaconException(ioe, "Unable to evict snapshots from dir {}", dirName);
        }
    }

    public static void deleteAllSnapshots(DistributedFileSystem fs, String dirName, final String prefix)
            throws BeaconException {
        try {
            dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
            String snapshotDir = dirName + Path.SEPARATOR + SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
            FileStatus[] snapshots = fs.listStatus(new Path(snapshotDir), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().startsWith(prefix);
                }
            });
            for (FileStatus snapshot: snapshots) {
                LOG.debug("Snapshot name: {}", snapshot.getPath().getName());
                fs.deleteSnapshot(new Path(dirName), snapshot.getPath().getName());
            }
        } catch (IOException e) {
            throw new BeaconException("Error while deleting existing snapshot(s).", e);
        }
    }


    static void handleSnapshotCreation(FileSystem fs, String stagingURI, String fsReplicationName)
            throws BeaconException {
        LOG.info("Creating snapshot on FS: {} for URI: {}", fs.toString(), stagingURI);
        FSSnapshotUtils.createSnapshotInFileSystem(stagingURI, fsReplicationName, fs);
    }

    static void handleSnapshotEviction(FileSystem fs, Properties fsDRProperties, String staginURI)
            throws BeaconException {
        String ageLimit = fsDRProperties.getProperty(
                FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
        int numSnapshots = Integer.parseInt(
                fsDRProperties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
        LOG.info("Snapshots eviction on FS: {}", fs.toString());
        FSSnapshotUtils.evictSnapshots((DistributedFileSystem) fs, staginURI, ageLimit, numSnapshots);
    }

    private static String getSnapshotDir(String dirName) {
        dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
        return dirName + Path.SEPARATOR + SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
    }

    public static void createFSDirectory(FileSystem fs, FsPermission fsPermission,
                                               String owner, String group,
                                               String targetDataSet) throws BeaconException {
        try {
            LOG.info("Creating target directory with permission : {} owner: {} group: {}", fsPermission.toString(),
                owner, group);
            FileSystemClientFactory.mkdirs(fs, new Path(targetDataSet), fsPermission);
            fs.setOwner(new Path(targetDataSet), owner, group);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    public static void allowSnapshot(final Configuration conf, String dataset, final URI fsEndPoint, Cluster cluster)
            throws
            IOException, InterruptedException, BeaconException {
        LOG.debug("Allowing snapshot on cluster {} at path {}", cluster.getName(), dataset);
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        HdfsAdmin hdfsAdmin = ugi.doAs(new PrivilegedExceptionAction<HdfsAdmin>() {
            @Override
            public HdfsAdmin run() throws IOException {
                return new HdfsAdmin(fsEndPoint, conf);
            }
        });
        hdfsAdmin.allowSnapshot(new Path(dataset));
        SnapshotListing.get().updateListing(cluster.getName(), cluster.getFsEndpoint(), Path.SEPARATOR);
    }

    static String getLatestSnapshot(FileSystem fileSystem, String path, String snapshotPrefix) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(getSnapshotDir(path)));
        if (fileStatuses.length > 0) {
            //sort target snapshots in desc order of creation time.
            Arrays.sort(fileStatuses, new Comparator<FileStatus>() {
                @Override
                public int compare(FileStatus f1, FileStatus f2) {
                    return Long.compare(f2.getModificationTime(), f1.getModificationTime());
                }
            });

            for (FileStatus fileStatus : fileStatuses) {
                String snapshotName = fileStatus.getPath().getName();
                if (snapshotName.startsWith(snapshotPrefix)) {
                    return snapshotName;
                }
            }
        }
        return null;
    }

    static String getSnapshotName(String jobName) {
        String fsReplicationName;
        fsReplicationName = SNAPSHOT_PREFIX
                .concat(jobName)
                .concat("-")
                .concat(String.valueOf(System.currentTimeMillis()));
        return fsReplicationName;
    }
}
