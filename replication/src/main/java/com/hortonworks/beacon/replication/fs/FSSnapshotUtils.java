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

import com.hortonworks.beacon.entity.FSDRProperties;
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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * FS Snapshotutils Methods.
 */
public final class FSSnapshotUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FSSnapshotUtils.class);

    static final String SNAPSHOT_PREFIX = "beacon-snapshot-";
    private static final String SNAPSHOT_DIR_PREFIX = ".snapshot";

    private FSSnapshotUtils() {
    }

    /* Path passed should be fully qualified absolute path */
    public static boolean isSnapShotsAvailable(DistributedFileSystem hdfs, Path path) throws BeaconException {
        if (path == null) {
            throw new BeaconException("isSnapShotsAvailable: Path cannot be null or empty");
        }
        try {
            LOG.debug("Validating if dir: {} is snapshotable.", path.toString());
            URI pathUri = path.toUri();
            if (pathUri.getAuthority() == null) {
                LOG.error("{} is not fully qualified path", path);
                throw new BeaconException("isSnapShotsAvailable: {} is not fully qualified path", path);
            }
            SnapshottableDirectoryStatus[] snapshotableDirs = hdfs.getSnapshottableDirListing();
            if (snapshotableDirs != null && snapshotableDirs.length > 0) {
                for (SnapshottableDirectoryStatus dir : snapshotableDirs) {
                    Path snapshotDirPath = dir.getFullPath();
                    URI snapshorDirUri = snapshotDirPath.toUri();
                    if (snapshorDirUri.getAuthority() == null) {
                        snapshotDirPath = new Path(hdfs.getUri().toString(), snapshotDirPath);
                    }
                    LOG.debug("snapshotDirPath: {}", snapshotDirPath);
                    String pathToCheck = path.toString().endsWith("/") ? path.toString() : path.toString() + "/";
                    String snapShotPathToCheck = snapshotDirPath.toString().endsWith("/")
                            ? path.toString() : snapshotDirPath.toString() + "/";
                    if (pathToCheck.startsWith(snapShotPathToCheck)) {
                        LOG.debug("isHCFS: {0}", "true");
                        return true;
                    }
                }
            }
            return false;
        } catch (IOException e) {
            LOG.error("Unable to verify if dir : {} is snapshot-able. {}", path.toString(), e.getMessage());
            throw new BeaconException(e, "Unable to verify if dir {} is snapshot-able", path.toString());
        }
    }

    public static boolean isDirectorySnapshottable(FileSystem sourceFs, FileSystem targetFs,
                                                   String sourceStagingUri, String targetStagingUri)
            throws BeaconException {

        boolean sourceSnapshottableDirectory = checkSnapshottableDirectory(sourceFs, sourceStagingUri);
        boolean targetSnapshottableDirectory =  false;

        if (sourceSnapshottableDirectory) {
            targetSnapshottableDirectory = checkSnapshottableDirectory(targetFs, targetStagingUri);
        }
        return sourceSnapshottableDirectory && targetSnapshottableDirectory;
    }

    public static boolean checkSnapshottableDirectory(FileSystem fs, String stagingUri) throws BeaconException {
        if (FSUtils.isHCFS(new Path(stagingUri)) || FSUtils.isHCFS(new Path(stagingUri))) {
            return false;
        }
        try {
            if (fs.exists(new Path(stagingUri))) {
                if (!isSnapShotsAvailable((DistributedFileSystem) fs, new Path(stagingUri))) {
                    return false;
                }
            } else {
                throw new BeaconException("{} does not exist.", stagingUri);
            }
        } catch (IOException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return true;
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

    public static void createFSDirectory(FileSystem fs, final  Configuration conf, FsPermission fsPermission,
                                               String owner, String group,
                                               String targetDataSet, boolean isSnapshottable) throws BeaconException {
        try {
            LOG.info("Creating target directory with permission : {} owner: {} group: {}", fsPermission.toString(),
                owner, group);
            FileSystemClientFactory.mkdirs(fs, new Path(targetDataSet), fsPermission);
            fs.setOwner(new Path(targetDataSet), owner, group);
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();
            DFSAdmin dfsAdmin = ugi.doAs(new PrivilegedAction<DFSAdmin>() {
                @Override
                public DFSAdmin run() {
                    return new DFSAdmin(conf);
                }
            });
            if (isSnapshottable) {
                String[] arg = {"-allowSnapshot", targetDataSet};
                dfsAdmin.allowSnapshot(arg);
            }
        } catch (IOException ioe) {
            throw new BeaconException(ioe);
        }
    }
}
