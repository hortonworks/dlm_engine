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
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.util.EvictionHelper;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * FS Snapshotutils Methods.
 */
public final class FSSnapshotUtils {
    private static final BeaconLog LOG = BeaconLog.getLog(FSSnapshotUtils.class);

    static final String SNAPSHOT_PREFIX = "beacon-snapshot-";
    private static final String SNAPSHOT_DIR_PREFIX = ".snapshot";

    private FSSnapshotUtils() {
    }

    /* Path passed should be fully qualified absolute path */
    public static boolean isSnapShotsAvailable(DistributedFileSystem hdfs, Path path) throws BeaconException {
        if (path == null) {
            throw new BeaconException(MessageCode.COMM_010008.name(), "isSnapShotsAvailable: Path");
        }
        try {
            LOG.info(MessageCode.REPL_000049.name(), path.toString());
            URI pathUri = path.toUri();
            if (pathUri.getAuthority() == null) {
                LOG.error(MessageCode.REPL_000011.name(), path);
                throw new BeaconException(MessageCode.REPL_000011.name(), path);
            }
            SnapshottableDirectoryStatus[] snapshotableDirs = hdfs.getSnapshottableDirListing();
            if (snapshotableDirs != null && snapshotableDirs.length > 0) {
                for (SnapshottableDirectoryStatus dir : snapshotableDirs) {
                    Path snapshotDirPath = dir.getFullPath();
                    URI snapshorDirUri = snapshotDirPath.toUri();
                    if (snapshorDirUri.getAuthority() == null) {
                        snapshotDirPath = new Path(hdfs.getUri().toString(), snapshotDirPath);
                    }
                    LOG.debug("snapshotDirPath: {0}", snapshotDirPath);
                    if (path.toString().startsWith(snapshotDirPath.toString())) {
                        LOG.debug("isHCFS: {0}", "true");
                        return true;
                    }
                }
            }
            return false;
        } catch (IOException e) {
            LOG.error(MessageCode.REPL_000012.name(), path.toString(), e.getMessage());
            throw new BeaconException(MessageCode.REPL_000012.name(), e, path.toString());
        }
    }

    public static boolean isDirectorySnapshottable(FileSystem sourceFs, FileSystem targetFs,
                                                   String sourceStagingUri, String targetStagingUri)
            throws BeaconException {
        if (FSUtils.isHCFS(new Path(sourceStagingUri)) || FSUtils.isHCFS(new Path(targetStagingUri))) {
            return false;
        }
        try {
            if (sourceFs.exists(new Path(sourceStagingUri))) {
                if (!isSnapShotsAvailable((DistributedFileSystem) sourceFs, new Path(sourceStagingUri))) {
                    return false;
                }
            } else {
                throw new BeaconException(MessageCode.REPL_000013.name(), sourceStagingUri);
            }

            if (targetFs.exists(new Path(targetStagingUri))) {
                if (!isSnapShotsAvailable((DistributedFileSystem) targetFs, new Path(targetStagingUri))) {
                    return false;
                }
            } else {
                throw new BeaconException(MessageCode.REPL_000013.name(), targetStagingUri);
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
            LOG.error(MessageCode.REPL_000014.name(), targetDir, e.getMessage());
            throw new BeaconException(MessageCode.REPL_000014.name(), e, targetDir);
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
            LOG.error(MessageCode.REPL_000079.name(), stagingUri, snapshotName);
            throw new BeaconException(MessageCode.REPL_000079.name(), e, stagingUri, snapshotName);
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
            LOG.error(MessageCode.REPL_000080.name(), stagingUri, snapshotName);
            throw new BeaconException(MessageCode.REPL_000080.name(), e, stagingUri, snapshotName);
        }
    }

    private static void createSnapshotInFileSystem(String dirName, String snapshotName,
                                                   FileSystem fs) throws BeaconException {
        try {
            LOG.info(MessageCode.REPL_000050.name(), snapshotName, dirName);
            fs.createSnapshot(new Path(dirName), snapshotName);
        } catch (IOException e) {
            LOG.error(MessageCode.REPL_000051.name(), snapshotName,
                    fs.getConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY), e.getMessage());
            throw new BeaconException(MessageCode.REPL_000015.name(), e, snapshotName);
        }
    }

    static void evictSnapshots(DistributedFileSystem fs, String dirName, String ageLimit, int numSnapshots)
            throws BeaconException {
        try {
            LOG.info(MessageCode.REPL_000052.name(), dirName, ageLimit, numSnapshots);

            long evictionTime = System.currentTimeMillis() - EvictionHelper.evalExpressionToMilliSeconds(ageLimit);

            dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
            String snapshotDir = dirName + Path.SEPARATOR + SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
            FileStatus[] snapshots = fs.listStatus(new Path(snapshotDir));
            if (snapshots.length <= numSnapshots) {
                LOG.info(MessageCode.REPL_000053.name(), snapshots.length, numSnapshots);
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
                    LOG.info(MessageCode.REPL_000054.name(), new Path(dirName), snapshots[i].getPath().getName());
                    synchronized (FSSnapshotUtils.class) {
                        if (fs.exists(new Path(snapshotDir, snapshots[i].getPath().getName()))) {
                            fs.deleteSnapshot(new Path(dirName), snapshots[i].getPath().getName());
                        }
                    }
                }
            }

        } catch (ELException ele) {
            LOG.warn(MessageCode.COMM_010001.name(), ageLimit, ele.getMessage());
            throw new BeaconException(MessageCode.COMM_010001.name(), ele, ageLimit, ele.getMessage());
        } catch (IOException ioe) {
            LOG.warn(MessageCode.REPL_000016.name(), dirName, ioe);
            throw new BeaconException(MessageCode.REPL_000016.name(), ioe, dirName);
        }

    }

    static void handleSnapshotCreation(FileSystem fs, String stagingURI, String fsReplicationName)
            throws BeaconException {
        LOG.info(MessageCode.REPL_000055.name(), fs.toString(), stagingURI);
        FSSnapshotUtils.createSnapshotInFileSystem(stagingURI, fsReplicationName, fs);
    }

    static void handleSnapshotEviction(FileSystem fs, Properties fsDRProperties, String staginURI)
            throws BeaconException {
        String ageLimit = fsDRProperties.getProperty(
                FSDRProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
        int numSnapshots = Integer.parseInt(
                fsDRProperties.getProperty(FSDRProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
        LOG.info(MessageCode.REPL_000056.name(), fs.toString());
        FSSnapshotUtils.evictSnapshots((DistributedFileSystem) fs, staginURI, ageLimit, numSnapshots);
    }

    private static String getSnapshotDir(String dirName) {
        dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
        return dirName + Path.SEPARATOR + SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
    }
}
