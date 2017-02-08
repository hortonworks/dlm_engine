/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.util;

import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Filesystem handling utilites.
 */
public final class FSUtils {
    private FSUtils() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);
    private static final List<String> HDFS_SCHEME_PREFIXES =
            Arrays.asList("file", "hdfs", "hftp", "hsftp", "webhdfs", "swebhdfs");

    public static final String SNAPSHOT_PREFIX = "beacon-snapshot-";
    public static final String SNAPSHOT_DIR_PREFIX = ".snapshot";
    public static final String TDE_ENCRYPTION_ENABLED = "tdeEncryptionEnabled";

    private static Configuration defaultConf = new Configuration();

    public static Configuration getDefaultConf() {
        return defaultConf;
    }

    public static void setDefaultConf(Configuration conf) {
        FSUtils.defaultConf = conf;
    }

    public static boolean isHCFS(Path filePath) throws BeaconException {

        if (filePath == null) {
            throw new BeaconException("filePath cannot be empty");
        }

        String scheme;
        try {
            FileSystem f = FileSystem.get(filePath.toUri(), getDefaultConf());
            scheme = f.getScheme();
            if (StringUtils.isBlank(scheme)) {
                throw new BeaconException("Cannot get valid scheme for " + filePath);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }

        return HDFS_SCHEME_PREFIXES.contains(scheme.toLowerCase().trim()) ? false : true;
    }


    public static FileSystem getFileSystem(String storageUrl,
                                           Configuration conf,
                                           boolean isHCFS) throws BeaconException {
        if (isHCFS) {
            conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, storageUrl);
            return FileSystemClientFactory.get().createProxiedFileSystem(conf);
        } else {
            conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, storageUrl);
            return FileSystemClientFactory.get().createDistributedProxiedFileSystem(conf);
        }
    }

    /* Path passed should be fully qualified absolute path */
    public static boolean isSnapShotsAvailable(DistributedFileSystem hdfs, Path path) throws BeaconException {
        if (path == null) {
            throw new BeaconException("isSnapShotsAvailable: Path cannot be null or empty");
        }
        try {
            LOG.info("Validating if dir : {} is snapshotable.", path.toString());
            URI pathUri = path.toUri();
            if (pathUri.getAuthority() == null) {
                LOG.error("{} is not fully qualified path", path);
                throw new BeaconException("isSnapShotsAvailable: " + path + " is not fully qualified path");
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
                    if (path.toString().startsWith(snapshotDirPath.toString())) {
                        LOG.debug("isHCFS: {}", "true");
                        return true;
                    }
                }
            }
            return false;
        } catch (IOException e) {
            LOG.error("Unable to verify if dir : {} is snapshot-able. {}", path.toString(), e.getMessage());
            throw new BeaconException("Unable to verify if dir " + path.toString() + " is snapshot-able", e);
        }
    }

    public static boolean isDirectorySnapshottable(FileSystem sourceFs, FileSystem targetFs,
                                                   String sourceStagingUri, String targetStagingUri)
            throws BeaconException {
        if (isHCFS(new Path(sourceStagingUri)) || isHCFS(new Path(targetStagingUri))) {
            return false;
        }
        try {
            if (sourceFs.exists(new Path(sourceStagingUri))) {
                if (!FSUtils.isSnapShotsAvailable((DistributedFileSystem) sourceFs, new Path(sourceStagingUri))) {
                    return false;
                }
            } else {
                throw new BeaconException(sourceStagingUri + " does not exist.");
            }

            if (targetFs.exists(new Path(targetStagingUri))) {
                if (!FSUtils.isSnapShotsAvailable((DistributedFileSystem) targetFs, new Path(targetStagingUri))) {
                    return false;
                }
            } else {
                throw new BeaconException(targetStagingUri + " does not exist.");
            }
        } catch (IOException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return true;
    }

    public static boolean createSnapshotInFileSystem(String dirName, String snapshotName,
                                                     FileSystem fs) throws BeaconException {
        boolean isSnapshot = false;
        try {
            LOG.info("Creating snapshot {} in directory {}", snapshotName, dirName);
            fs.createSnapshot(new Path(dirName), snapshotName);
            isSnapshot = true;
        } catch (IOException e) {
            LOG.error("Unable to create snapshot {} in filesystem {}. Exception is {}",
                    snapshotName, fs.getConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY), e.getMessage());
            throw new BeaconException("Unable to create snapshot " + snapshotName, e);
        } finally {
            return isSnapshot;
        }
    }

    public static void evictSnapshots(DistributedFileSystem fs, String dirName, String ageLimit,
                                      int numSnapshots) throws BeaconException {
        try {
            LOG.info("Started evicting snapshots on dir {} , agelimit {}, numSnapshot {}",
                    dirName, ageLimit, numSnapshots);

            long evictionTime = System.currentTimeMillis() - EvictionHelper.evalExpressionToMilliSeconds(ageLimit);

            dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
            String snapshotDir = dirName + Path.SEPARATOR + FSUtils.SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
            FileStatus[] snapshots = fs.listStatus(new Path(snapshotDir));
            if (snapshots.length <= numSnapshots) {
                LOG.info("No Eviction Required as number of snapshots : {} is less than "
                       + "numSnapshots: {}", snapshots.length, numSnapshots);
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
                    LOG.info("Deleting snapshots with path : {} and snapshot path: {}",
                            new Path(dirName), snapshots[i].getPath().getName());
                    fs.deleteSnapshot(new Path(dirName), snapshots[i].getPath().getName());
                }
            }

        } catch (ELException ele) {
            LOG.warn("Unable to parse retention age limit {} {}", ageLimit, ele.getMessage());
            throw new BeaconException("Unable to parse retention age limit " + ageLimit, ele);
        } catch (IOException ioe) {
            LOG.warn("Unable to evict snapshots from dir {} {}", dirName, ioe);
            throw new BeaconException("Unable to evict snapshots from dir " + dirName, ioe);
        }

    }

    public static String getSnapshotDir(String dirName) {
        dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
        return dirName + Path.SEPARATOR + SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
    }

    public static String getStagingUri(String dataset, String namenodeEndpoint) throws BeaconException {
        String stagingUri;
        if (isHCFS(new Path(dataset))) {
            // HCFS dataset has full path
            stagingUri = dataset;
        } else {
            try {
                URI pathUri = new URI(dataset.trim());
                String authority = pathUri.getAuthority();
                if (authority == null) {
                    stagingUri = new Path(namenodeEndpoint, dataset).toString();
                } else {
                    stagingUri = dataset;
                }
            } catch (URISyntaxException e) {
                throw new BeaconException(e);
            }
        }
        return stagingUri;
    }

}

