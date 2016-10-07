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

package com.hortonworks.beacon.scheduler.hdfssnapshot;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by pbishnoi on 10/7/16.
 */
public class HDFSSnapshotUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSSnapshotDRImpl.class);

    public static final String SNAPSHOT_PREFIX = "beacon-snapshot-";
    public static final String SNAPSHOT_DIR_PREFIX = ".snapshot";

    private HDFSSnapshotUtil() {
    }

    public static DistributedFileSystem getSourceFileSystem(HDFSSnapshotReplicationJobDetails details,
                                                            Configuration conf) throws BeaconException {
        String sourceStorageUrl = details.getSourceNN();
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, sourceStorageUrl);
        return FileSystemClientFactory.get().createDistributedProxiedFileSystem(conf);
    }

    public static DistributedFileSystem getTargetFileSystem(HDFSSnapshotReplicationJobDetails details,
                                                            Configuration conf) throws BeaconException {
        String targetStorageUrl = details.getTargetNN();
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, targetStorageUrl);
        return FileSystemClientFactory.get().createDistributedProxiedFileSystem(conf);
    }

    public static boolean isDirSnapshotable(DistributedFileSystem hdfs, Path path) throws BeaconException {
        try {
            LOG.info("Validating if dir : {} is snapshotable.", path.toString());
            SnapshottableDirectoryStatus[] snapshotableDirs = hdfs.getSnapshottableDirListing();
            if (snapshotableDirs != null && snapshotableDirs.length > 0) {
                for (SnapshottableDirectoryStatus dir : snapshotableDirs) {
                    if (dir.getFullPath().toString().equals(path.toString())) {
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
}
