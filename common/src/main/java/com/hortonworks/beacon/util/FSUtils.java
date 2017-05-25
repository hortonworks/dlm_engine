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
import com.hortonworks.beacon.log.BeaconLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Filesystem handling utilites.
 */
public final class FSUtils {
    private FSUtils() {
    }

    private static final BeaconLog LOG = BeaconLog.getLog(FSUtils.class);
    private static final List<String> HDFS_SCHEME_PREFIXES =
            Arrays.asList("file", "hdfs", "hftp", "hsftp", "webhdfs", "swebhdfs");

    private static Configuration defaultConf = new Configuration();

    private static Configuration getDefaultConf() {
        return defaultConf;
    }

    public static void setDefaultConf(Configuration conf) {
        FSUtils.defaultConf = conf;
    }

    public static boolean isHCFS(Path filePath) throws BeaconException {
        LOG.info("Checking for HCFS Path : {}", filePath);

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

        return !HDFS_SCHEME_PREFIXES.contains(scheme.toLowerCase().trim());
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

    public static String getStagingUri(String namenodeEndpoint, String dataset) throws BeaconException {
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

