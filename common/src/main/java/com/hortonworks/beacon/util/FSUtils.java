/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.util;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;

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
        LOG.info(MessageCode.COMM_000037.name(), filePath);

        if (filePath == null) {
            throw new BeaconException(MessageCode.COMM_010008.name(), "filePath");
        }

        String scheme;
        try {
            FileSystem f = FileSystem.get(filePath.toUri(), getDefaultConf());
            scheme = f.getScheme();
            if (StringUtils.isBlank(scheme)) {
                throw new BeaconException(MessageCode.COMM_000013.name(), filePath);
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

