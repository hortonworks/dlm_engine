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

package com.hortonworks.beacon.util;

import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Filesystem handling utilites.
 */
public final class FSUtils {
    private FSUtils() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);
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
        if (filePath == null) {
            throw new BeaconException("filePath cannot be null or empty");
        }

        String scheme;
        try {
            URI uri = filePath.toUri();
            scheme = uri.getScheme();
            scheme = StringUtils.isBlank(scheme) ? FileSystem.get(uri, getDefaultConf()).getScheme() : scheme;
            if (StringUtils.isBlank(scheme)) {
                throw new BeaconException("Cannot get valid scheme for {}", filePath);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }

        return !HDFS_SCHEME_PREFIXES.contains(scheme.toLowerCase().trim());
    }


    public static FileSystem getFileSystem(String storageUrl, Configuration conf) throws BeaconException {
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, storageUrl);
        return FileSystemClientFactory.get().createFileSystem(conf);
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


    public static Configuration merge(Configuration source, Configuration overlay) {
        Iterator<Map.Entry<String, String>> iterator = overlay.iterator();
        while(iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            source.set(entry.getKey(), entry.getValue());
        }
        return source;
    }
}

