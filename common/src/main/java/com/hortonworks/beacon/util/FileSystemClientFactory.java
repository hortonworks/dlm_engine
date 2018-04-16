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

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A factory implementation to dole out FileSystem handles based on the logged in user.
 */
public final class FileSystemClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemClientFactory.class);
    private static final FileSystemClientFactory INSTANCE = new FileSystemClientFactory();
    private static FileSystem fileSystem;

    @VisibleForTesting
    public static void setFileSystem(FileSystem fileSystem) {
        FileSystemClientFactory.fileSystem = fileSystem;
    }

    private FileSystemClientFactory() {
    }

    public static FileSystemClientFactory get() {
        return INSTANCE;
    }

    /**
     * Return a FileSystem created with the authenticated proxy user for the specified conf.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided proxyUser/group.
     * @throws BeaconException
     *          if the filesystem could not be created.
     */
    public FileSystem createFileSystem(final Configuration conf) throws BeaconException {
        Validate.notNull(conf, "configuration cannot be null or empty");

        if (fileSystem != null) {   //used for testing
            return fileSystem;
        }
        try {
            return FileSystem.get(conf);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    public FileSystem createFileSystem(final String path,
                                              final Configuration conf) throws BeaconException {
        Validate.notNull(path, "path cannot be null or empty");

        if (fileSystem != null) {   //used for testing
            return fileSystem;
        }
        try {
            return new Path(path).getFileSystem(conf);
        } catch (IOException e) {
            throw new BeaconException("Exception while getting FileSystem", e);
        }
    }

    public static void mkdirs(FileSystem fs, Path path,
                              FsPermission permission) throws BeaconException {
        try {
            if (!FileSystem.mkdirs(fs, path, permission)) {
                throw new BeaconException("Directory creation failed for path {}", path);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }
}
