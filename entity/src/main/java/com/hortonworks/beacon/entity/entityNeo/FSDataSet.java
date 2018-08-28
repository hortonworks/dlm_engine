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

package com.hortonworks.beacon.entity.entityNeo;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static com.hortonworks.beacon.util.FSUtils.merge;

/**
 * Base class for HCFS compatible data set.
 */
public abstract class FSDataSet extends DataSet {

    private static final Logger LOG = LoggerFactory.getLogger(FSDataSet.class);

    protected String path;
    protected FileSystem fileSystem;
    private Configuration conf;


    protected FSDataSet(String path, ReplicationPolicy policy) throws BeaconException {
        this(path, null, policy);
    }

    protected FSDataSet(FileSystem fileSystem, String path) {
        this.path = path;
        this.fileSystem = fileSystem;
    }

    public String getResolvedPath() {
        return this.path;
    }

    protected abstract Configuration getHadoopConf(String path, ReplicationPolicy policy) throws BeaconException;


    private FSDataSet(String path, Configuration conf, ReplicationPolicy policy) throws BeaconException {
        this.path = resolvePath(path, policy);
        this.conf = conf != null ? conf : getHadoopConf(path, policy);
        Configuration defaultConf = new Configuration(true);
        merge(defaultConf, this.conf);
        fileSystem = FileSystemClientFactory.get().createFileSystem(this.path, defaultConf);
    }

    public FSDataSet(String path, Configuration conf) throws BeaconException {
        this(path, conf, null);
    }

    protected String resolvePath(String ipath, ReplicationPolicy policy) {
        return ipath;
    }

    public Configuration getHadoopConf() {
        return conf;
    }

    @Override
    public boolean exists() throws IOException {
        return fileSystem.exists(new Path(path));
    }

    @Override
    public void create() throws IOException {
        fileSystem.mkdirs(new Path(path));
    }

    @Override
    public boolean isEmpty() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(path), true);
        return !(files != null && files.hasNext());
    }

    @Override
    public void isWriteAllowed() throws ValidationException {
        String tmpFileName = ".Beacon_" + System.currentTimeMillis() + ".tmp";
        try {
            URI uri = new URI(path);
            Path bucketPath = new Path(uri.getScheme() + "://" + uri.getHost());
            Path tmpFilePath = new Path(bucketPath.toString() + Path.SEPARATOR + tmpFileName);
            FSDataOutputStream os = fileSystem.create(tmpFilePath);
            os.close();
            boolean tmpDeleted = fileSystem.delete(tmpFilePath, false);
            if (tmpDeleted) {
                LOG.debug("Deleted the temp file {} created during policy validation process", tmpFileName);
            } else {
                LOG.warn("Could not delete the temp file {} created during policy validation process", tmpFileName);
            }
        } catch (IOException ioEx) {
            throw new ValidationException(ioEx, ioEx.getCause().getMessage());
        } catch (URISyntaxException e) {
            throw new ValidationException(e, "URI from cloud path could not be obtained");
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    LOG.debug("IOException while closing fileSystem", e);
                }
            }
        }
        LOG.info("Validation for write access to cloud path {} succeeded.", path);
    }

    public static FSDataSet create(String path, String clusterName, ReplicationPolicy policy) throws BeaconException {
        if (clusterName != null) {
            return new HDFSDataSet(path, clusterName);
        }

        BeaconCloudCred cloudCred = new BeaconCloudCred(policy.getCloudCred());
        switch (cloudCred.getProvider()) {
            case AWS:
                return new S3FSDataSet(path, policy);

            case WASB:
                return new WASBFSDataSet(path, policy);

            default:
                break;
        }
        throw new IllegalStateException("Unhandled dataset " + path);
    }

    @VisibleForTesting
    public static FSDataSet create(FileSystem fileSystem, String path) throws BeaconException {
        if (fileSystem != null && path != null) {
            return new HDFSDataSet(fileSystem, path);
        }
        throw new IllegalStateException("Unhandled dataset " + path);
    }

    public String toString() {
        return path;
    }
}
