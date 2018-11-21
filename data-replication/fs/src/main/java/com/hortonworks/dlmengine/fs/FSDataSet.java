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

package com.hortonworks.dlmengine.fs;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import com.hortonworks.dlmengine.DataSet;
import com.hortonworks.dlmengine.fs.gcs.GCSFSDataSet;
import com.hortonworks.dlmengine.fs.hdfs.HDFSDataSet;
import com.hortonworks.dlmengine.fs.s3.S3FSDataSet;
import com.hortonworks.dlmengine.fs.wasb.WASBFSDataSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.hortonworks.beacon.util.FSUtils.merge;

/**
 * Base class for HCFS compatible data set.
 */
public abstract class FSDataSet extends DataSet {

    private static final Logger LOG = LoggerFactory.getLogger(FSDataSet.class);

    protected Path path;
    protected FileSystem fileSystem;
    private Configuration conf;

    protected FSDataSet(String path, ReplicationPolicy policy) throws BeaconException {
        this(path, null, policy);
    }

    // This is used by hdfsDataSet only
    protected FSDataSet(FileSystem fileSystem, String path) {
        super(path);
        this.path = new Path(resolvePath(path, null));
        this.fileSystem = fileSystem;
    }

    private FSDataSet(String path, Configuration conf, ReplicationPolicy policy) throws BeaconException {
        super(path);
        this.path = new Path(resolvePath(path, policy));
        this.conf = conf != null ? conf : getHadoopConf(path, policy);
        Configuration defaultConf = new Configuration(true);
        merge(defaultConf, this.conf);
        fileSystem = FileSystemClientFactory.get().createFileSystem(resolvePath(path, policy), defaultConf);
    }

    public FSDataSet(String path, Configuration conf) throws BeaconException {
        this(path, conf, null);
    }


    public abstract String resolvePath(String path, ReplicationPolicy policy);

    protected abstract Configuration getHadoopConf(String path, ReplicationPolicy policy) throws BeaconException;

    @Override
    public Configuration getHadoopConf() throws BeaconException {
        return conf;
    }

    @Override
    public void create(FileStatus fileStatus) throws BeaconException {
        try {
            //TODO check if this works with S3
            LOG.info("Creating directory {} with permissions {}, owner {}, group {}", path, fileStatus.getPermission(),
                    fileStatus.getOwner(), fileStatus.getGroup());
            boolean created = fileSystem.mkdirs(path, fileStatus.getPermission());
            fileSystem.setOwner(path, fileStatus.getOwner(), fileStatus.getGroup());
            if (!created) {
                throw new BeaconException("Failed buildReplicationPolicy directory " + path);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public boolean exists() throws BeaconException {
        try {
            return fileSystem.exists(path);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public FileStatus getFileStatus() throws BeaconException {
        try {
            return fileSystem.getFileStatus(path);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    @Override
    public boolean isEmpty() throws BeaconException {
        try {
            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
            return !(files != null && files.hasNext());
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    public static FSDataSet create(String path, BeaconCloudCred cloudCred) throws BeaconException {
        return create(path, null, cloudCred, null);
    }

    public static FSDataSet create(String path, String clusterName) throws BeaconException {
        return create(path, clusterName, null, null);
    }

    public static FSDataSet create(String path, ReplicationPolicy policy) throws BeaconException {
        return create(path, null, null, policy);
    }

    public static FSDataSet create(String path, BeaconCloudCred cloudCred, ReplicationPolicy policy)
            throws BeaconException {
        return create(path, null, cloudCred, policy);
    }

    public static FSDataSet create(String path, String clusterName, ReplicationPolicy policy)
            throws BeaconException {
        return create(path, clusterName, null, policy);
    }

    public static FSDataSet create(String path, String clusterName, BeaconCloudCred cloudCred,
                                   ReplicationPolicy policy) throws BeaconException {

        CloudCred.Provider provider = null;
        String scheme = new Path(path).toUri().getScheme();
        FSDataSet dataset = null;

        if (scheme != null) {
            provider = CloudCred.Provider.createFromScheme(scheme);
            dataset = getDataset(provider, path, policy, cloudCred);
        }

        if (dataset == null && clusterName != null) {
            dataset = new HDFSDataSet(path, clusterName);
        }

        if (dataset == null && policy.getCloudCred() != null) {
            dataset = getDataset(cloudCred.getProvider(), path, policy, cloudCred);
        }
        return dataset;
    }

    private static FSDataSet getDataset(CloudCred.Provider provider, String path,
                                        ReplicationPolicy policy, BeaconCloudCred cloudCred)
            throws BeaconException {
        if (provider == null) {
            return null;
        }

        switch (provider) {
            case AWS:
                return new S3FSDataSet(path, cloudCred, policy);

            case WASB:
                return new WASBFSDataSet(path, cloudCred, policy);

            case GCS:
                return new GCSFSDataSet(path, cloudCred, policy);

            default:
                break;
        }
        return null;
    }

    public String toString() {
        return path.toString();
    }

    @Override
    public boolean conflicts(String otherPath) {
        if (name.equals("/") || otherPath.equals("/")) {
            return true;
        }

        String np1 = new Path(name).toString() + "/";    //name points to path in the policy
        String np2 = new Path(otherPath).toString() + "/";
        return np1.startsWith(np2) || np2.startsWith(np1);
    }

    public Path getPath() {
        return path;
    }

    public RemoteIterator<FileStatus> listStatusIterator() throws BeaconException {
        try {
            return fileSystem.listStatusIterator(path);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    public abstract boolean isEncrypted(Path path) throws BeaconException;

    @VisibleForTesting
    public static FSDataSet create(FileSystem fileSystem, String path, String clusterName) throws BeaconException {
        if (fileSystem != null && path != null) {
            return new HDFSDataSet(fileSystem, path, clusterName);
        }
        throw new IllegalStateException("Unhandled dataset " + path);
    }
    @Override
    public boolean isEncrypted() throws BeaconException {
        return isEncrypted(path);
    }
}
