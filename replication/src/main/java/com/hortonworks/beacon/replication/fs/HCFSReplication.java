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

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.ExecutionType;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.CloudCredDao;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobContext;
import com.hortonworks.beacon.metrics.ReplicationMetrics;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.BeaconGlobbedCopyListing;
import org.apache.hadoop.tools.BeaconSimpleCopyListing;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DefaultFilter;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hortonworks.beacon.constants.BeaconConstants.SNAPSHOT_PREFIX;
import static com.hortonworks.beacon.util.FSUtils.merge;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_FILTERS_CLASS;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS;


/**
 * HCFS FileSystem Replication implementation.
 */
public class HCFSReplication extends FSReplication {

    private static final Logger LOG = LoggerFactory.getLogger(HCFSReplication.class);
    private boolean isPushRepl;

    public HCFSReplication(ReplicationJobDetails details) {
        super(details);
    }

    @Override
    public void perform(JobContext jobContext) throws BeaconException, InterruptedException {
        performCopy(jobContext, ReplicationMetrics.JobType.MAIN);
        if (properties.containsKey(BeaconConstants.META_LOCATION)) {
            String metaLocation = properties.getProperty(BeaconConstants.META_LOCATION);
            Path metaLocationPath = new Path(metaLocation);
            try {
                if (!sourceFs.exists(metaLocationPath)) {
                    sourceFs.create(metaLocationPath);
                }
            } catch (IOException e) {
                throw new BeaconException("Unable to create meta directory : {}", metaLocation, e);
            }
            performPreserveMeta(properties.getProperty(BeaconConstants.META_LOCATION));
        }
        performPostReplJobExecution(jobContext, job, ReplicationMetrics.JobType.MAIN);
    }


    private void performPreserveMeta(String metaLocation) throws BeaconException {
        try {
            Path metaPath = new Path(metaLocation, "fileList.seq");
            String sortedChildPath = ".dlm-engine/fileList.seq_sorted";
            FileSystem cloudtargetFs = new Path(targetStagingUri).getFileSystem(getHCFSConfiguration());
            ExecutionType executionType = ExecutionType.valueOf(properties.getProperty(FSDRProperties.EXECUTION_TYPE
                    .getName()));
            Path cloudMetaPath = new Path(targetStagingUri, sortedChildPath);
            if (executionType == ExecutionType.FS_HCFS_SNAPSHOT) {
                LOG.debug("Source Path: {} Target Path: {}", metaPath.toString(), cloudMetaPath.toString());
                Configuration conf = getConfiguration();
                Path sortedSourceListing = DistCpUtils.sortListing(sourceFs, conf, metaPath);
                if (cloudtargetFs.exists(cloudMetaPath)) {
                    mergeMeta(sourceFs, sortedSourceListing, cloudMetaPath);
                }
                try {
                    boolean deleteSuccessful = sourceFs.delete(metaPath, false);
                    if (!deleteSuccessful) {
                        LOG.error("Unable to delete :{}", metaPath);
                    }
                } catch (IOException e) {
                    LOG.error("Unable to delete :{}", metaPath);
                }
            }
            createMeta(metaLocation);
            Path[] metaFilePath = listFiles(metaLocation);
            copyMeta(cloudtargetFs, metaFilePath);
        } catch (IOException e) {
            throw new BeaconException("Error while preserving the meta information", e);
        }
    }

    private void copyMeta(FileSystem cloudtargetFs, Path[] metaFilePath) throws BeaconException {
        Path targetPath = new Path(targetStagingUri, ".dlm-engine");
        LOG.debug("Copying meta files from [{}] to {}.", Arrays.toString(metaFilePath), targetPath.toString());
        try {
            if (!cloudtargetFs.exists(targetPath)) {
                cloudtargetFs.mkdirs(targetPath);
            }
            boolean copySuccessful = FileUtil.copy(sourceFs, metaFilePath, cloudtargetFs, targetPath,
                    true, true, getHCFSConfiguration());
            if (!copySuccessful) {
                throw new BeaconException("Unable to move meta directory to {}.", targetPath.toString());
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        LOG.info("Meta directory copy successful to {}", targetPath.toString());
    }

    private void debugMetaInfo(Path metaFilePath) {
        LOG.debug("Logging Meta info preserved at {}", metaFilePath.toString());
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(getConfiguration(),
                    SequenceFile.Reader.file(metaFilePath));
            CopyListingFileStatus fileStatus = new CopyListingFileStatus();
            Text relPath = new Text();
            while (reader.next(relPath, fileStatus)) {
                LOG.info("Path: {}, File Status: {}", relPath, fileStatus);
            }
        } catch (IOException e) {
            LOG.error("Error while reading the meta file: {}", metaFilePath.toString(), e);
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    private Path[] listFiles(String path) throws BeaconException {
        FileStatus[] fileStatuses;
        try {
            fileStatuses = sourceFs.listStatus(new Path(path));
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        Path[] filePaths = new Path[fileStatuses.length];
        for (int i=0; i<fileStatuses.length; i++) {
            filePaths[i] = fileStatuses[i].getPath();
        }
        return filePaths;
    }

    private void createMeta(String metaLocation) throws BeaconException {
        LOG.debug("Creating source dataset meta information.");
        SequenceFile.Writer writer = null;
        Path sourceInfoPath = new Path(metaLocation, "sourceInfo.seq");
        try {
            writer = getWriter(sourceInfoPath, getConfiguration());
            CopyListingFileStatus fileStatus = DistCpUtils.toCopyListingFileStatus(sourceFs, sourceFs.getFileStatus(
                    new Path(sourceStagingUri)), true, true, true);
            writer.append(new Text(Path.SEPARATOR), fileStatus);
        } catch (IOException e) {
            throw new BeaconException(e);
        } finally {
            IOUtils.closeStream(writer);
            if (LOG.isDebugEnabled()) {
                debugMetaInfo(sourceInfoPath);
            }
        }
    }

    private void mergeMeta(FileSystem sourceFs, Path sortedSourceListing, Path cloudMetaPath) throws BeaconException {
        SequenceFile.Reader existingReader = null;
        SequenceFile.Reader modifiedReader = null;
        SequenceFile.Writer writer = null;
        try {
            Path path = new Path(sortedSourceListing.getParent(), "final.seq");
            LOG.debug("Final output path: {}", path.toString());
            existingReader = new SequenceFile.Reader(getHCFSConfiguration(),
                    SequenceFile.Reader.file(cloudMetaPath));
            modifiedReader = new SequenceFile.Reader(getConfiguration(),
                    SequenceFile.Reader.file(sortedSourceListing));
            CopyListingFileStatus existingFileStatus = new CopyListingFileStatus();
            Text existingRelPath = new Text();
            CopyListingFileStatus modifiedFileStatus = new CopyListingFileStatus();
            Text modifiedRelPath = new Text();
            writer = getWriter(path, getConfiguration());

            boolean modifiedAvailable = modifiedReader.next(modifiedRelPath, modifiedFileStatus);
            boolean existingAvailable = existingReader.next(existingRelPath, existingFileStatus);
            while (existingAvailable) {
                // Replace the one which got modified in the current replication.
                if (modifiedAvailable && modifiedRelPath.compareTo(existingRelPath) <= 0) {
                    LOG.debug("Path modified: {}, Meta: {}", modifiedRelPath.toString(), modifiedFileStatus.toString());
                    writer.append(modifiedRelPath, modifiedFileStatus);
                    modifiedAvailable = modifiedReader.next(modifiedRelPath, modifiedFileStatus);
                } else {
                    LOG.debug("Path not modified: {}, Meta: {}", existingRelPath.toString(),
                            existingFileStatus.toString());
                    writer.append(existingRelPath, existingFileStatus);
                    existingAvailable = existingReader.next(existingRelPath, existingFileStatus);
                }
            }
            while (modifiedAvailable) {
                LOG.debug("Path modified: {}, Meta: {}", modifiedRelPath.toString(), modifiedFileStatus.toString());
                writer.append(modifiedRelPath, modifiedFileStatus);
                modifiedAvailable = modifiedReader.next(modifiedRelPath, modifiedFileStatus);
            }
            writer.close();
            boolean copySuccessful = FileUtil.copy(sourceFs, path, sourceFs, sortedSourceListing, true,
                    getConfiguration());
            if (!copySuccessful) {
                throw new BeaconException("Unable to move meta file {} to {}.", path.toString(), sortedSourceListing
                        .toString());
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        } finally {
            IOUtils.closeStream(existingReader);
            IOUtils.closeStream(modifiedReader);
            IOUtils.cleanupWithLogger(LOG, writer);
        }
    }

    private Job performCopy(JobContext jobContext, ReplicationMetrics.JobType jobType)
            throws BeaconException, InterruptedException {
        try {
            String toSnapshot = null;
            String fromSnapshot = null;
            if (isSnapshot && isPushRepl) {
                toSnapshot = FSSnapshotUtils.TEMP_REPLICATION_SNAPSHOT;
                String snapshotPrefix = SNAPSHOT_PREFIX + getDetails().getName();
                fromSnapshot = FSSnapshotUtils.getLatestSnapshot(sourceFs, sourceStagingUri, snapshotPrefix);
                FSSnapshotUtils.checkAndCreateSnapshot(sourceFs, sourceStagingUri, toSnapshot);
            }
            DistCpOptions options = getDistCpOptions(fromSnapshot, toSnapshot);
            Configuration conf = getHCFSConfiguration();
            performCopy(jobContext, options, conf, jobType);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
        return job;
    }

    private Configuration getHCFSConfiguration() throws BeaconException {
        Configuration conf = getConfiguration();
        String cloudCredId = properties.getProperty(FSDRProperties.CLOUD_CRED.getName());
        BeaconCloudCred cloudCred = new BeaconCloudCred(new CloudCredDao().getCloudCred(cloudCredId));
        Configuration cloudConf = cloudCred.getHadoopConf(false);
        String cloudPath = getCloudPath();
        merge(cloudConf, cloudCred.getCloudEncryptionTypeConf(properties, cloudPath));
        merge(cloudConf, getConfigurationWithBucketEndPoint(cloudCred));
        return merge(conf, cloudConf);
    }

    private Configuration getConfigurationWithBucketEndPoint(BeaconCloudCred cloudCred) throws BeaconException {
        return cloudCred.getBucketEndpointConf(getCloudPath());
    }

    private String getCloudPath() throws BeaconException {
        String cloudPath;
        String sourcePath = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        if (PolicyHelper.isDatasetHCFS(sourcePath)) {
            cloudPath = sourcePath;
        } else {
            cloudPath = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
        }
        return cloudPath;
    }

    private Configuration getConfiguration() {
        Configuration conf = getHAConfigOrDefault();
        String queueName = properties.getProperty(FSDRProperties.QUEUE_NAME.getName());
        if (queueName != null) {
            conf.set(BeaconConstants.MAPRED_QUEUE_NAME, queueName);
        }

        conf.set(CONF_LABEL_FILTERS_CLASS, DefaultFilter.class.getName());
        conf.setInt(CONF_LABEL_LISTSTATUS_THREADS, 20);
        conf.set(DistCpConstants.DISTCP_EXCLUDE_FILE_REGEX, BeaconConfig.getInstance()
                .getEngine().getExcludeFileRegex());
        if (properties.containsKey(BeaconConstants.META_LOCATION)) {
            conf.set(BeaconConstants.META_LOCATION, properties.getProperty(BeaconConstants.META_LOCATION));
            conf.set(DistCpConstants.CONF_LABEL_COPY_LISTING_CLASS, BeaconGlobbedCopyListing.class.getName());
            conf.set(DistCpConstants.CONF_LABEL_SIMPLE_COPY_LISTING_CLASS, BeaconSimpleCopyListing.class.getName());
        }

        return conf;
    }

    private Configuration getHAConfigOrDefault() {
        Configuration conf = new Configuration();
        if (properties.containsKey(BeaconConstants.HA_CONFIG_KEYS)) {
            String haConfigKeys = properties.getProperty(BeaconConstants.HA_CONFIG_KEYS);
            for(String haConfigKey: haConfigKeys.split(BeaconConstants.COMMA_SEPARATOR)) {
                conf.set(haConfigKey, properties.getProperty(haConfigKey));
            }
        }
        return conf;
    }

    protected void initializeProperties() throws BeaconException {
        String sourceDS = properties.getProperty(FSDRProperties.SOURCE_DATASET.getName());
        String targetDS = properties.getProperty(FSDRProperties.TARGET_DATASET.getName());
        String sourceCN = properties.getProperty(FSDRProperties.SOURCE_CLUSTER_NAME.getName());
        String targetCN = properties.getProperty(FSDRProperties.TARGET_CLUSTER_NAME.getName());

        properties.setProperty(FSDRProperties.SOURCE_DATASET.getName(), sourceDS);
        properties.setProperty(FSDRProperties.TARGET_DATASET.getName(), targetDS);

        if (FSUtils.isHCFS(new Path(sourceDS))) {
            Cluster targetCluster = ClusterHelper.getActiveCluster(targetCN);
            properties.setProperty(FSDRProperties.TARGET_NN.getName(), targetCluster.getFsEndpoint());
            isPushRepl = false;
        } else {
            Cluster sourceCluster = ClusterHelper.getActiveCluster(sourceCN);
            properties.setProperty(FSDRProperties.SOURCE_NN.getName(), sourceCluster.getFsEndpoint());
            isPushRepl = true;
            boolean isTDEon = Boolean.valueOf(properties.getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName()));
            if (!isTDEon) {
                isSnapshot = SnapshotListing.get().isSnapshottable(sourceCN, sourceCluster.getFsEndpoint(), sourceDS);
            }
        }
    }

    @Override
    protected void initializeFileSystem() throws BeaconException {
        if (isPushRepl) {
            String sourceClusterName = properties.getProperty(FSDRProperties.SOURCE_CLUSTER_NAME.getName());
            Configuration sourceConf = ClusterHelper.getHAConfigurationOrDefault(sourceClusterName);
            sourceFs = FSUtils.getFileSystem(properties.getProperty(FSDRProperties.SOURCE_NN.getName()), sourceConf);
        } else {
            String targetClusterName = properties.getProperty(FSDRProperties.TARGET_CLUSTER_NAME.getName());
            Configuration targetConf = ClusterHelper.getHAConfigurationOrDefault(targetClusterName);
            targetFs = FSUtils.getFileSystem(properties.getProperty(FSDRProperties.TARGET_NN.getName()), targetConf);
        }
    }

    private DistCpOptions getDistCpOptions(String fromSnapshot, String toSnapshot) throws BeaconException, IOException {
        // DistCpOptions expects the first argument to be a file OR a list of Paths

        List<Path> sourceUris = new ArrayList<>();
        Path targetPath;
        sourceUris.add(new Path(sourceStagingUri));
        targetPath = new Path(targetStagingUri);

        return DistCpOptionsUtil.getHCFSDistCpOptions(properties, sourceUris, targetPath,
                isSnapshot, fromSnapshot, toSnapshot);
    }

    private void performPostReplJobExecution(JobContext jobContext, Job job,
                                             ReplicationMetrics.JobType jobType) throws BeaconException {
        try {
            if (job.isComplete() && job.isSuccessful()) {
                if (isSnapshot && isPushRepl) {
                    FSSnapshotUtils.checkAndRenameSnapshot(sourceFs, sourceStagingUri,
                            FSSnapshotUtils.TEMP_REPLICATION_SNAPSHOT,
                            FSSnapshotUtils.getSnapshotName(getDetails().getName()));
                    FSSnapshotUtils.handleSnapshotEviction(sourceFs, properties, sourceStagingUri);
                }
                LOG.info("HCFS Distcp copy is successful.");
            }
        } catch (Exception e) {
            throw new BeaconException(e);
        }
    }



    @Override
    public void recover(JobContext jobContext) {
        jobContext.setPerformJobAfterRecovery(true);
    }

    private static SequenceFile.Writer getWriter(Path pathToListFile, Configuration conf) throws IOException {
        FileSystem fs = pathToListFile.getFileSystem(conf);
        if (fs.exists(pathToListFile)) {
            fs.delete(pathToListFile, false);
        }
        return SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(pathToListFile),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(CopyListingFileStatus.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
    }
}
