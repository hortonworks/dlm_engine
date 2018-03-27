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


package com.hortonworks.beacon.api.util;

import com.hortonworks.beacon.Destination;
import com.hortonworks.beacon.EncryptionAlgorithmType;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.api.EncryptionZoneListing;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.CloudCredDao;
import com.hortonworks.beacon.entity.util.ClusterDao;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClientFactory;
import com.hortonworks.beacon.notification.BeaconNotification;
import com.hortonworks.beacon.replication.ReplicationUtils;
import com.hortonworks.beacon.replication.fs.FSPolicyHelper;
import com.hortonworks.beacon.replication.fs.FSSnapshotUtils;
import com.hortonworks.beacon.replication.fs.SnapshotListing;
import com.hortonworks.beacon.replication.hive.HivePolicyHelper;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.hortonworks.beacon.util.FSUtils.merge;


/**
 * Utility class to validate API requests.
 */
public final class ValidationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationUtil.class);
    private static ClusterDao clusterDao = new ClusterDao();
    private static CloudCredDao cloudCredDao = new CloudCredDao();


    private ValidationUtil() {
    }

    public static void validateClusterPairing(Cluster localCluster, Cluster remoteCluster) {
        Properties localCustomProperties = localCluster.getCustomProperties();
        Properties remoteCustomProperties = remoteCluster.getCustomProperties();
        if (ClusterHelper.isHDFSEnabled(localCustomProperties)
                != ClusterHelper.isHDFSEnabled(remoteCustomProperties)) {
            LOG.error("HDFS is not enabled in either {} or {} cluster", localCluster.getName(),
                    remoteCluster.getName());
        }

        if ((ClusterHelper.isHDFSEnabled(localCustomProperties) && ClusterHelper.isHighlyAvailableHDFS(
                localCustomProperties))
                != (ClusterHelper.isHDFSEnabled(remoteCustomProperties) && ClusterHelper.isHighlyAvailableHDFS(
                        remoteCustomProperties))) {
            LOG.error("NameNode HA is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
        if (ClusterHelper.isHiveEnabled(localCluster)
                && !(Boolean.valueOf(remoteCluster.getCustomProperties().getProperty(
                    Cluster.ClusterFields.CLOUDDATALAKE.getName())))
                && (ClusterHelper.isHighlyAvailableHive(localCluster.getHsEndpoint())
                != ClusterHelper.isHighlyAvailableHive(remoteCluster.getHsEndpoint()))) {
            LOG.error("Hive HA is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
        if (ClusterHelper.isRangerEnabled(localCluster.getRangerEndpoint())
                != ClusterHelper.isRangerEnabled(remoteCluster.getRangerEndpoint())) {
            LOG.error("Ranger is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
        if (ClusterHelper.isHiveEnabled(localCluster) != ClusterHelper.isHiveEnabled(remoteCluster)) {
            LOG.error("Hive is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
        if (ClusterHelper.isKerberized(localCluster) != ClusterHelper.isKerberized(remoteCluster)) {
            LOG.error("Kerberos is not enabled in either {} or {} cluster", localCluster.getName(),
                remoteCluster.getName());
        }
    }

    public static void validationOnSubmission(ReplicationPolicy replicationPolicy,
                                              boolean validateCloudCred) throws BeaconException {
        validateIfAPIRequestAllowed(replicationPolicy);
        validatePolicy(replicationPolicy, validateCloudCred);
        validateEntityDataset(replicationPolicy);
    }

    public static boolean validatePolicyCloudPath(ReplicationPolicy replicationPolicy,
                                                      String path) {
        CloudCred cloudCred = getCloudCred(replicationPolicy);
        boolean cloudPathExists = validateCloudPath(cloudCred, path);
        LOG.info("Cloud credentials validation is successful.");
        return cloudPathExists;
    }

    private static CloudCred getCloudCred(ReplicationPolicy replicationPolicy) {
        Properties properties = replicationPolicy.getCustomProperties();
        String cloudCredId = properties.getProperty(ReplicationPolicy.ReplicationPolicyFields.CLOUDCRED.getName());
        if (cloudCredId == null) {
            throw new IllegalArgumentException("Cloud cred id is missing.");
        }

        return cloudCredDao.getCloudCred(cloudCredId);
    }

    public static boolean validateCloudPath(CloudCred cloudCred, String pathStr) {
        FileSystem fileSystem = null;
        try {
            String cloudPath = prepareCloudPath(pathStr, cloudCred.getProvider());
            Path path = new Path(cloudPath);
            Configuration conf = new BeaconCloudCred(cloudCred).getHadoopConf();
            Configuration confWithS3EndPoint = new BeaconCloudCred(cloudCred).getBucketEndpointConf(cloudPath);
            merge(conf, confWithS3EndPoint);
            fileSystem = path.getFileSystem(conf);
            return fileSystem.exists(path);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (URISyntaxException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (AccessDeniedException e) {
            throw BeaconWebException.newAPIException(Response.Status.BAD_REQUEST, e,
                    "Invalid credentials");
        } catch(IOException | BeaconException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    LOG.error(StringFormat.format("Exception while closing file system. {}", e.getMessage()), e);
                }
            }
        }
    }

    public static String prepareCloudPath(String path, CloudCred.Provider provider) throws URISyntaxException {
        URI uri = new URI(path);
        String scheme = uri.getScheme();
        if (StringUtils.isBlank(scheme)) {
            path = provider.getScheme().concat("://").concat(path);
        } else {
            path = path.replaceFirst(scheme, provider.getScheme());
        }
        return path;
    }

    public static void validateIfAPIRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        if (policy == null) {
            throw new BeaconException("ReplicationPolicy cannot be null or empty");
        }

        isRequestAllowed(policy);
    }

    private static void isRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        if (policy.getType().equalsIgnoreCase(ReplicationType.FS.getName())) {
            isFSRequestAllowed(policy);
        } else if (policy.getType().equalsIgnoreCase(ReplicationType.HIVE.getName())) {
            isHiveRequestAllowed(policy);
        }
    }

    private static void isFSRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = ClusterHelper.getLocalCluster().getName();

        if (localClusterName.equalsIgnoreCase(sourceClusterName)
                && !PolicyHelper.isPolicyHCFS(policy.getSourceDataset(), policy.getTargetDataset())) {
            throw BeaconWebException.newAPIException(
                    "This operation is not allowed on source cluster: {}. Try it on target cluster: {}",
                    sourceClusterName, targetClusterName);
        }
    }

    private static void isHiveRequestAllowed(ReplicationPolicy policy) throws BeaconException {
        String sourceClusterName = policy.getSourceCluster();
        String targetClusterName = policy.getTargetCluster();
        String localClusterName = ClusterHelper.getLocalCluster().getName();

        if (!isDataLake(policy) && localClusterName.equalsIgnoreCase(sourceClusterName)) {
            throw BeaconWebException.newAPIException(
                    "This operation is not allowed on source cluster: {}. Try it on target cluster: {}",
                    sourceClusterName, targetClusterName);
        }

        if (isDataLake(policy) && localClusterName.equalsIgnoreCase(targetClusterName)) {
            throw BeaconWebException.newAPIException(
                    "This operation is not allowed on target cluster: {}. Try it on source cluster: {}",
                    targetClusterName, sourceClusterName);
        }
    }

    private static boolean isDataLake(ReplicationPolicy policy) throws BeaconException {
        if (policy.getType().equalsIgnoreCase(ReplicationType.HIVE.getName())) {
            Cluster tgtCluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
            return Boolean.valueOf(tgtCluster.getCustomProperties()
                    .getProperty(Cluster.ClusterFields.CLOUDDATALAKE.getName()));
        }
        return false;
    }

    private static void validatePolicy(final ReplicationPolicy policy,
                                       boolean validateCloudCred) throws BeaconException {
        updateSnapshotCache(policy);
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                FSPolicyHelper.validateFSReplicationProperties(FSPolicyHelper.buildFSReplicationProperties(policy));
                validateFSSourceDS(policy);
                validateFSTargetDS(policy);
                String cloudPath = null;
                if (PolicyHelper.isDatasetHCFS(policy.getSourceDataset())) {
                    cloudPath = policy.getSourceDataset();
                } else if (PolicyHelper.isDatasetHCFS(policy.getTargetDataset())) {
                    cloudPath = policy.getTargetDataset();
                }
                if (validateCloudCred && cloudPath != null) {
                    boolean cloudPathExists = validatePolicyCloudPath(policy, cloudPath);
                    if (!cloudPathExists && FSUtils.isHCFS(new Path(policy.getSourceDataset()))) {
                        throw BeaconWebException.newAPIException(Response.Status.NOT_FOUND,
                                "sourceDataset does not exist");
                    }
                }
                if (cloudPath != null) {
                    validateEncryptionAlgorithmType(policy);
                    ensureCloudEncryptionAndClusterTDECompatibility(policy);
                }
                break;
            case HIVE:
                HivePolicyHelper.validateHiveReplicationProperties(
                        HivePolicyHelper.buildHiveReplicationProperties(policy));
                validateDBSourceDS(policy);
                validateDBTargetDS(policy);
                break;
            default:
                throw new IllegalArgumentException("Invalid policy type: " + policy.getType());
        }
    }

    private static void ensureCloudEncryptionAndClusterTDECompatibility(ReplicationPolicy policy)
                                                                                               throws BeaconException {
        Cluster cluster;
        String clusterDataSet;
        if (StringUtils.isNotBlank(policy.getSourceCluster())){
            cluster  = ClusterHelper.getActiveCluster(policy.getSourceCluster());
            clusterDataSet = policy.getSourceDataset();
        } else {
            cluster  = ClusterHelper.getActiveCluster(policy.getTargetCluster());
            clusterDataSet = policy.getTargetDataset();
        }
        boolean tdeOn = isTDEEnabled(cluster, clusterDataSet);
        boolean encOn = !EncryptionAlgorithmType.NONE.equals(PolicyHelper.getCloudEncryptionAlgorithm(policy));
        if (tdeOn ^ encOn) {
            if (tdeOn && clusterDataSet.equals(policy.getSourceDataset())){
                throw new BeaconException("Source data set is TDE enabled but target is not encryption enabled");
            }
            if (encOn && clusterDataSet.equals(policy.getTargetDataset())){
                throw new BeaconException("Source data set is encrypted but target cluster is not TDE enabled");
            }
        }
    }

    private static void ensureClusterTDECompatibilityForHive(ReplicationPolicy policy)
            throws BeaconException {
        Cluster cluster  = ClusterHelper.getActiveCluster(policy.getSourceCluster());
        boolean tdeOn = isTDEEnabled(cluster, cluster.getHiveWarehouseLocation());
        boolean encOn = !EncryptionAlgorithmType.NONE.equals(PolicyHelper.getCloudEncryptionAlgorithm(policy));
        if (tdeOn ^ encOn) {
            if (tdeOn) {
                throw new BeaconException("Source data set is TDE enabled but target is not encryption enabled");
            }
        }
    }

    private static void validateEncryptionAlgorithmType(ReplicationPolicy policy) throws ValidationException {
        String encryptionAlgorithm = policy.getCloudEncryptionAlgorithm();
        if (StringUtils.isEmpty(encryptionAlgorithm)) {
            if (StringUtils.isNotEmpty(policy.getCloudEncryptionKey())) {
                throw new ValidationException("Cloud Encryption key without a cloud algorithm not allowed");
            }
            return;
        }
        try {
            EncryptionAlgorithmType encryptionAlgorithmType = EncryptionAlgorithmType.valueOf(encryptionAlgorithm);
            switch (encryptionAlgorithmType) {
                case AWS_SSEKMS:
                    if (StringUtils.isEmpty(policy.getCloudEncryptionKey())) {
                        throw new ValidationException("Cloud Encryption key is mandatory with AWS_SSEKMS algorithm");
                    }
                    break;
                default:
                    if (StringUtils.isNotEmpty(policy.getCloudEncryptionKey())) {
                        throw new ValidationException("Cloud encryption key is not applicable to algorithm {}",
                                encryptionAlgorithm);
                    }
                    break;
            }
        } catch (IllegalArgumentException iaEx) {
            throw new ValidationException("Cloud encryption algorithm {} is not supported", encryptionAlgorithm, iaEx);
        }
    }

    private static void validateFSSourceDS(ReplicationPolicy policy) throws BeaconException {
        FileSystem fileSystem;
        String sourceDataset = policy.getSourceDataset();
        if (PolicyHelper.isDatasetHCFS(sourceDataset)) {
            fileSystem = getCloudFileSystem(policy, Destination.SOURCE);
        } else {
            fileSystem = getFileSystem(policy.getSourceCluster());
        }

        try {
            if (!fileSystem.exists(new Path(sourceDataset))) {
                throw new ValidationException("Dataset {} doesn't exists in {} cluster", policy.getSourceDataset(),
                        policy.getSourceCluster());
            }
            if (PolicyHelper.isPolicyHCFS(policy)) {
                return;
            }
            String clusterName = policy.getSourceCluster();
            Cluster cluster = clusterDao.getActiveCluster(clusterName);

            boolean tdeEnabled = isTDEEnabled(cluster, sourceDataset);
            boolean markSourceSnapshottable = Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties
                                     .SOURCE_SETSNAPSHOTTABLE.getName()));
            boolean snapshottable = FSSnapshotUtils.checkSnapshottableDirectory(clusterName, FSUtils.getStagingUri(
                    cluster.getFsEndpoint(), sourceDataset));
            if (tdeEnabled && (snapshottable || markSourceSnapshottable)) {
                throw new ValidationException("TDE enabled zone can't be used for snapshot based replication.");
            }
            if (tdeEnabled) {
                policy.getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
            }
            if (markSourceSnapshottable) {
                FSSnapshotUtils.allowSnapshot(ClusterHelper.getHAConfigurationOrDefault(clusterName), sourceDataset,
                        new URI(cluster.getFsEndpoint()), cluster);
            }
        } catch (IOException e) {
            throw new  ValidationException(e, "Dataset {} doesn't exists.", sourceDataset);
        } catch (URISyntaxException | InterruptedException e) {
            throw new BeaconException(e);
        }
    }

    private static boolean isTDEEnabled(Cluster cluster, String dataset) throws BeaconException {
        String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(cluster.getName(),
                cluster.getFsEndpoint(), dataset);
        return StringUtils.isNotEmpty(baseEncryptedPath);
    }

    private static void validateEntityDataset(final ReplicationPolicy policy) throws BeaconException {
        BeaconNotification notification = new BeaconNotification();
        // TODO : Handle cases when multiple cloud object store are in picture.
        if (PolicyHelper.isDatasetHCFS(policy.getTargetDataset())) {
            boolean sourceDatasetConflicted = ReplicationUtils.isDatasetConflicting(ReplicationHelper
                    .getReplicationType(policy.getType()), policy.getSourceDataset());
            if (sourceDatasetConflicted) {
                notification.addError("Source dataset already in replication to cloud.");
            }
        }
        boolean targetDatasetConflicted = ReplicationUtils.isDatasetConflicting(ReplicationHelper.getReplicationType(
                policy.getType()), policy.getTargetDataset());
        if (targetDatasetConflicted) {
            notification.addError("Target dataset already in replication.");
        }
        if (notification.hasErrors()) {
            throw new BeaconException(notification.errorMessage());
        }
    }

    // TODO : Move it to respective Dataset class.
    private static FileSystem getCloudFileSystem(ReplicationPolicy policy, Destination dest) throws
            BeaconException {
        String dataset = getDataset(policy, dest);
        CloudCred cloudCred = getCloudCred(policy);
        Configuration conf = new BeaconCloudCred(cloudCred).getHadoopConf();
        Configuration confWithS3EndPoint = new BeaconCloudCred(cloudCred).getBucketEndpointConf(dataset);
        merge(conf, confWithS3EndPoint);

        Path cloudPath = new Path(dataset);
        try {
            return FileSystem.get(cloudPath.toUri(), conf);
        } catch (IOException e) {
            throw new BeaconException(e);
        }
    }

    private static String getDataset(ReplicationPolicy policy, Destination dest) {
        return dest == Destination.SOURCE ? policy.getSourceDataset() : policy.getTargetDataset();
    }

    private static FileSystem getFileSystem(String clusterName) throws BeaconException {
        Cluster cluster = clusterDao.getActiveCluster(clusterName);
        return FSUtils.getFileSystem(cluster.getFsEndpoint(), new Configuration(), false);
    }


    private static void validateFSTargetDS(ReplicationPolicy policy) throws BeaconException {
        FileSystem fileSystem;
        boolean isHCFS = PolicyHelper.isPolicyHCFS(policy);
        String targetDataset = policy.getTargetDataset();
        if (PolicyHelper.isDatasetHCFS(targetDataset)) {
            fileSystem = getCloudFileSystem(policy, Destination.TARGET);
        } else {
            fileSystem = getFileSystem(policy.getTargetCluster());
        }
        try {
            if (fileSystem.exists(new Path(targetDataset))) {
                RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(targetDataset), true);
                if (files != null && files.hasNext()) {
                    throw new ValidationException("Target dataset directory {} is not empty.", targetDataset);
                }
                if (isHCFS) {
                    return;
                }
                boolean isEncrypted;
                String clusterName = policy.getTargetCluster();
                Cluster sourceCluster = clusterDao.getActiveCluster(policy.getSourceCluster());
                Cluster targetCluster = clusterDao.getActiveCluster(clusterName);
                String sourceDataset = policy.getSourceDataset();
                isEncrypted = isTDEEnabled(targetCluster, targetDataset);
                boolean sourceSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(sourceCluster.getName(),
                        FSUtils.getStagingUri(sourceCluster.getFsEndpoint(), sourceDataset));
                boolean targetSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(clusterName, FSUtils
                        .getStagingUri(targetCluster.getFsEndpoint(), targetDataset));
                LOG.info("Is source directory: {} snapshottable: {}", sourceDataset, sourceSnapshottable);
                if (isEncrypted && (sourceSnapshottable || targetSnapshottable)) {
                    throw new ValidationException("TDE enabled zone can't be used for snapshot based replication.");
                }
                if (sourceSnapshottable && !targetSnapshottable) {
                    FSSnapshotUtils.allowSnapshot(ClusterHelper.getHAConfigurationOrDefault(clusterName),
                            targetDataset, new URI(targetCluster.getFsEndpoint()), targetCluster);
                }
                if (Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED
                        .getName())) && !isEncrypted) {
                    throw new ValidationException("Target dataset directory {} is not encrypted.", targetDataset);
                } else if (isEncrypted) {
                    policy.getCustomProperties()
                            .setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
                }
            } else if (!isHCFS) {
                createTargetFSDirectory(policy);
            }
        } catch (IOException | InterruptedException | URISyntaxException e) {
            throw new BeaconException(e);
        }
    }

    private static void validateDBTargetDS(ReplicationPolicy policy) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getTargetCluster());
        String targetDataset = policy.getTargetDataset();

        HiveMetadataClient hiveClient = null;
        try {
            hiveClient = HiveMetadataClientFactory.getClient(cluster);
            boolean dbExists = hiveClient.doesDBExist(targetDataset);

            if (dbExists) {
                List<String> tables = hiveClient.getTables(targetDataset);
                if (!tables.isEmpty()) {
                    throw new ValidationException("Target Hive server already has dataset {} with tables",
                            targetDataset);
                }
            }
            boolean isHCFS = PolicyHelper.isDatasetHCFS(cluster.getHiveWarehouseLocation());
            if (isHCFS) {
                validateEncryptionAlgorithmType(policy);
                ensureClusterTDECompatibilityForHive(policy);
            } else {
                boolean sourceEncrypted = Boolean.valueOf(policy.getCustomProperties().getProperty(FSDRProperties
                        .TDE_ENCRYPTION_ENABLED.getName()));
                if (dbExists && sourceEncrypted) {
                    Path dbLocation = hiveClient.getDatabaseLocation(targetDataset);
                    String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(
                            cluster.getName(), cluster.getFsEndpoint(), dbLocation.toString());
                    if (StringUtils.isEmpty(baseEncryptedPath)) {
                        throw new ValidationException("Target dataset DB {} is not encrypted.",
                                targetDataset);
                    }
                }
            }
        } finally {
            HiveMetadataClientFactory.close(hiveClient);
        }
    }

    private static void validateDBSourceDS(ReplicationPolicy policy) throws BeaconException {
        Cluster cluster = ClusterHelper.getActiveCluster(policy.getSourceCluster());
        String sourceDataset = policy.getSourceDataset();

        HiveMetadataClient hiveClient = null;
        try {
            hiveClient = HiveMetadataClientFactory.getClient(cluster);
            Path dbPath = hiveClient.getDatabaseLocation(sourceDataset);
            String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(cluster.getName(),
                    cluster.getFsEndpoint(), dbPath.toUri().getPath());
            if (StringUtils.isNotEmpty(baseEncryptedPath)) {
                policy.getCustomProperties().setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
            }
        } finally {
            HiveMetadataClientFactory.close(hiveClient);
        }
    }


    private static void createTargetFSDirectory(ReplicationPolicy policy) throws BeaconException, IOException {
        LOG.info("Creating snapshot data directory on target file system: {}", policy.getTargetDataset());
        String sourceDataset = policy.getSourceDataset();
        String targetDataSet = policy.getTargetDataset();
        if (FSUtils.isHCFS(new Path(sourceDataset))) {
            Cluster targetCluster = clusterDao.getActiveCluster(policy.getTargetCluster());
            FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(targetCluster), false);
            UserGroupInformation user = UserGroupInformation.getLoginUser();
            FSSnapshotUtils.createFSDirectory(targetFS, FsPermission.getDirDefault(),
                    user.getShortUserName(), user.getShortUserName(), targetDataSet);
            return;
        }
        Cluster sourceCluster = clusterDao.getActiveCluster(policy.getSourceCluster());
        Cluster targetCluster = clusterDao.getActiveCluster(policy.getTargetCluster());
        sourceDataset = FSUtils.getStagingUri(sourceCluster.getFsEndpoint(), policy.getSourceDataset());
        targetDataSet = FSUtils.getStagingUri(targetCluster.getFsEndpoint(), policy.getTargetDataset());

        try {
            FileSystem sourceFS = FSUtils.getFileSystem(sourceCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(sourceCluster), false);
            FileSystem targetFS = FSUtils.getFileSystem(targetCluster.getFsEndpoint(),
                    ClusterHelper.getHAConfigurationOrDefault(targetCluster), false);

            boolean isSourceDirSnapshottable = FSSnapshotUtils.checkSnapshottableDirectory(sourceCluster.getName(),
                    sourceDataset);
            LOG.info("Is source directory: {} snapshottable: {}", sourceDataset, isSourceDirSnapshottable);

            FileStatus fsStatus = sourceFS.getFileStatus(new Path(sourceDataset));
            Configuration conf = ClusterHelper.getHAConfigurationOrDefault(targetCluster);
            conf.set(BeaconConstants.FS_DEFAULT_NAME_KEY, targetCluster.getFsEndpoint());
            FSSnapshotUtils.createFSDirectory(targetFS, fsStatus.getPermission(),
                    fsStatus.getOwner(), fsStatus.getGroup(), targetDataSet);
            if (isSourceDirSnapshottable) {
                FSSnapshotUtils.allowSnapshot(conf, targetDataSet, targetFS.getUri(), targetCluster);
            }
        } catch (IOException | InterruptedException e) {
            LOG.error("Exception occurred while creating snapshottable directory on target: {}", e);
            throw new BeaconException("Exception occurred while creating snapshottable directory on target: ", e);
        }
    }

    private static void updateSnapshotCache(ReplicationPolicy policy) throws BeaconException {
        if (!PolicyHelper.isDatasetHCFS(policy.getSourceDataset())) {
            Cluster sourceCluster = clusterDao.getActiveCluster(policy.getSourceCluster());
            SnapshotListing.get().updateListing(sourceCluster.getName(), sourceCluster.getFsEndpoint(), Path.SEPARATOR);
        }
        if (!PolicyHelper.isDatasetHCFS(policy.getTargetDataset())) {
            Cluster targetCluster = clusterDao.getActiveCluster(policy.getTargetCluster());
            SnapshotListing.get().updateListing(targetCluster.getName(), targetCluster.getFsEndpoint(), Path.SEPARATOR);
        }
    }
}
