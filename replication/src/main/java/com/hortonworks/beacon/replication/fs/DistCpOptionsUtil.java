/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.fs;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions;

import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ReplicationDistCpOption;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.util.FileSystemClientFactory;

/**
 * Utility to set DistCp options.
 */
final class DistCpOptionsUtil {
    private static final BeaconLog LOG = BeaconLog.getLog(DistCpOptionsUtil.class);

    private DistCpOptionsUtil() {
    }

    static DistCpOptions getDistCpOptions(Properties fsDRProperties, List<Path> sourcePaths, Path targetPath,
                                          boolean isSnapshot, String fromSnapshot,
                                          String toSnapshot, boolean isInRecoveryMode)
            throws BeaconException, IOException {
        LOG.info(MessageCode.REPL_000030.name());
        DistCpOptions distcpOptions = new DistCpOptions(sourcePaths, targetPath);
        distcpOptions.setBlocking(true);

        String tdeEncryptionEnabled = fsDRProperties.getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName());
        if (StringUtils.isNotBlank(tdeEncryptionEnabled)
                && tdeEncryptionEnabled.equalsIgnoreCase(Boolean.TRUE.toString())) {
            distcpOptions.setSyncFolder(true);
            distcpOptions.setSkipCRC(true);
        } else {
            if (!isSnapshot) {
                String overwrite = fsDRProperties.getProperty(
                        ReplicationDistCpOption.DISTCP_OPTION_OVERWRITE.getName());
                if (StringUtils.isNotEmpty(overwrite) && overwrite.equalsIgnoreCase(Boolean.TRUE.toString())) {
                    distcpOptions.setOverwrite(Boolean.parseBoolean(overwrite));
                } else {
                    distcpOptions.setSyncFolder(true);
                }
            }

            String skipChecksum = fsDRProperties.getProperty(
                    ReplicationDistCpOption.DISTCP_OPTION_SKIP_CHECKSUM.getName());
            if (StringUtils.isNotEmpty(skipChecksum)) {
                distcpOptions.setSkipCRC(Boolean.parseBoolean(skipChecksum));
            }
        }

        if (isSnapshot) {
            // Settings needed for Snapshot distCp.
            distcpOptions.setSyncFolder(true);
            distcpOptions.setDeleteMissing(true);
        } else {
            String removeDeletedFiles = fsDRProperties.getProperty(
                    ReplicationDistCpOption.DISTCP_OPTION_REMOVE_DELETED_FILES.getName(), "true");
            boolean deleteMissing = Boolean.parseBoolean(removeDeletedFiles);
            distcpOptions.setDeleteMissing(deleteMissing);
            if (deleteMissing) {
                // DistCP will fail with InvalidInputException if deleteMissing is set to true and
                // if targetPath does not exist. Create targetPath to avoid failures.
                FileSystem fs = FileSystemClientFactory.get().createProxiedFileSystem(targetPath.toUri(),
                        new Configuration());
                if (!fs.exists(targetPath)) {
                    fs.mkdirs(targetPath);
                }
            }
        }

        if (isSnapshot && StringUtils.isNotBlank(fromSnapshot)) {
            if (isInRecoveryMode) {
                distcpOptions.setUseRdiff(fromSnapshot, toSnapshot);
            } else {
                distcpOptions.setUseDiff(fromSnapshot, toSnapshot);
            }
        }

        String ignoreErrors = fsDRProperties.getProperty(ReplicationDistCpOption.DISTCP_OPTION_IGNORE_ERRORS.getName());
        if (StringUtils.isNotBlank(ignoreErrors)) {
            distcpOptions.setIgnoreFailures(Boolean.parseBoolean(ignoreErrors));
        }

        String preserveBlockSize = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_BLOCK_SIZE.getName());
        if (StringUtils.isNotBlank(preserveBlockSize) && Boolean.parseBoolean(preserveBlockSize)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
        }

        String preserveReplicationCount = fsDRProperties.getProperty(ReplicationDistCpOption
                .DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER.getName());
        if (StringUtils.isNotBlank(preserveReplicationCount) && Boolean.parseBoolean(preserveReplicationCount)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.REPLICATION);
        }

        String preservePermission = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_PERMISSIONS.getName(), "true");
        if (StringUtils.isNotBlank(preservePermission) && Boolean.parseBoolean(preservePermission)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.PERMISSION);
        }

        String preserveUser = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_USER.getName(), "true");
        if (StringUtils.isNotBlank(preserveUser) && Boolean.parseBoolean(preserveUser)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.USER);
        }

        String preserveGroup = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_GROUP.getName(), "true");
        if (StringUtils.isNotBlank(preserveGroup) && Boolean.parseBoolean(preserveGroup)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.GROUP);
        }

        String preserveChecksumType = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE.getName());
        if (StringUtils.isNotBlank(preserveChecksumType) && Boolean.parseBoolean(preserveChecksumType)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.CHECKSUMTYPE);
        }

        String preserveAcl = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_ACL.getName());
        if (StringUtils.isNotBlank(preserveAcl) && Boolean.parseBoolean(preserveAcl)) {
            LOG.info(MessageCode.REPL_000031.name(), preserveAcl);
            distcpOptions.preserve(DistCpOptions.FileAttribute.ACL);
        }

        String preserveXattr = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_XATTR.getName(), "true");
        if (StringUtils.isNotBlank(preserveXattr) && Boolean.parseBoolean(preserveXattr)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.XATTR);
        }

        String preserveTimes = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_TIMES.getName());
        if (StringUtils.isNotBlank(preserveTimes) && Boolean.parseBoolean(preserveTimes)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.TIMES);
        }

        String maxMaps = fsDRProperties.getProperty(FSDRProperties.DISTCP_MAX_MAPS.getName());
        if (maxMaps != null) {
            distcpOptions.setMaxMaps(Integer.parseInt(maxMaps));
        }

        String maxBandwidth = fsDRProperties.getProperty(FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName());
        if (maxBandwidth != null) {
            distcpOptions.setMapBandwidth(Integer.parseInt(maxBandwidth));
        }
        LOG.info(MessageCode.REPL_000081.name(), distcpOptions.toString());
        return distcpOptions;
    }
}
