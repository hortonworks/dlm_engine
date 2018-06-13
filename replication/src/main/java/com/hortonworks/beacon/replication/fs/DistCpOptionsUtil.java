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

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ReplicationDistCpOption;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.FileSystemClientFactory;

/**
 * Utility to set DistCp options.
 */
final class DistCpOptionsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DistCpOptionsUtil.class);

    private DistCpOptionsUtil() {
    }

    static DistCpOptions getHCFSDistCpOptions(Properties fsDRProperties, List<Path> sourcePaths, Path targetPath,
                                              boolean isSnapshot, String fromSnapshot, String toSnapshot)
                                              throws IOException, BeaconException {
        LOG.info("Setting HCFS distCp options for source paths and target path");
        DistCpOptions distcpOptions = new DistCpOptions(sourcePaths, targetPath);
        distcpOptions.setBlocking(true);
        distcpOptions.setTargetPathExists(false);
        distcpOptions.setSyncFolder(true);
        distcpOptions.setDeleteMissing(true);
        if (isSnapshot && StringUtils.isNotBlank(fromSnapshot)) {
            distcpOptions.setUseDiff(fromSnapshot, toSnapshot);
        }

        String ignoreErrors = fsDRProperties.getProperty(ReplicationDistCpOption.DISTCP_OPTION_IGNORE_ERRORS.getName());
        if (StringUtils.isNotBlank(ignoreErrors)) {
            distcpOptions.setIgnoreFailures(Boolean.parseBoolean(ignoreErrors));
        }

        String maxMaps = fsDRProperties.getProperty(FSDRProperties.DISTCP_MAX_MAPS.getName());
        if (maxMaps != null) {
            distcpOptions.setMaxMaps(Integer.parseInt(maxMaps));
        }

        String maxBandwidth = fsDRProperties.getProperty(FSDRProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName());
        if (maxBandwidth != null) {
            distcpOptions.setMapBandwidth(Integer.parseInt(maxBandwidth));
        }

        LOG.info("HCFS DistCp options submitted: [{}]", distcpOptions.toString());
        return distcpOptions;
    }

    static DistCpOptions getDistCpOptions(Properties fsDRProperties, List<Path> sourcePaths, Path targetPath,
                                          boolean isSnapshot, String fromSnapshot,
                                          String toSnapshot, boolean isInRecoveryMode)
            throws BeaconException, IOException {
        LOG.info("Setting distcp options for source paths and target path");
        DistCpOptions distcpOptions = new DistCpOptions(sourcePaths, targetPath);
        distcpOptions.setBlocking(true);

        boolean tdeEncryptionEnabled = Boolean.valueOf(fsDRProperties.getProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED
                .getName()));
        boolean tdeSameKey = Boolean.valueOf(fsDRProperties.getProperty(FSDRProperties.TDE_SAMEKEY.getName()));
        if (tdeEncryptionEnabled && !tdeSameKey) {
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
                FileSystem fs = FileSystemClientFactory.get().createFileSystem(targetPath.toString(),
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
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_BLOCK_SIZE.getName(), "true");
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
            LOG.info("Preserve ACL: {}", preserveAcl);
            distcpOptions.preserve(DistCpOptions.FileAttribute.ACL);
        }

        String preserveXattr = fsDRProperties.getProperty(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_XATTR.getName());
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
        LOG.info("DistCp options submitted: [{}]", distcpOptions.toString());
        return distcpOptions;
    }
}
