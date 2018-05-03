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

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.SchemeType;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Notification;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.client.util.EntityHelper;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.ReplicationPolicyProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

/**
 * Builder class to construct Beacon ReplicationPolicy resource.
 */
public final class ReplicationPolicyBuilder {
    private ReplicationPolicyBuilder() {
    }

    public static ReplicationPolicy buildPolicy(final PropertiesIgnoreCase requestProperties,
                                                final String policyName, boolean isDryRun) throws BeaconException {
        requestProperties.put(ReplicationPolicyProperties.NAME.getName(), policyName);
        if (isDryRun) {
            requestProperties.put(ReplicationPolicyFields.FREQUENCYINSEC.getName(), "1");
        }
        for (ReplicationPolicyProperties property : ReplicationPolicyProperties.values()) {
            if (requestProperties.getPropertyIgnoreCase(property.getName()) == null && property.isRequired()) {
                throw new BeaconException("Missing parameter: {}", property.getName());
            }
        }

        String name = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.NAME.getName());
        String type = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.TYPE.getName());
        String description = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.DESCRIPTION.getName());
        ReplicationType replType = ReplicationHelper.getReplicationType(type);
        type = replType.toString();

        String sourceCluster = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.SOURCECLUSTER.getName());
        String targetCluster = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.TARGETCLUSTER.getName());
        String cloudEntityId = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyFields.CLOUDCRED.getName());
        String sourceDataset = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.SOURCEDATASET.getName());
        String targetDataset = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.TARGETDATASET.getName());

        if (PolicyHelper.isPolicyHCFS(sourceDataset, targetDataset)) {
            if (StringUtils.isBlank(cloudEntityId)) {
                throw new ValidationException("Missing parameter: {}", ReplicationPolicyFields.CLOUDCRED.getName());
            }
        }

        if (ReplicationType.FS == replType) {
            // If dataset is not HCFS, clusters are mandatory
            if (StringUtils.isBlank(cloudEntityId)) {
                if (StringUtils.isBlank(sourceCluster)) {
                    throw new BeaconException("Missing parameter: {}",
                        ReplicationPolicyProperties.SOURCECLUSTER.getName());
                }
                checkHDFSEnabled(ClusterHelper.getActiveCluster(sourceCluster));
                if (StringUtils.isBlank(targetCluster)) {
                    throw new BeaconException("Missing parameter: {}",
                        ReplicationPolicyProperties.TARGETCLUSTER.getName());
                }
                checkHDFSEnabled(ClusterHelper.getActiveCluster(targetCluster));
            } else {
                if (StringUtils.isNotBlank(sourceCluster) == StringUtils.isNotBlank(targetCluster)) {
                    throw new ValidationException("Either source or target cluster must be provided and only one.");
                }
                if (StringUtils.isBlank(sourceCluster)) {
                    sourceDataset = appendCloudSchema(cloudEntityId, sourceDataset, SchemeType.NAME);
                    checkHDFSEnabled(ClusterHelper.getActiveCluster(targetCluster));
                }
                if (StringUtils.isBlank(targetCluster)) {
                    targetDataset = appendCloudSchema(cloudEntityId, targetDataset, SchemeType.NAME);
                    checkHDFSEnabled(ClusterHelper.getActiveCluster(sourceCluster));
                }
            }

            // If HCFS, both datasets are mandatory and both datasets can't be HCFS
            if (PolicyHelper.isPolicyHCFS(sourceDataset, targetDataset)) {
                if (StringUtils.isBlank(sourceDataset)) {
                    throw new BeaconException("Missing parameter: {}",
                            ReplicationPolicyProperties.SOURCEDATASET.getName());
                }
                if (StringUtils.isBlank(targetDataset)) {
                    throw new BeaconException("Missing parameter: {}",
                            ReplicationPolicyProperties.TARGETDATASET.getName());
                }

                if (FSUtils.isHCFS(new Path(sourceDataset)) && FSUtils.isHCFS(new Path(targetDataset))) {
                    throw new BeaconException("HCFS to HCFS replication is not allowed");
                }
            }
        }

        String localClusterName = ClusterHelper.getLocalCluster().getName();
        if (!localClusterName.equalsIgnoreCase(sourceCluster) && !localClusterName.equalsIgnoreCase(targetCluster)) {
            throw new BeaconException("Either sourceCluster or targetCluster should be same as local cluster name: {}",
                localClusterName);
        }


        if (StringUtils.isBlank(targetDataset)) {
            // Get only dir path if full absolute path is passed for source dataset
            try {
                URI sourceUri = new URI(sourceDataset.trim());
                targetDataset = sourceUri.getPath();
            } catch (URISyntaxException e) {
                throw new BeaconException(e);
            }
        }
        if (isDryRun) {
            Properties customProps = EntityHelper.getCustomProperties(requestProperties,
                    ReplicationPolicyProperties.getPolicyElements());
            Retry retry = new Retry(Retry.RETRY_ATTEMPTS, Retry.RETRY_DELAY);
            return new ReplicationPolicy.Builder(name, type, sourceDataset, targetDataset,
                    sourceCluster,
                    targetCluster,
                    1).customProperties(customProps).retry(retry).build();
        }
        Date start = DateUtil.parseDate(requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.STARTTIME.getName()));
        Date end = DateUtil.parseDate(requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.ENDTIME.getName()));
        String tags = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.TAGS.getName());
        Integer frequencyInSec = Integer.parseInt(requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.FREQUENCY.getName()));

        int defaultReplicationFrequencyInSec = BeaconConfig.getInstance().getScheduler().getMinReplicationFrequency();
        if (frequencyInSec < defaultReplicationFrequencyInSec) {
            throw new BeaconException("Specified Replication frequency {} seconds should not be less than {} seconds",
                frequencyInSec, defaultReplicationFrequencyInSec);
        }

        setMetaLocation(requestProperties);
        Properties properties = EntityHelper.getCustomProperties(requestProperties,
                ReplicationPolicyProperties.getPolicyElements());

        int attempts = Retry.RETRY_ATTEMPTS;
        String retryAttempts = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.RETRY_ATTEMPTS.getName());
        if (StringUtils.isNotBlank(retryAttempts)) {
            attempts = Integer.parseInt(retryAttempts);
        }

        long delay = Retry.RETRY_DELAY;
        String retryDelay = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.RETRY_DELAY.getName());
        if (StringUtils.isNotBlank(retryDelay)) {
            delay = Long.parseLong(retryDelay);
        }

        Retry retry = new Retry(attempts, delay);
        String user = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.USER.getName());
        user = StringUtils.isBlank(user) ? System.getProperty("user.name") : user;

        String to = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.NOTIFICATION_ADDRESS.getName());
        String notificationType = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.NOTIFICATION_TYPE.getName());
        Notification notification = new Notification(to, notificationType);

        return new ReplicationPolicy.Builder(name, type, sourceDataset, targetDataset,
                sourceCluster,
                targetCluster,
                frequencyInSec).startTime(start).endTime(end).tags(tags).customProperties(properties).retry(retry)
                .user(user).notification(notification).description(description).build();
    }

    private static void setMetaLocation(PropertiesIgnoreCase requestProperties) {
        boolean preserveMeta = BeaconConfig.getInstance().getEngine().isPreserveMeta()
                || Boolean.valueOf(requestProperties.getPropertyIgnoreCase("preserve.meta"));

        String policyName = requestProperties.getPropertyIgnoreCase(ReplicationPolicyFields.NAME.getName());
        String metaLocation = new Path(BeaconConfig.getInstance().getEngine().getPluginStagingPath(), policyName)
                .toString();
        requestProperties.setProperty(BeaconConstants.PLUGIN_STAGING_DIR, metaLocation);
        if (preserveMeta) {
            requestProperties.setProperty(BeaconConstants.META_LOCATION, metaLocation);
        }
    }

    public static String appendCloudSchema(String cloudEntityId, String dataset, SchemeType schemeType)
            throws BeaconException {
        CloudCredDao cloudCredDao = new CloudCredDao();
        CloudCred cloudCred = cloudCredDao.getCloudCred(cloudEntityId);
        return appendCloudSchema(cloudCred, dataset, schemeType);
    }

    public static String appendCloudSchema(CloudCred cloudCred, String dataset, SchemeType schemeType)
            throws BeaconException {
        Path path = new Path(dataset);
        URI uri = path.toUri();
        String scheme = uri.getScheme();
        String replaceScheme;
        if (schemeType == SchemeType.HCFS_NAME) {
            replaceScheme = cloudCred.getProvider().getHcfsScheme().toLowerCase(Locale.ENGLISH);
        } else {
            replaceScheme = cloudCred.getProvider().getScheme().toLowerCase(Locale.ENGLISH);
        }
        if (StringUtils.isNotBlank(scheme) && FSUtils.isHCFS(path)) {
            dataset = dataset.replaceFirst(scheme, replaceScheme);
        } else {
            dataset = replaceScheme.concat("://").concat(dataset);
        }
        return dataset.endsWith(Path.SEPARATOR) ? dataset : dataset.concat(Path.SEPARATOR);
    }

    private static void checkHDFSEnabled(Cluster cluster) throws ValidationException {
        if (ClusterHelper.isHDFSEnabled(cluster)) {
            return;
        }
        throw new ValidationException("Namenode endpoint not found in cluster: {}",
                cluster.getName());
    }
}
