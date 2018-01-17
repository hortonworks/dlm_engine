/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Notification;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.FSDRProperties;
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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;

/**
 * Builder class to construct Beacon ReplicationPolicy resource.
 */
public final class ReplicationPolicyBuilder {
    private ReplicationPolicyBuilder() {
    }

    public static ReplicationPolicy buildPolicy(final PropertiesIgnoreCase requestProperties,
                                                final String policyName) throws BeaconException {
        requestProperties.put(ReplicationPolicyProperties.NAME.getName(), policyName);
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
                ReplicationPolicyProperties.SOURCELUSTER.getName());
        String targetCluster = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.TARGETCLUSTER.getName());
        String cloudEntityId = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.CLOUD_CRED.getName());
        String sourceDataset = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.SOURCEDATASET.getName());
        String targetDataset = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.TARGETDATASET.getName());

        if (ReplicationType.FS == replType) {
            // If dataset is not HCFS, clusters are mandatory
            if (StringUtils.isBlank(cloudEntityId)) {
                if (StringUtils.isBlank(sourceCluster)) {
                    throw new BeaconException("Missing parameter: {}",
                        ReplicationPolicyProperties.SOURCELUSTER.getName());
                }
                if (StringUtils.isBlank(targetCluster)) {
                    throw new BeaconException("Missing parameter: {}",
                        ReplicationPolicyProperties.TARGETCLUSTER.getName());
                }
            } else {
                if (StringUtils.isNotBlank(sourceCluster) == StringUtils.isNotBlank(targetCluster)) {
                    throw new ValidationException("Either source or target cluster must be provided and only one.");
                }

                if (StringUtils.isBlank(sourceCluster)) {
                    sourceCluster = cloudEntityId;
                    sourceDataset = appendCloudSchema(cloudEntityId, sourceDataset);
                }

                if (StringUtils.isBlank(targetCluster)) {
                    targetCluster = cloudEntityId;
                    targetDataset = appendCloudSchema(cloudEntityId, targetDataset);
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

            Cluster cluster = ClusterHelper.getActiveCluster(sourceCluster);
            try {
                String baseEncryptedPath = EncryptionZoneListing.get().getBaseEncryptedPath(cluster.getName(),
                        cluster.getFsEndpoint(), sourceDataset);
                if (StringUtils.isNotEmpty(baseEncryptedPath)) {
                    requestProperties.put(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
                }
            } catch (IOException e) {
                throw new BeaconException(e);
            } catch (URISyntaxException e) {
                throw new BeaconException(e, "Source dataset path {} might not be valid.", sourceDataset);
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
                .user(user).notification(notification).description(description).cloudCred(cloudEntityId).build();
    }

    private static String appendCloudSchema(String cloudEntityId, String dataset) throws BeaconException {
        Path path = new Path(dataset);
        URI uri = path.toUri();
        String scheme = uri.getScheme();
        CloudCredDao cloudCredDao = new CloudCredDao();
        CloudCred cred = cloudCredDao.getCloudCred(cloudEntityId);
        if (StringUtils.isNotBlank(scheme) && FSUtils.isHCFS(path)) {
            dataset = dataset.replaceFirst(scheme, cred.getProvider().name().toLowerCase());
        } else {
            dataset = cred.getProvider().name().toLowerCase().concat("://").concat(dataset);
        }
        return dataset;
    }
}
