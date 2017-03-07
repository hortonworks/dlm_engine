/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Acl;
import com.hortonworks.beacon.client.entity.Notification;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.ReplicationPolicyProperties;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
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
                throw new BeaconException("Missing parameter: " + property.getName());
            }
        }

        String name = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.NAME.getName());
        String type = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.TYPE.getName());
        ReplicationHelper.validateReplicationType(type);
        ReplicationType replType = ReplicationHelper.getReplicationType(type);

        String sourceCluster = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.SOURCELUSTER.getName());
        String targetCluster = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.TARGETCLUSTER.getName());
        String sourceDataset = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.SOURCEDATASET.getName());
        String targetDataset = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.TARGETDATASET.getName());

        if (ReplicationType.FS == replType) {
            // If dataset is not HCFS, clusters are mandatory
            if (!PolicyHelper.isPolicyHCFS(sourceDataset, targetDataset)) {
                if (StringUtils.isBlank(sourceCluster)) {
                    throw new BeaconException("Missing parameter: " + ReplicationPolicyProperties.SOURCELUSTER.getName());
                }
                if (StringUtils.isBlank(targetCluster)) {
                    throw new BeaconException("Missing parameter: " + ReplicationPolicyProperties.TARGETCLUSTER.getName());
                }
            }

            // If HCFS, both datasets are mandatory and both datasets can't be HCFS
            if (PolicyHelper.isPolicyHCFS(sourceDataset, targetDataset)) {
                if (StringUtils.isBlank(sourceDataset)) {
                    throw new BeaconException("Missing parameter: " + ReplicationPolicyProperties.SOURCEDATASET.getName());
                }
                if (StringUtils.isBlank(targetDataset)) {
                    throw new BeaconException("Missing parameter: " + ReplicationPolicyProperties.TARGETDATASET.getName());
                }

                if (FSUtils.isHCFS(new Path(sourceDataset)) && FSUtils.isHCFS(new Path(targetDataset))) {
                    throw new BeaconException("HCFS to HCFS replication is not allowed");
                }
            }
        }

        String localClusterName = BeaconConfig.getInstance().getEngine().getLocalClusterName();
        if (!localClusterName.equalsIgnoreCase(sourceCluster) && !localClusterName.equalsIgnoreCase(targetCluster)) {
            throw new BeaconException("Either sourceCluster or targetCluster should be same as local cluster "
                    + "name: " + localClusterName);
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

        Date start = validateAndGetDate(requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.STARTTIME.getName()));
        Date end = validateAndGetDate(requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.ENDTIME.getName()));
        String tags = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.TAGS.getName());
        Integer frequencyInSec = Integer.parseInt(requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.FREQUENCY.getName()));
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

        String aclOwner = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.ACL_OWNER.getName());
        String aclGroup = requestProperties.getPropertyIgnoreCase(ReplicationPolicyProperties.ACL_GROUP.getName());
        String aclPermission = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.ACL_PERMISSION.getName());
        Acl acl = new Acl(aclOwner, aclGroup, aclPermission);

        String to = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.NOTIFICATION_ADDRESS.getName());
        String notificationType = requestProperties.getPropertyIgnoreCase(
                ReplicationPolicyProperties.NOTIFICATION_TYPE.getName());
        Notification notification = new Notification(to, notificationType);

        return new ReplicationPolicy.Builder(name, type, sourceDataset, targetDataset,
                sourceCluster,
                targetCluster,
                frequencyInSec).startTime(start).endTime(end).tags(tags).customProperties(properties).retry(retry)
                .acl(acl).notification(notification).build();
    }

    private static Date validateAndGetDate(final String strDate) throws BeaconException {
        if (StringUtils.isBlank(strDate)) {
            return null;
        }
        Date date;
        try {
            date = DateUtil.getDateFormat().parse(strDate);
        } catch (ParseException e) {
            throw new BeaconException(e);
        }
        return date;
    }
}
