package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Acl;
import com.hortonworks.beacon.client.entity.Notification;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.ReplicationPolicyProperties;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

public final class ReplicationPolicyBuilder {
    private ReplicationPolicyBuilder() {
    }

    public static ReplicationPolicy buildPolicy(final Properties requestProperties) throws BeaconException {
        for (ReplicationPolicyProperties property : ReplicationPolicyProperties.values()) {
            if (requestProperties.getProperty(property.getName()) == null && property.isRequired()) {
                throw new BeaconException("Missing parameter: " + property.getName());
            }
        }

        String localClusterName = BeaconConfig.getInstance().getEngine().getLocalClusterName();
        String sourceCluster = requestProperties.getProperty(ReplicationPolicyProperties.SOURCELUSTER.getName());
        String targetCluster = requestProperties.getProperty(ReplicationPolicyProperties.TARGETCLUSTER.getName());

        if (!localClusterName.equalsIgnoreCase(sourceCluster) && !localClusterName.equalsIgnoreCase(targetCluster)) {
            throw new BeaconException("Either sourceCluster or targetCluster should be same as local cluster " +
                    "name: " + localClusterName);
        }

        String name = requestProperties.getProperty(ReplicationPolicyProperties.NAME.getName());
        String type = requestProperties.getProperty(ReplicationPolicyProperties.TYPE.getName());
        String dataset = requestProperties.getProperty(ReplicationPolicyProperties.DATASET.getName());

        Date start = validateAndGetDate(requestProperties.getProperty(
                ReplicationPolicyProperties.STARTTIME.getName()));
        Date end = validateAndGetDate(requestProperties.getProperty(
                ReplicationPolicyProperties.ENDTIME.getName()));
        String tags = requestProperties.getProperty(ReplicationPolicyProperties.TAGS.getName());
        Long frequencyInSec = Long.parseLong(requestProperties.getProperty(
                ReplicationPolicyProperties.FREQUENCY.getName()));
        Properties properties = EntityHelper.getCustomProperties(requestProperties,
                ReplicationPolicyProperties.getPolicyElements());

        int attempts = Retry.RETRY_ATTEMPTS;
        String retryAttempts = requestProperties.getProperty(ReplicationPolicyProperties.RETRY_ATTEMPTS.getName());
        if (StringUtils.isNotBlank(retryAttempts)) {
            attempts = Integer.parseInt(retryAttempts);
        }

        long delay = Retry.RETRY_DELAY;
        String retryDelay = requestProperties.getProperty(ReplicationPolicyProperties.RETRY_DELAY.getName());
        if (StringUtils.isNotBlank(retryDelay)) {
            delay = Long.parseLong(retryDelay);
        }

        Retry retry = new Retry(attempts, delay);

        String aclOwner = requestProperties.getProperty(ReplicationPolicyProperties.ACL_OWNER.getName());
        String aclGroup = requestProperties.getProperty(ReplicationPolicyProperties.ACL_GROUP.getName());
        String aclPermission = requestProperties.getProperty(ReplicationPolicyProperties.ACL_PERMISSION.getName());
        Acl acl = new Acl(aclOwner, aclGroup, aclPermission);

        String to = requestProperties.getProperty(ReplicationPolicyProperties.NOTIFICATION_ADDRESS.getName());
        String notificationType = requestProperties.getProperty(
                ReplicationPolicyProperties.NOTIFICATION_TYPE.getName());
        Notification notification = new Notification(to, notificationType);

        return new ReplicationPolicy.Builder(name, type, dataset, sourceCluster, targetCluster,
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