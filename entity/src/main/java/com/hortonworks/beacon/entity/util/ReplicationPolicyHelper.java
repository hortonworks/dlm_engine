package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.entity.Notification;
import com.hortonworks.beacon.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.ReplicationPolicyProperties;
import com.hortonworks.beacon.entity.Retry;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public final class ReplicationPolicyHelper {
    private ReplicationPolicyHelper() {
    }

    public static ReplicationPolicy buildPolicy(final Properties requestProperties) {
        ReplicationPolicy policy = new ReplicationPolicy();

        policy.setName(requestProperties.getProperty(ReplicationPolicyProperties.NAME.getName()));
        policy.setType(requestProperties.getProperty(ReplicationPolicyProperties.TYPE.getName()));
        policy.setDataset(requestProperties.getProperty(ReplicationPolicyProperties.DATASET.getName()));
        policy.setTags(requestProperties.getProperty(ReplicationPolicyProperties.TAGS.getName()));
        policy.setSourceCluster(requestProperties.getProperty(ReplicationPolicyProperties.SOURCELUSTER.getName()));
        policy.setTargetCluster(requestProperties.getProperty(ReplicationPolicyProperties.TARGETCLUSTER.getName()));
        policy.setFrequencyInSec(Long.parseLong(requestProperties.getProperty(
                ReplicationPolicyProperties.FREQUENCY.getName())));
        policy.setCustomProperties(EntityHelper.getCustomProperties(requestProperties,
                ReplicationPolicyProperties.getPolicyElements()));

        final int defaultRetryAttempts = 3;
        // 30 minutes in sec
        final long defaultRetryDelay = 1800;
        Retry retry = new Retry();
        String retryAttempts = requestProperties.getProperty(ReplicationPolicyProperties.RETRY_ATTEMPTS.getName());
        if (StringUtils.isNotBlank(retryAttempts)) {
            retry.setAttempts(Integer.parseInt(retryAttempts));
        } else {
            retry.setAttempts(defaultRetryAttempts);
        }

        String retryDelay = requestProperties.getProperty(ReplicationPolicyProperties.RETRY_DELAY.getName());
        if (StringUtils.isNotBlank(retryDelay)) {
            retry.setDelay(Long.parseLong(retryDelay));
        } else {
            retry.setDelay(defaultRetryDelay);
        }
        policy.setRetry(retry);

        String aclOwner = requestProperties.getProperty(ReplicationPolicyProperties.ACL_OWNER.getName());
        String aclGroup = requestProperties.getProperty(ReplicationPolicyProperties.ACL_GROUP.getName());
        String aclPermission = requestProperties.getProperty(ReplicationPolicyProperties.ACL_PERMISSION.getName());
        policy.setAcl(EntityHelper.buildACL(aclOwner, aclGroup, aclPermission));

        Notification notification = new Notification();
        notification.setTo(requestProperties.getProperty(ReplicationPolicyProperties.NOTIFICATION_ADDRESS.getName()));
        notification.setType(requestProperties.getProperty(ReplicationPolicyProperties.NOTIFICATION_TYPE.getName()));
        policy.setNotification(notification);

        return policy;
    }
}