/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.log;

import com.hortonworks.beacon.config.BeaconConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;

/**
 * Utility class for BLogs.
 */
public final class BeaconLogUtils {
    private BeaconLogUtils() {
    }


    public static void createPrefix(String userId, String clusterName) {
        BeaconLog.Info info = new BeaconLog.Info();
        clearLogPrefix();
        if (StringUtils.isNotBlank(userId)) {
            info.setParameter(BeaconLogParams.USER.name(), userId);
        }
        if (StringUtils.isNotBlank(clusterName)) {
            info.setParameter(BeaconLogParams.CLUSTER.name(), clusterName);
        }
        NDC.push(info.resetPrefix());
    }

    public static void createPrefix(String userName, String clusterName, String policyName,
                                  String policyId, String instanceId) {
        BeaconLog.Info info = new BeaconLog.Info();
        clearLogPrefix();
        info.setParameter(BeaconLogParams.USER.name(), userName);
        info.setParameter(BeaconLogParams.CLUSTER.name(), clusterName);
        info.setParameter(BeaconLogParams.POLICYNAME.name(), policyName);
        info.setParameter(BeaconLogParams.POLICYID.name(), policyId);
        info.setParameter(BeaconLogParams.INSTANCEID.name(), instanceId);
        NDC.push(info.resetPrefix());
    }

    public static void createPrefix(String userName, String clusterName, String policyName,
                                  String policyId) {
        BeaconLog.Info info = new BeaconLog.Info();
        clearLogPrefix();
        if (StringUtils.isNotBlank(userName)) {
            info.setParameter(BeaconLogParams.USER.name(), userName);
        }
        if (StringUtils.isNotBlank(clusterName)) {
            info.setParameter(BeaconLogParams.CLUSTER.name(), clusterName);
        }
        if (StringUtils.isNotBlank(policyName)) {
            info.setParameter(BeaconLogParams.POLICYNAME.name(), policyName);
        }
        if (StringUtils.isNotBlank(policyId)) {
            info.setParameter(BeaconLogParams.POLICYID.name(), policyId);
        }
        NDC.push(info.resetPrefix());
    }

    public static void createPrefix(String userName, String clusterName, String policyName) {
        createPrefix(userName, clusterName, policyName, null);
    }

    public static void createPrefix(String id) {
        BeaconLog.Info info = new BeaconLog.Info();
        clearLogPrefix();
        info.setParameter(BeaconLogParams.CLUSTER.getName(),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        if (id.contains("@")) {
            String jobId = id.substring(0, id.indexOf("@"));
            info.setParameter(BeaconLogParams.POLICYID.name(), jobId);
            info.setParameter(BeaconLogParams.INSTANCEID.name(), id);
        } else {
            info.setParameter(BeaconLogParams.POLICYID.name(), id);
            info.setParameter(BeaconLogParams.INSTANCEID.name(), "");
        }
        NDC.push(info.resetPrefix());
    }

    public static BeaconLog setLogInfo(BeaconLog log, String userName, String clusterName,
                                       String policyId, String instanceId) {
        BeaconLog.Info info = new BeaconLog.Info();
        clearLogPrefix();
        if (StringUtils.isNotBlank(userName)) {
            info.setParameter(BeaconLogParams.USER.name(), userName);
        }
        if (StringUtils.isNotBlank(clusterName)) {
            info.setParameter(BeaconLogParams.CLUSTER.name(), clusterName);
        }
        if (StringUtils.isNotBlank(policyId)) {
            info.setParameter(BeaconLogParams.POLICYID.name(), policyId);
        }
        if (StringUtils.isNotBlank(instanceId)) {
            info.setParameter(BeaconLogParams.INSTANCEID.name(), instanceId);
        }

        return BeaconLog.resetPrefix(log);
    }

    public static void clearLogPrefix() {
        BeaconLog.Info info = new BeaconLog.Info();
        info.clearParameter(BeaconLogParams.USER.name());
        info.clearParameter(BeaconLogParams.CLUSTER.name());
        info.clearParameter(BeaconLogParams.POLICYNAME.name());
        info.clearParameter(BeaconLogParams.POLICYID.name());
        info.clearParameter(BeaconLogParams.INSTANCEID.name());
        info.resetPrefix();
    }

    public static void deletePrefix(){
        clearLogPrefix();
        NDC.pop();
    }
}
