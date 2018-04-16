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
package com.hortonworks.beacon.log;

import com.hortonworks.beacon.RequestContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class for BLogs.
 */
public final class BeaconLogUtils {
    private BeaconLogUtils() {
    }

    public static void prefixRequest(String requestId) {
        Info info = RequestContext.get().getLogPrefix();
        info.setParameter(BeaconLogParams.REQUEST_ID, requestId);
        NDC.clear();
        NDC.push(info.getPrefixString());
    }

    static void prefixPolicy(String policyName, String policyId, String instanceId) {
        Info info = RequestContext.get().getLogPrefix();
        if (StringUtils.isNotBlank(policyName)) {
            info.setParameter(BeaconLogParams.POLICYNAME, policyName);
        }
        if (StringUtils.isNotBlank(policyId)) {
            info.setParameter(BeaconLogParams.POLICYID, policyId);
        }
        if (StringUtils.isNotBlank(instanceId)) {
            info.setParameter(BeaconLogParams.INSTANCEID, instanceId);
        }
        NDC.clear();
        NDC.push(info.getPrefixString());
    }

    public static void prefixPolicy(String policyName, String policyId) {
        prefixPolicy(policyName, policyId, null);
    }

    public static void prefixPolicy(String policyName) {
        prefixPolicy(policyName, null);
    }

    public static void prefixId(String id) {
        Info info = RequestContext.get().getLogPrefix();
        if (id.contains("@")) {
            String jobId = id.substring(0, id.indexOf("@"));
            info.setParameter(BeaconLogParams.POLICYID, jobId);
            info.setParameter(BeaconLogParams.INSTANCEID, id);
        } else {
            info.setParameter(BeaconLogParams.POLICYID, id);
        }
        NDC.clear();
        NDC.push(info.getPrefixString());
    }

    /**
     * Info class to store contextual information to create log prefixes.
     */
    public static class Info {
        private Map<BeaconLogParams, String> parameters = new LinkedHashMap<>();

        public Info() {
        }

        public void setParameter(BeaconLogParams name, String value) {
            parameters.put(name, value);
        }

        String getPrefixString() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<BeaconLogParams, String> entry : parameters.entrySet()){
                sb.append(entry.getKey().getName());
                sb.append("[");
                sb.append(entry.getValue());
                sb.append("] ");
            }

            return sb.toString().trim();
        }
    }
}
