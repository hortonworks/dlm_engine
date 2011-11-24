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

package com.hortonworks.beacon.authorize.simple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.authorize.BeaconActionTypes;
import com.hortonworks.beacon.authorize.BeaconResourceTypes;
/**
 * This is utility class for policies.
 */
public final class PolicyUtil {

    private static Logger logger = LoggerFactory.getLogger(PolicyUtil.class);
    private static boolean isDebugEnabled = logger.isDebugEnabled();

    private PolicyUtil(){
    }
    public static Map<String, Map<BeaconResourceTypes, List<String>>> createPermissionMap(List<PolicyDef> policyDefList,
        BeaconActionTypes permissionType, SimpleBeaconAuthorizer.BeaconAccessorTypes principalType) {
        if (isDebugEnabled) {
            logger.debug("Creating Permission Map for :: {} & {}", permissionType, principalType);
        }
        Map<String, Map<BeaconResourceTypes, List<String>>> userReadMap = new HashMap<>();

        // Iterate over the list of policies to create map
        for (PolicyDef policyDef : policyDefList) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing policy def : {}", policyDef);
            }

            Map<String, List<BeaconActionTypes>> principalMap =
                principalType.equals(SimpleBeaconAuthorizer.BeaconAccessorTypes.USER) ? policyDef.getUsers() : policyDef
                    .getGroups();
            // For every policy extract the resource list and populate the user map
            for (Entry<String, List<BeaconActionTypes>> e : principalMap.entrySet()) {
                // Check if the user has passed permission type like READ
                if (!e.getValue().contains(permissionType)) {
                    continue;
                }
                // See if the current user is already added to map
                String username = e.getKey();
                Map<BeaconResourceTypes, List<String>> userResourceList = userReadMap.get(username);

                // If its not added then create a new resource list
                if (userResourceList == null) {
                    if (isDebugEnabled) {
                        logger.debug("Resource list not found for {}, creating it", username);
                    }
                    userResourceList = new HashMap<>();
                }
                /*
                 * Iterate over resources from the current policy def and update the resource list for the current user
                 */
                if (policyDef.getResources()!=null) {
                    for (Entry<BeaconResourceTypes, List<String>> resourceTypeMap : policyDef.getResources()
                            .entrySet()) {
                        // For the current resourceType in the policyDef, get the
                        // current list of resources already added
                        BeaconResourceTypes type = resourceTypeMap.getKey();
                        List<String> resourceList = userResourceList.get(type);

                        if (resourceList == null) {
                            // if the resource list was not added for this type then
                            // create and add all the resources in this policy
                            resourceList = new ArrayList<>();
                            resourceList.addAll(resourceTypeMap.getValue());
                        } else {
                            // if the resource list is present then merge both the
                            // list
                            resourceList.removeAll(resourceTypeMap.getValue());
                            resourceList.addAll(resourceTypeMap.getValue());
                        }

                        userResourceList.put(type, resourceList);
                    }
                }
                userReadMap.put(username, userResourceList);

                if (logger.isDebugEnabled()) {
                    logger.debug("userReadMap {}", userReadMap);
                }
            }
        }
        if (isDebugEnabled) {
            logger.debug("Returning Map for {} :: {}", principalType, userReadMap);
        }
        return userReadMap;
    }
}
