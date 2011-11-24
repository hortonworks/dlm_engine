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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.authorize.BeaconAccessRequest;
import com.hortonworks.beacon.authorize.BeaconActionTypes;
import com.hortonworks.beacon.authorize.BeaconAuthorizationException;
import com.hortonworks.beacon.authorize.BeaconAuthorizer;
import com.hortonworks.beacon.authorize.BeaconResourceTypes;
import com.hortonworks.beacon.config.PropertiesUtil;

/** This class contains simple implementation of BeaconAuthorizer class.
 */

public final class SimpleBeaconAuthorizer implements BeaconAuthorizer {
    /**
     *  Accessor types that can be used.
     *  USER
     *  GROUP
     */
    public enum BeaconAccessorTypes {
        USER, GROUP
    }

    private static final String WILDCARD_ASTERISK = "*";
    private static final String WILDCARDS = "*?";

    private static final Logger LOG = LoggerFactory.getLogger(SimpleBeaconAuthorizer.class);
    private boolean isDebugEnabled = LOG.isDebugEnabled();

    private boolean optIgnoreCase = false;

    private Map<String, Map<BeaconResourceTypes, List<String>>> userReadMap = null;
    private Map<String, Map<BeaconResourceTypes, List<String>>> userWriteMap = null;
    private Map<String, Map<BeaconResourceTypes, List<String>>> userUpdateMap = null;
    private Map<String, Map<BeaconResourceTypes, List<String>>> userDeleteMap = null;
    private Map<String, Map<BeaconResourceTypes, List<String>>> groupReadMap = null;
    private Map<String, Map<BeaconResourceTypes, List<String>>> groupWriteMap = null;
    private Map<String, Map<BeaconResourceTypes, List<String>>> groupUpdateMap = null;
    private Map<String, Map<BeaconResourceTypes, List<String>>> groupDeleteMap = null;

    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();
    private static final String BEACON_AUTH_POLICY_FILE="beacon.authorization.policy.file";
    private static final String BEACON_AUTH_POLICY_DEFAULT_FILE="policy-store.txt";
    public SimpleBeaconAuthorizer() {
    }

    @Override
    public void init() {
        if (isDebugEnabled) {
            LOG.debug("==> SimpleBeaconAuthorizer init");
        }
        try {

            PolicyParser parser = new PolicyParser();
            optIgnoreCase = Boolean.valueOf(AUTHCONFIG.getProperty("optIgnoreCase", "false"));

            if (isDebugEnabled) {
                LOG.debug("Read from PropertiesUtil --> optIgnoreCase :: {}", optIgnoreCase);
            }

            String policyStoreFile=AUTHCONFIG.getProperty(BEACON_AUTH_POLICY_FILE, BEACON_AUTH_POLICY_DEFAULT_FILE);
            InputStream policyStoreStream = AUTHCONFIG.getFileAsInputStream(policyStoreFile);

            List<String> policies = null;
            try {
                policies = FileReaderUtil.readFile(policyStoreStream);
            } catch (Exception ex) {
                LOG.error("SimpleBeaconAuthorizer could not read file due to: {}", ex);
            } finally {
                policyStoreStream.close();
            }
            if (policies!=null) {
                List<PolicyDef> policyDef = parser.parsePolicies(policies);

                userReadMap = PolicyUtil.createPermissionMap(policyDef, BeaconActionTypes.READ,
                        BeaconAccessorTypes.USER);
                userWriteMap = PolicyUtil.createPermissionMap(policyDef, BeaconActionTypes.CREATE,
                        BeaconAccessorTypes.USER);
                userUpdateMap = PolicyUtil.createPermissionMap(policyDef, BeaconActionTypes.UPDATE,
                        BeaconAccessorTypes.USER);
                userDeleteMap = PolicyUtil.createPermissionMap(policyDef, BeaconActionTypes.DELETE,
                        BeaconAccessorTypes.USER);

                groupReadMap = PolicyUtil.createPermissionMap(policyDef, BeaconActionTypes.READ,
                        BeaconAccessorTypes.GROUP);
                groupWriteMap = PolicyUtil.createPermissionMap(policyDef, BeaconActionTypes.CREATE,
                        BeaconAccessorTypes.GROUP);
                groupUpdateMap = PolicyUtil.createPermissionMap(policyDef, BeaconActionTypes.UPDATE,
                        BeaconAccessorTypes.GROUP);
                groupDeleteMap = PolicyUtil.createPermissionMap(policyDef, BeaconActionTypes.DELETE,
                        BeaconAccessorTypes.GROUP);

                if (isDebugEnabled) {
                    LOG.debug("\n\nUserReadMap :: {}\nGroupReadMap :: {}", userReadMap, groupReadMap);
                    LOG.debug("\n\nUserWriteMap :: {}\nGroupWriteMap :: {}", userWriteMap, groupWriteMap);
                    LOG.debug("\n\nUserUpdateMap :: {}\nGroupUpdateMap :: {}", userUpdateMap, groupUpdateMap);
                    LOG.debug("\n\nUserDeleteMap :: {}\nGroupDeleteMap :: {}", userDeleteMap, groupDeleteMap);
                }
            }

        } catch (IOException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("SimpleBeaconAuthorizer could not be initialized properly due to: {}", e);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isAccessAllowed(BeaconAccessRequest request) throws BeaconAuthorizationException {
        LOG.info("==> SimpleBeaconAuthorizer isAccessAllowed");
        if (isDebugEnabled) {
            LOG.debug("isAccessAllowd({})", request);
        }
        String user = request.getUser();
        Set<String> groups = request.getUserGroups();
        BeaconActionTypes action = request.getAction();
        String resource = request.getResource();
        Set<BeaconResourceTypes> resourceTypes = request.getResourceTypes();
        LOG.info("Checking for :: \nUser :: {}\nGroups :: {}\nAction :: {}\nResource :: {}", user, groups, action,
                resource);
        if (isDebugEnabled) {
            LOG.debug("Checking for :: \nUser :: {}\nGroups :: {}\nAction :: {}\nResource :: {}", user, groups,
                action, resource);
        }

        boolean isAccessAllowed = false;
        boolean isUser = user != null;
        boolean isGroup = groups != null;

        if ((!isUser && !isGroup) || action == null || resource == null) {
            if (isDebugEnabled) {
                LOG.debug("Please check the formation BeaconAccessRequest.");
            }
            return isAccessAllowed;
        } else {
            if (isDebugEnabled) {
                LOG.debug("checkAccess for Operation :: {} on Resource {}:{}", action, resourceTypes, resource);
            }
            switch (action) {
                case READ:
                    isAccessAllowed = checkAccess(user, resourceTypes, resource, userReadMap);
                    isAccessAllowed =
                            isAccessAllowed || checkAccessForGroups(groups, resourceTypes, resource, groupReadMap);
                    break;
                case CREATE:
                    isAccessAllowed = checkAccess(user, resourceTypes, resource, userWriteMap);
                    isAccessAllowed =
                            isAccessAllowed || checkAccessForGroups(groups, resourceTypes, resource, groupWriteMap);
                    break;
                case UPDATE:
                    isAccessAllowed = checkAccess(user, resourceTypes, resource, userUpdateMap);
                    isAccessAllowed =
                            isAccessAllowed || checkAccessForGroups(groups, resourceTypes, resource, groupUpdateMap);
                    break;
                case DELETE:
                    isAccessAllowed = checkAccess(user, resourceTypes, resource, userDeleteMap);
                    isAccessAllowed =
                            isAccessAllowed || checkAccessForGroups(groups, resourceTypes, resource, groupDeleteMap);
                    break;
                default:
                    if (isDebugEnabled) {
                        LOG.debug("Invalid Action {}\nRaising BeaconAuthorizationException!!!", action);
                    }
                    throw new BeaconAuthorizationException("Invalid action: '{}'", action);
            }
        }

        if (isDebugEnabled) {
            LOG.debug("<== SimpleBeaconAuthorizer isAccessAllowed = {}", isAccessAllowed);
        }

        return isAccessAllowed;
    }

    private boolean checkAccess(String accessor, Set<BeaconResourceTypes> resourceTypes, String resource,
        Map<String, Map<BeaconResourceTypes, List<String>>> map) {
        if (isDebugEnabled) {
            LOG.debug("==> SimpleBeaconAuthorizer checkAccess");
            LOG.debug("Now checking access for accessor : {}\nResource Types : {}\nResource : {}\nMap : {}",
                accessor, resourceTypes, resource, map);
        }
        boolean result = true;
        Map<BeaconResourceTypes, List<String>> rescMap = map.get(accessor);
        if (rescMap != null) {
            for (BeaconResourceTypes resourceType : resourceTypes) {
                List<String> accessList = rescMap.get(resourceType);
                if (isDebugEnabled) {
                    LOG.debug("\nChecking for resource : {} in list : {}\n", resource, accessList);
                }
                if (accessList != null) {
                    result = result && isMatch(resource, accessList);
                } else {
                    result = false;
                }
            }
        } else {
            result = false;
            if (isDebugEnabled) {
                LOG.debug("Key {} missing. Returning with result : {}", accessor, result);
            }
        }

        if (isDebugEnabled) {
            LOG.debug("Check for {} :: {}", accessor, result);
            LOG.debug("<== SimpleBeaconAuthorizer checkAccess");
        }
        return result;
    }

    private boolean checkAccessForGroups(Set<String> groups, Set<BeaconResourceTypes> resourceType, String resource,
        Map<String, Map<BeaconResourceTypes, List<String>>> map) {
        boolean isAccessAllowed = false;
        if (isDebugEnabled) {
            LOG.debug("==> SimpleBeaconAuthorizer checkAccessForGroups");
        }

        if (CollectionUtils.isNotEmpty(groups)) {
            for (String group : groups) {
                isAccessAllowed = checkAccess(group, resourceType, resource, map);
                if (isAccessAllowed) {
                    break;
                }
            }
        }

        if (isDebugEnabled) {
            LOG.debug("<== SimpleBeaconAuthorizer checkAccessForGroups");
        }
        return isAccessAllowed;
    }

    private boolean resourceMatchHelper(List<String> policyResource) {
        boolean isMatchAny = false;
        if (isDebugEnabled) {
            LOG.debug("==> SimpleBeaconAuthorizer resourceMatchHelper");
        }

        if (policyResource != null) {
            boolean isWildCardPresent = false;
            for (String policyValue : policyResource) {
                if (StringUtils.isEmpty(policyValue)) {
                    continue;
                }
                if (StringUtils.containsOnly(policyValue, WILDCARD_ASTERISK)) {
                    isMatchAny = true;
                } else if (!isWildCardPresent && StringUtils.containsAny(policyValue, WILDCARDS)) {
                    isWildCardPresent = true;
                }
            }
        } else {
            isMatchAny = false;
        }

        if (isDebugEnabled) {
            LOG.debug("<== SimpleBeaconAuthorizer resourceMatchHelper");
        }
        return isMatchAny;
    }

    private boolean isMatch(String resource, List<String> policyValues) {
        if (isDebugEnabled) {
            LOG.debug("==> SimpleBeaconAuthorizer isMatch");
        }
        boolean isMatchAny = resourceMatchHelper(policyValues);
        boolean isMatch = false;
        boolean allValuesRequested = isAllValuesRequested(resource);

        if (allValuesRequested || isMatchAny) {
            isMatch = isMatchAny;
        } else {
            for (String policyValue : policyValues) {
                if (policyValue.contains("*")) {
                    isMatch =
                        optIgnoreCase ? FilenameUtils.wildcardMatch(resource, policyValue, IOCase.INSENSITIVE)
                            : FilenameUtils.wildcardMatch(resource, policyValue, IOCase.SENSITIVE);
                } else {
                    isMatch =
                        optIgnoreCase ? StringUtils.equalsIgnoreCase(resource, policyValue) : StringUtils.equals(
                            resource, policyValue);
                }
                if (isMatch) {
                    break;
                }
            }
        }

        if (!isMatch) {
            if (isDebugEnabled) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (String policyValue : policyValues) {
                    sb.append(policyValue);
                    sb.append(" ");
                }
                sb.append("]");

                LOG.debug("BeaconDefaultResourceMatcher.isMatch returns FALSE, (resource={}, policyValues={})",
                        resource, sb.toString());
            }

        }

        if (isDebugEnabled) {
            LOG.debug("<== SimpleBeaconAuthorizer isMatch({}): {}", resource, isMatch);
        }

        return isMatch;
    }

    private boolean isAllValuesRequested(String resource) {
        return StringUtils.isEmpty(resource) || WILDCARD_ASTERISK.equals(resource);
    }

    @Override
    public void cleanUp() {
        if (isDebugEnabled) {
            LOG.debug("==> +SimpleBeaconAuthorizer cleanUp");
        }
        userReadMap = null;
        userWriteMap = null;
        userUpdateMap = null;
        userDeleteMap = null;
        groupReadMap = null;
        groupWriteMap = null;
        groupUpdateMap = null;
        groupDeleteMap = null;
        if (isDebugEnabled) {
            LOG.debug("<== +SimpleBeaconAuthorizer cleanUp");
        }
    }

    /*
     * NOTE :: This method is added for setting the maps for testing purpose.
     */
    @VisibleForTesting
    public void setResourcesForTesting(Map<String, Map<BeaconResourceTypes, List<String>>> userMap,
        Map<String, Map<BeaconResourceTypes, List<String>>> groupMap, BeaconActionTypes actionTypes) {

        switch (actionTypes) {
            case READ:
                this.userReadMap = userMap;
                this.groupReadMap = groupMap;
                break;

            case CREATE:

                this.userWriteMap = userMap;
                this.groupWriteMap = groupMap;
                break;
            case UPDATE:

                this.userUpdateMap = userMap;
                this.groupUpdateMap = groupMap;
                break;
            case DELETE:

                this.userDeleteMap = userMap;
                this.groupDeleteMap = groupMap;
                break;

            default:
                if (isDebugEnabled) {
                    LOG.debug("No such action available");
                }
                break;
        }
    }

}


