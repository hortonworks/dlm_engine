/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.authorize.simple;

import com.hortonworks.beacon.authorize.BeaconActionTypes;
import com.hortonworks.beacon.authorize.BeaconResourceTypes;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * This class contains implementation of Parsing policies.
 */

public class PolicyParser {

    private static Logger logger = LoggerFactory.getLogger(PolicyParser.class);
    public static final int POLICYNAME = 0;

    public static final int USER_INDEX = 1;
    public static final int USERNAME = 0;
    public static final int USER_AUTHORITIES = 1;

    public static final int GROUP_INDEX = 2;
    public static final int GROUPNAME = 0;
    public static final int GROUP_AUTHORITIES = 1;

    public static final int RESOURCE_INDEX = 3;
    public static final int RESOURCE_TYPE = 0;
    public static final int RESOURCE_NAME = 1;

    private List<BeaconActionTypes> getListOfAutorities(String auth) {
        List<BeaconActionTypes> authorities = new ArrayList<>();

        for (int i = 0; i < auth.length(); i++) {
            char access = auth.toLowerCase().charAt(i);
            switch (access) {
                case 'r':
                    authorities.add(BeaconActionTypes.READ);
                    break;
                case 'w':
                    authorities.add(BeaconActionTypes.CREATE);
                    break;
                case 'u':
                    authorities.add(BeaconActionTypes.UPDATE);
                    break;
                case 'd':
                    authorities.add(BeaconActionTypes.DELETE);
                    break;

                default:
                    if (logger.isErrorEnabled()) {
                        logger.error("Invalid action: '{}'", access);
                    }
                    break;
            }
        }
        return authorities;
    }

    public List<PolicyDef> parsePolicies(List<String> policies) {
        List<PolicyDef> policyDefs = new ArrayList<>();
        for (String policy : policies) {
            PolicyDef policyDef = parsePolicy(policy);
            if (policyDef != null) {
                policyDefs.add(policyDef);
            }
        }
        logger.debug(StringUtils.join(policyDefs, ", "));
        return policyDefs;
    }

    private PolicyDef parsePolicy(String data) {
        PolicyDef def = null;
        String[] props = data.split(";;");

        if (props.length < RESOURCE_INDEX) {
            logger.warn("Skipping invalid policy line: {}", data);
        } else {
            def = new PolicyDef();
            def.setPolicyName(props[POLICYNAME]);
            parseUsers(props[USER_INDEX], def);
            parseGroups(props[GROUP_INDEX], def);
            parseResources(props[RESOURCE_INDEX], def);
            logger.debug("policy successfully parsed!!!");
        }
        return def;
    }

    private boolean validateEntity(String entity) {
        boolean isValidEntity = Pattern.matches("(.+:.+)+", entity);
        boolean isEmpty = entity.isEmpty();
        if (!isValidEntity || isEmpty) {
            logger.debug("group/user/resource not properly define in Policy");
            return false;
        } else {
            return true;
        }

    }

    private void parseUsers(String usersDef, PolicyDef def) {
        String[] users = usersDef.split(",");
        String[] userAndRole = null;
        Map<String, List<BeaconActionTypes>> usersMap = new HashMap<>();
        if (validateEntity(usersDef)) {
            for (String user : users) {
                if (!Pattern.matches("(.+:.+)+", user)) {
                    continue;
                }
                userAndRole = user.split(":");
                if (def.getUsers() != null) {
                    usersMap = def.getUsers();
                }
                List<BeaconActionTypes> userAutorities = getListOfAutorities(userAndRole[USER_AUTHORITIES]);
                usersMap.put(userAndRole[USERNAME], userAutorities);
                def.setUsers(usersMap);
            }

        } else {
            def.setUsers(usersMap);
        }
    }

    private void parseGroups(String groupsDef, PolicyDef def) {
        String[] groups = groupsDef.split("\\,");
        String[] groupAndRole = null;
        Map<String, List<BeaconActionTypes>> groupsMap = new HashMap<>();
        if (validateEntity(groupsDef.trim())) {
            for (String group : groups) {
                if (!Pattern.matches("(.+:.+)+", group)) {
                    continue;
                }
                groupAndRole = group.split("[:]");
                if (def.getGroups() != null) {
                    groupsMap = def.getGroups();
                }
                List<BeaconActionTypes> groupAutorities = getListOfAutorities(groupAndRole[GROUP_AUTHORITIES]);
                groupsMap.put(groupAndRole[GROUPNAME], groupAutorities);
                def.setGroups(groupsMap);
            }

        } else {
            def.setGroups(groupsMap);
        }
    }

    private void parseResources(String resourceDef, PolicyDef def) {
        String[] resources = resourceDef.split(",");
        String[] resourceTypeAndName = null;
        Map<BeaconResourceTypes, List<String>> resourcesMap = new HashMap<>();
        if (validateEntity(resourceDef)) {
            for (String resource : resources) {
                if (!Pattern.matches("(.+:.+)+", resource)) {
                    continue;
                }
                resourceTypeAndName = resource.split("[:]");
                if (def.getResources() != null) {
                    resourcesMap = def.getResources();
                }
                BeaconResourceTypes resourceType = null;
                String type = resourceTypeAndName[RESOURCE_TYPE].toUpperCase();
                if (type.equalsIgnoreCase("CLUSTER")) {
                    resourceType = BeaconResourceTypes.CLUSTER;
                } else if (type.equalsIgnoreCase("POLICY")) {
                    resourceType = BeaconResourceTypes.POLICY;
                } else if (type.equalsIgnoreCase("SCHEDULE")) {
                    resourceType = BeaconResourceTypes.SCHEDULE;
                } else if (type.equalsIgnoreCase("EVENT")) {
                    resourceType = BeaconResourceTypes.EVENT;
                } else if (type.equalsIgnoreCase("LOGS")) {
                    resourceType = BeaconResourceTypes.LOGS;
                } else {
                    //Log.warn(type + " is invalid resource please check PolicyStore file");
                    continue;
                }

                List<String> resourceList = resourcesMap.get(resourceType);
                if (resourceList == null) {
                    resourceList = new ArrayList<>();
                }
                resourceList.add(resourceTypeAndName[RESOURCE_NAME]);
                resourcesMap.put(resourceType, resourceList);
                def.setResources(resourcesMap);
            }
        } else {
            def.setResources(resourcesMap);
        }
    }
}
