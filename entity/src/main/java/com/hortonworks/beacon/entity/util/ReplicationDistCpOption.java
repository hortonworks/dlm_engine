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

/**
 * Supported Distcp option for Replication.
 */

public enum ReplicationDistCpOption {
    DISTCP_OPTION_OVERWRITE("overwrite", "overwrite"),
    DISTCP_OPTION_IGNORE_ERRORS("ignoreErrors", "i"),
    DISTCP_OPTION_SKIP_CHECKSUM("skipChecksum", "skipcrccheck"),
    DISTCP_OPTION_REMOVE_DELETED_FILES("removeDeletedFiles", "delete"),
    DISTCP_OPTION_PRESERVE_BLOCK_SIZE("preserveBlockSize", "b"),
    DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER("preserveReplicationNumber", "r"),
    DISTCP_OPTION_PRESERVE_PERMISSIONS("preservePermission", "p"),
    DISTCP_OPTION_PRESERVE_USER("preserveUser", "u"),
    DISTCP_OPTION_PRESERVE_GROUP("preserveGroup", "g"),
    DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE("preserveChecksumType", "c"),
    DISTCP_OPTION_PRESERVE_ACL("preserveAcl", "a"),
    DISTCP_OPTION_PRESERVE_XATTR("preserveXattr", "x"),
    DISTCP_OPTION_PRESERVE_TIMES("preserveTimes", "t");

    private final String name;
    private final String sName;

    ReplicationDistCpOption(String name, String sName) {
        this.name = name;
        this.sName = sName;
    }

    public String getName() {
        return name;
    }

    public String getSName() {
        return sName;
    }
}


