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
    DISTCP_OPTION_UPDATE("update", "update"),
    DISTCP_OPTION_IGNORE_ERRORS("ignoreErrors", "i"),
    DISTCP_OPTION_SKIP_CHECKSUM("skipChecksum", "skipcrccheck"),
    DISTCP_OPTION_REMOVE_DELETED_FILES("removeDeletedFiles", "delete"),
    DISTCP_OPTION_PRESERVE_BLOCK_SIZE("preserveBlockSize", "pb"),
    DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER("preserveReplicationNumber", "pr"),
    DISTCP_OPTION_PRESERVE_PERMISSIONS("preservePermission", "pp"),
    DISTCP_OPTION_PRESERVE_USER("preserveUser", "pu"),
    DISTCP_OPTION_PRESERVE_GROUP("preserveGroup", "pg"),
    DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE("preserveChecksumType", "pc"),
    DISTCP_OPTION_PRESERVE_ACL("preserveAcl", "pa"),
    DISTCP_OPTION_PRESERVE_XATTR("preserveXattr", "px"),
    DISTCP_OPTION_PRESERVE_TIMES("preserveTimes", "pt");

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


