/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication;

/**
 * Supported Distcp option for Replication.
 */

public enum ReplicationDistCpOption {

    DISTCP_OPTION_OVERWRITE("overwrite"),
    DISTCP_OPTION_IGNORE_ERRORS("ignoreErrors"),
    DISTCP_OPTION_SKIP_CHECKSUM("skipChecksum"),
    DISTCP_OPTION_REMOVE_DELETED_FILES("removeDeletedFiles"),
    DISTCP_OPTION_PRESERVE_BLOCK_SIZE("preserveBlockSize"),
    DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER("preserveReplicationNumber"),
    DISTCP_OPTION_PRESERVE_PERMISSIONS("preservePermission"),
    DISTCP_OPTION_PRESERVE_USER("preserveUser"),
    DISTCP_OPTION_PRESERVE_GROUP("preserveGroup"),
    DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE("preserveChecksumType"),
    DISTCP_OPTION_PRESERVE_ACL("preserveAcl"),
    DISTCP_OPTION_PRESERVE_XATTR("preserveXattr"),
    DISTCP_OPTION_PRESERVE_TIMES("preserveTimes");

    private final String name;

    ReplicationDistCpOption(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
