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


