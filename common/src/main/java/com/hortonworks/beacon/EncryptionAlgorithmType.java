/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon;

/**
 * Cloud Object store encryption algorithm types.
 */
public enum EncryptionAlgorithmType {
    AWS_SSES3("AES256", "fs.s3a.server-side-encryption-algorithm"),
    AWS_SSEKMS("SSE-KMS", "fs.s3a.server-side-encryption-algorithm"),
    NONE("NONE", "fs.s3a.server-side-encryption-algorithm");

    private final String name;
    private final String confName;

    EncryptionAlgorithmType(String name, String confName) {
        this.name = name;
        this.confName = confName;
    }

    public String getName() {
        return name;
    }

    public String getConfName() {
        return confName;
    }
}
