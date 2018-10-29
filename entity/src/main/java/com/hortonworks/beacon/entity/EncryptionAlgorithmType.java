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

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.PolicyHelper;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

import static com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields.CLOUD_ENCRYPTIONALGORITHM;
import static com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields.CLOUD_ENCRYPTIONKEY;

/**
 * Cloud Object store encryption algorithm types.
 */
public enum EncryptionAlgorithmType {
    AWS_SSES3("AES256", "fs.s3a.bucket.%s.server-side-encryption-algorithm"),
    AWS_SSEKMS("SSE-KMS", "fs.s3a.bucket.%s.server-side-encryption-algorithm"),
    NONE("NONE", "fs.s3a.bucket.%s.server-side-encryption-algorithm");

    private final String name;
    private final String confName;

    EncryptionAlgorithmType(String name, String confName) {
        this.name = name;
        this.confName = confName;
    }

    public String getConfValue() {
        return name;
    }

    public String getConfName() {
        return confName;
    }

    public String getName() {
        return name;
    }

    public static EncryptionAlgorithmType getEncryptionAlgorithm(final String name) throws BeaconException {
        if (StringUtils.isEmpty(name)) {
            return NONE;
        }

        //Try value of
        try {
            return EncryptionAlgorithmType.valueOf(name);
        } catch (IllegalArgumentException ex) {
            //ignore
        }

        //Try by name
        EncryptionAlgorithmType[] algorithmTypes = EncryptionAlgorithmType.values();
        for (EncryptionAlgorithmType algorithmType: algorithmTypes) {
            if (algorithmType.getName().equalsIgnoreCase(name)) {
                return algorithmType;
            }
        }
        throw new BeaconException("Encryption algorithm {} is not supported", name);
    }

    private String getActualConfName(String argument) {
        return String.format(getConfName(), argument);
    }

    private static final String AWS_SSEKMSKEY = "fs.s3a.bucket.%s.server-side-encryption.key";
    private static final String AWS_SSES3KEY = "fs.s3a.bucket.%s.server-side-encryption-algorithm";

    public static Configuration getHadoopConf(ReplicationPolicy policy, Path cloudPath)
            throws BeaconException {
        Configuration conf = new Configuration(false);
        if (policy.getCloudEncryptionAlgorithm() != null) {
            EncryptionAlgorithmType encryptionAlgorithm = PolicyHelper.getCloudEncryptionAlgorithm(policy);
            String bucketName = cloudPath.toUri().getAuthority();
            conf.set(encryptionAlgorithm.getActualConfName(bucketName), encryptionAlgorithm.getConfValue());

            switch (encryptionAlgorithm) {
                case AWS_SSEKMS:
                    String confName = String.format(AWS_SSEKMSKEY, bucketName);
                    String encryptionKey = PolicyHelper.getCloudEncryptionKey(policy);
                    conf.set(confName, encryptionKey);
                    break;
                case AWS_SSES3:
                    String awsSSES3AlgoConfig = String.format(AWS_SSES3KEY, bucketName);
                    conf.set(awsSSES3AlgoConfig, encryptionAlgorithm.getName());
                    break;
                default:
                    throw new BeaconException(StringFormat.format(
                            "EncryptionAlgorithm {} not supported", encryptionAlgorithm));
            }
        }
        return conf;
    }

    public static void validate(ReplicationPolicy policy) throws BeaconException {
        Properties cloudEncProps = new Properties();

        if (policy.getCloudEncryptionAlgorithm() != null) {
            cloudEncProps.put(CLOUD_ENCRYPTIONALGORITHM.getName(), policy.getCloudEncryptionAlgorithm());
        }

        if (policy.getCloudEncryptionKey() != null) {
            cloudEncProps.put(CLOUD_ENCRYPTIONKEY.getName(), policy.getCloudEncryptionKey());
        }
        // When a sourceDataset is on Cloud, beacon doesn't need an encryption key and hence that is not mandatory.
        boolean isKeyMandatory = true;
        if (PolicyHelper.isDatasetHCFS(policy.getSourceDataset())) {
            isKeyMandatory = false;
        }
        validateEncryptionAlgorithmType(cloudEncProps, isKeyMandatory);
    }

    private static void validateEncryptionAlgorithmType(Properties cloudEncProps, boolean isKeyMandatory)
            throws ValidationException {
        String encryptionAlgorithm = cloudEncProps.getProperty(CLOUD_ENCRYPTIONALGORITHM.getName());
        String encryptionKey = cloudEncProps.getProperty(CLOUD_ENCRYPTIONKEY.getName());
        if (StringUtils.isEmpty(encryptionAlgorithm)) {
            if (StringUtils.isNotEmpty(encryptionKey)) {
                throw new ValidationException(
                        "Cloud Encryption key without a cloud encryption algorithm is not allowed");
            }
            return;
        }
        try {
            EncryptionAlgorithmType encryptionAlgorithmType = EncryptionAlgorithmType.valueOf(encryptionAlgorithm);
            switch (encryptionAlgorithmType) {
                case AWS_SSEKMS:
                    if (StringUtils.isEmpty(encryptionKey) && isKeyMandatory) {
                        throw new ValidationException(
                                "Cloud Encryption key is mandatory with this cloud encryption algorithm");
                    }
                    break;
                default:
                    if (StringUtils.isNotEmpty(encryptionKey)) {
                        throw new ValidationException(
                                "Cloud encryption key is not applicable to this cloud encryption algorithm",
                                encryptionAlgorithm);
                    }
                    break;
            }
        } catch (IllegalArgumentException iaEx) {
            throw new ValidationException("Cloud encryption algorithm {} is not supported", encryptionAlgorithm, iaEx);
        }
    }

}
