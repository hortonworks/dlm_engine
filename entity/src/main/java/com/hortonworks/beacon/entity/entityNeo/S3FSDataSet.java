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

package com.hortonworks.beacon.entity.entityNeo;

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.S3Operation;
import com.hortonworks.beacon.entity.S3OperationFactory;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.BeaconCredentialProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hortonworks.beacon.util.FSUtils.merge;

/**
 * S3 based implementation of HCFS data set.
 */
public class S3FSDataSet extends HCFSDataset {
    public static final Logger LOG = LoggerFactory.getLogger(S3FSDataSet.class);

    public S3FSDataSet(String path, ReplicationPolicy policy) throws BeaconException {
        super(path, policy);
    }

    @Override
    public String resolvePath(String path, ReplicationPolicy policy) {
        return path.replaceFirst(CloudCred.Provider.AWS.getScheme(),
                CloudCred.Provider.AWS.getHcfsScheme());
    }

    @Override
    protected Configuration getHadoopConf(String path, ReplicationPolicy policy) throws BeaconException {
        Configuration conf = super.getHadoopConf(path, policy);

        BeaconCloudCred cloudCred = new BeaconCloudCred(policy.getCloudCred());
        Configuration bucketConf = getBucketEndpointConf(path, cloudCred);
        merge(conf, bucketConf);
        return conf;
    }

    private boolean isBucketEndPointConfAvailable(String path) {
        Configuration defaultConf = new Configuration();
        String bucket = getBucketName(path);
        String bucketEndPointKey = getBucketEndpointConfKey(bucket);
        return StringUtils.isNotEmpty(defaultConf.getTrimmed(bucketEndPointKey));
    }

    private String getBucketEndpointConfKey(String bucket) {
        return String.format(BeaconConstants.AWS_BUCKET_ENDPOINT, bucket);
    }

    public Configuration getBucketEndpointConf(String path, BeaconCloudCred cloudCred) throws BeaconException {
        Configuration conf = new Configuration(false);
        if (isBucketEndPointConfAvailable(path)) {
            return conf;
        }

        S3Operation s3Operation;
        switch (cloudCred.getAuthType()) {
            case AWS_ACCESSKEY:
                String credentialProviderPath = cloudCred.getHadoopCredentialPath();
                BeaconCredentialProvider beaconCredentialProvider = new BeaconCredentialProvider(
                        credentialProviderPath);
                String accessKey = beaconCredentialProvider.resolveAlias(CloudCred.Config.AWS_ACCESS_KEY
                        .getHadoopConfigName());
                String secretKey = beaconCredentialProvider.resolveAlias(CloudCred.Config.AWS_SECRET_KEY
                        .getHadoopConfigName());
                s3Operation = S3OperationFactory.getINSTANCE().createS3Operation(accessKey, secretKey);
                break;

            case AWS_INSTANCEPROFILE:
                s3Operation = S3OperationFactory.getINSTANCE().createS3Operation();
                break;
            default:
                throw new BeaconException("AuthType {} not supported.", cloudCred.getAuthType());
        }
        String bucketName = getBucketName(path);
        String bucketEndPoint = s3Operation.getBucketEndPoint(bucketName);
        String bucketEndPointConfKey = getBucketEndpointConfKey(bucketName);
        LOG.debug("Path: {}, Conf Key: {} Bucket Endpoint: {}", path, bucketEndPointConfKey, bucketEndPoint);
        conf.set(bucketEndPointConfKey, bucketEndPoint);
        return conf;
    }

    private String getBucketName(String path) {
        return new Path(path).toUri().getHost();
    }
}
