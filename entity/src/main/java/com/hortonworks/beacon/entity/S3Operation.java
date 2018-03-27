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

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * S3 related operation class.
 */
public final class S3Operation {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconCloudCred.class);

    private AmazonS3Client amazonS3Client;

    private static final String REGEX = "Cannot create enum from (.*) value!";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    public S3Operation() {
        amazonS3Client = new AmazonS3Client();
    }

    public S3Operation(String accessKey, String secretKey) {
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        amazonS3Client = new AmazonS3Client(awsCredentials);
    }

    public String getBucketEndPoint(String bucketName) throws BeaconException, URISyntaxException {
        String regionName;
        try {
            regionName = amazonS3Client.getBucketLocation(bucketName);
        } catch (IllegalArgumentException e) {
            String message = e.getMessage();
            Matcher matcher = PATTERN.matcher(message);
            if (matcher.find()) {
                regionName = matcher.group(1);
            } else {
                throw new ValidationException(message);
            }
        } catch (AmazonClientException e) {
            throw new BeaconException(S3AUtils.translateException("Get Bucket location", bucketName, e));
        }
        LOG.debug("Bucket {} location: {}", bucketName, regionName);
        return getBucketRegionEndPoint(regionName);
    }

    private String getBucketRegionEndPoint(String regionName) {
        StringBuilder regionEndPoint = new StringBuilder();
        if (regionName.equalsIgnoreCase("US")) {
            regionName = Regions.US_EAST_1.getName();
        } else if (regionName.equalsIgnoreCase("EU")) {
            regionName = Regions.EU_WEST_1.getName();
        }
        // s3.<region-name>.amazonaws.com
        regionEndPoint.append("s3.").append(regionName).append(".amazonaws.com");
        String cnRegionName = "cn";
        if (regionName.startsWith(cnRegionName)) {
            regionEndPoint.append(cnRegionName);
        }
        return regionEndPoint.toString();
    }
}
