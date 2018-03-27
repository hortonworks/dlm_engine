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

package com.hortonworks.beacon.util;

import com.hortonworks.beacon.exceptions.BeaconException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test class for the Filesystem utilities.
 */
public class FSUtilsTest{

    @BeforeClass
    private void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.s3n.awsAccessKeyId", "testS3KeyId");
        conf.set("fs.s3n.awsSecretAccessKey", "testS3AccessKey");
        conf.set("fs.azure.account.key.mystorage.blob.core.windows.net", "dGVzdEF6dXJlQWNjZXNzS2V5");
        FSUtils.setDefaultConf(conf);
    }

    @Test(expectedExceptions = BeaconException.class,
                    expectedExceptionsMessageRegExp = "filePath cannot be null or empty")
    public void testIsHCFSEmptyPath() throws Exception {
        FSUtils.isHCFS(null);
    }

    @Test
    public void testIsHCFS() throws Exception {
        boolean isHCFSPath = FSUtils.isHCFS(new Path("/apps/dr"));
        Assert.assertFalse(isHCFSPath);

        isHCFSPath = FSUtils.isHCFS(new Path("hdfs://localhost:54136/apps/dr"));
        Assert.assertFalse(isHCFSPath);

        isHCFSPath = FSUtils.isHCFS(new Path("s3n://testBucket/apps/dr"));
        Assert.assertTrue(isHCFSPath);

        isHCFSPath = FSUtils.isHCFS(new Path("wasb://replication-test@mystorage.blob.core.windows.net/apps/dr"));
        Assert.assertTrue(isHCFSPath);
    }
}
