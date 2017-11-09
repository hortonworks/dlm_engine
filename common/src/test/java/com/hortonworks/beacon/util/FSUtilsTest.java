/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
