package com.hortonworks.beacon.util;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FSUtilsTest {

    @BeforeClass
    private void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.s3n.awsAccessKeyId", "testS3KeyId");
        conf.set("fs.s3n.awsSecretAccessKey", "testS3AccessKey");
        conf.set("fs.azure.account.key.mystorage.blob.core.windows.net", "dGVzdEF6dXJlQWNjZXNzS2V5");
        FSUtils.setConf(conf);
    }

    @Test(expectedExceptions = BeaconException.class, expectedExceptionsMessageRegExp = "filePath cannot be empty")
    public void testIsHCFSEmptyPath() throws Exception {
        FSUtils.isHCFS(null);
    }

    @Test
    public void testIsHCFSInTestMode() throws Exception {
        BeaconConfig.getInstance().getEngine().setInTestMode(true);
        boolean isHCFSPath = FSUtils.isHCFS(new Path("/apps/dr"));
        Assert.assertFalse(isHCFSPath);

        isHCFSPath = FSUtils.isHCFS(new Path("hdfs://localhost:54136/apps/dr"));
        Assert.assertFalse(isHCFSPath);

        isHCFSPath = FSUtils.isHCFS(new Path("s3n://testBucket/apps/dr"));
        Assert.assertTrue(isHCFSPath);

        isHCFSPath = FSUtils.isHCFS(new Path("wasb://replication-test@mystorage.blob.core.windows.net/apps/dr"));
        Assert.assertTrue(isHCFSPath);
    }

    @Test
    public void testIsHCFSInNonTestMode() throws Exception {
        boolean isHCFSPath = FSUtils.isHCFS(new Path("hdfs://localhost:54136/apps/dr"));
        Assert.assertFalse(isHCFSPath);

        isHCFSPath = FSUtils.isHCFS(new Path("s3n://testBucket/apps/dr"));
        Assert.assertTrue(isHCFSPath);

        isHCFSPath = FSUtils.isHCFS(new Path("wasb://replication-test@mystorage.blob.core.windows.net/apps/dr"));
        Assert.assertTrue(isHCFSPath);
    }

}
