/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package org.apache.hadoop.tools;

import com.hortonworks.beacon.constants.BeaconConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestDefaultCopyFilter {

    @Test(dataProvider = "getTestPaths")
    public void testShouldExcludeDefault(String path, boolean shouldCopy) throws Exception {

        boolean isCopied = false;
        Configuration conf = new Configuration();
        conf.set(BeaconConstants.DISTCP_EXCLUDE_FILE_REGEX,
                "\\/.*_COPYING$|^.*\\/\\.[^\\/]*$|\\/_temporary$|\\/\\_temporary\\/");
        DefaultFilter defaultFilter = new DefaultFilter(conf);
        Path filterPath = new Path(path);
        if (defaultFilter.shouldCopy(filterPath)) {
            isCopied = true;
        }
        Assert.assertEquals(isCopied, shouldCopy);
    }

    @DataProvider
    private Object[][] getTestPaths() {
        return new Object[][]{{"/user/bar/_temporary/_temporary", false},
                {"/user/bar/_temporary/1/_temporary", false},
                {"/user/foo/._WIP_testing", false},
                {"/mapred/.staging_job", false},
                {"/mapred/._temporary", false},
                {"/mapred/_COPYING", false},
                {"/test/temporary", true},
                {"/user/foo/bar", true},
                {"/mapred/test/_temp/_staging/_", true},
                {"/mapred/_temporarytemporary", true},
                {"/mapred/temporary.test", true},
                {"/mapred/test._temporary", true},
                {"/hive/test_temporary", true},
    };
    }

}

