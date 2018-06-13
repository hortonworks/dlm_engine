/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools;

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
        conf.set(DistCpConstants.DISTCP_EXCLUDE_FILE_REGEX,
                "\\/.*_COPYING$|^.*\\/\\.[^\\/]*$|\\/_temporary$|\\/\\_temporary\\/|.*/\\.Trash\\/.*");
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
                {"/user/bar/.Trash/foo", false},
                {"/user/bar/.Trash/foo/.Trash/bar1", false},
                {"/user/bar/.Trash/.Trash/foo", false},
                {"/test/temporary", true},
                {"/user/foo/bar", true},
                {"/mapred/test/_temp/_staging/_", true},
                {"/mapred/_temporarytemporary", true},
                {"/mapred/temporary.test", true},
                {"/mapred/test._temporary", true},
                {"/hive/test_temporary", true},
                {"/user/bar/Trash/foo", true},
                {"/user/bar/.TrashSkip/foo", true},
                {"/user/bar/NoTrash.Trash/foo", true},
    };
    }

}

