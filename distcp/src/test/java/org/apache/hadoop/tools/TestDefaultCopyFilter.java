package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestDefaultCopyFilter {

    @Test
    public void testShouldExcludeDefault() throws Exception {
        List<Path> toCopy = getTestPaths();

        int shouldCopyCount = 0;

        DefaultFilter defaultFilter = new DefaultFilter(new Configuration());

        for (Path path: toCopy) {
            if (defaultFilter.shouldCopy(path)) {
                shouldCopyCount++;
            }
        }
        Assert.assertEquals(5, shouldCopyCount);
    }

    private List<Path> getTestPaths() {
        List<Path> toCopy = new ArrayList<>();
        toCopy.add(new Path("/user/bar/_temporary/_temporary"));
        toCopy.add(new Path("/user/bar/_temporary/1/_temporary"));
        toCopy.add(new Path("/user/foo/._WIP_testing"));
        toCopy.add(new Path("/hive/test_temporary"));
        toCopy.add(new Path("/test/temporary"));
        toCopy.add(new Path("/user/foo/bar"));
        toCopy.add(new Path("/mapred/.staging_job"));
        return toCopy;
    }

}

