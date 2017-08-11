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

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class TestRegexCopyFilter {

  @Test
  public void testShouldCopyTrue() {
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile("user"));

    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    Path shouldCopyPath = new Path("/user/bar");
    Assert.assertTrue(regexCopyFilter.shouldCopy(shouldCopyPath));
  }

  @Test
  public void testShouldCopyFalse() {
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile(".*test.*"));

    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    Path shouldNotCopyPath = new Path("/user/testing");
    Assert.assertFalse(regexCopyFilter.shouldCopy(shouldNotCopyPath));
  }

  @Test
  public void testShouldCopyWithMultipleFilters() {
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile(".*test.*"));
    filters.add(Pattern.compile("/user/b.*"));
    filters.add(Pattern.compile(".*_SUCCESS"));

    List<Path> toCopy = getTestPaths();

    int shouldCopyCount = 0;

    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    for (Path path: toCopy) {
      if (regexCopyFilter.shouldCopy(path)) {
        shouldCopyCount++;
      }
    }

    Assert.assertEquals(2, shouldCopyCount);
  }

  @Test
  public void testShouldExcludeAll() {
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile(".*test.*"));
    filters.add(Pattern.compile("/user/b.*"));
    filters.add(Pattern.compile(".*"));           // exclude everything

    List<Path> toCopy = getTestPaths();

    int shouldCopyCount = 0;

    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    for (Path path: toCopy) {
      if (regexCopyFilter.shouldCopy(path)) {
        shouldCopyCount++;
      }
    }

    Assert.assertEquals(0, shouldCopyCount);
  }

  private List<Path> getTestPaths() {
    List<Path> toCopy = new ArrayList<>();
    toCopy.add(new Path("/user/bar"));
    toCopy.add(new Path("/user/foo/_SUCCESS"));
    toCopy.add(new Path("/hive/test_data"));
    toCopy.add(new Path("test"));
    toCopy.add(new Path("/user/foo/bar"));
    toCopy.add(new Path("/mapred/.staging_job"));
    return toCopy;
  }

}
