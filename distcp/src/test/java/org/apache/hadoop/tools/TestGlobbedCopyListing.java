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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.security.Credentials;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestGlobbedCopyListing {

  private static MiniDFSCluster cluster;

  private static final Credentials CREDENTIALS = new Credentials();

  public static Map<String, String> expectedValues = new HashMap<String, String>();

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new MiniDFSCluster.Builder(new Configuration()).build();
    createSourceData();
  }

  private static void createSourceData() throws Exception {
    mkdirs("/tmp/source/1");
    mkdirs("/tmp/source/2");
    mkdirs("/tmp/source/2/3");
    mkdirs("/tmp/source/2/3/4");
    mkdirs("/tmp/source/5");
    touchFile("/tmp/source/5/6");
    mkdirs("/tmp/source/7");
    mkdirs("/tmp/source/7/8");
    touchFile("/tmp/source/7/8/9");
  }

  private static void mkdirs(String path) throws Exception {
    FileSystem fileSystem = null;
    try {
      fileSystem = cluster.getFileSystem();
      fileSystem.mkdirs(new Path(path));
      recordInExpectedValues(path);
    }
    finally {
      IOUtils.cleanup(null, fileSystem);
    }
  }

  private static void touchFile(String path) throws Exception {
    FileSystem fileSystem = null;
    DataOutputStream outputStream = null;
    try {
      fileSystem = cluster.getFileSystem();
      outputStream = fileSystem.create(new Path(path), true, 0);
      recordInExpectedValues(path);
    }
    finally {
      IOUtils.cleanup(null, fileSystem, outputStream);
    }
  }

  private static void recordInExpectedValues(String path) throws Exception {
    FileSystem fileSystem = cluster.getFileSystem();
    Path sourcePath = new Path(fileSystem.getUri().toString() + path);
    expectedValues.put(sourcePath.toString(), DistCpUtils.getRelativePath(
        new Path("/tmp/source"), sourcePath));
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testRun() throws Exception {
    final URI uri = cluster.getFileSystem().getUri();
    final String pathString = uri.toString();
    Path fileSystemPath = new Path(pathString);
    Path source = new Path(fileSystemPath.toString() + "/tmp/source");
    Path target = new Path(fileSystemPath.toString() + "/tmp/target");
    Path listingPath = new Path(fileSystemPath.toString() + "/tmp/META/fileList.seq");
    DistCpOptions options = new DistCpOptions(Arrays.asList(source), target);
    options.setTargetPathExists(false);
    new GlobbedCopyListing(new Configuration(), CREDENTIALS).buildListing(listingPath, options);

    verifyContents(listingPath);
  }

  private void verifyContents(Path listingPath) throws Exception {
    SequenceFile.Reader reader = new SequenceFile.Reader(cluster.getFileSystem(),
                                              listingPath, new Configuration());
    Text key   = new Text();
    CopyListingFileStatus value = new CopyListingFileStatus();
    Map<String, String> actualValues = new HashMap<String, String>();
    while (reader.next(key, value)) {
      if (value.isDirectory() && key.toString().equals("")) {
        // ignore root with empty relPath, which is an entry to be
        // used for preserving root attributes etc.
        continue;
      }
      actualValues.put(value.getPath().toString(), key.toString());
    }

    Assert.assertEquals(expectedValues.size(), actualValues.size());
    for (Map.Entry<String, String> entry : actualValues.entrySet()) {
      Assert.assertEquals(entry.getValue(), expectedValues.get(entry.getKey()));
    }
  }
}
