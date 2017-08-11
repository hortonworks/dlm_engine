/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package org.apache.hadoop.tools.mapred.lib;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.tools.DistCpConstants;

import java.io.IOException;

/**
 * Class to initialize the DynamicInputChunk invariants.
 */
class DynamicInputChunkContext<K, V> {

  private static Log LOG = LogFactory.getLog(DynamicInputChunkContext.class);
  private Configuration configuration;
  private Path chunkRootPath = null;
  private String chunkFilePrefix;
  private FileSystem fs;
  private int numChunksLeft = -1; // Un-initialized before 1st dir-scan.

  public DynamicInputChunkContext(Configuration config)
      throws IOException {
    this.configuration = config;
    Path listingFilePath = new Path(getListingFilePath(configuration));
    chunkRootPath = new Path(listingFilePath.getParent(), "chunkDir");
    fs = chunkRootPath.getFileSystem(configuration);
    chunkFilePrefix = listingFilePath.getName() + ".chunk.";
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Path getChunkRootPath() {
    return chunkRootPath;
  }

  public String getChunkFilePrefix() {
    return chunkFilePrefix;
  }

  public FileSystem getFs() {
    return fs;
  }

  private static String getListingFilePath(Configuration configuration) {
    final String listingFileString = configuration.get(
        DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");
    assert !listingFileString.equals("") : "Listing file not found.";
    return listingFileString;
  }

  public int getNumChunksLeft() {
    return numChunksLeft;
  }

  public DynamicInputChunk acquire(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    String taskId
        = taskAttemptContext.getTaskAttemptID().getTaskID().toString();
    Path acquiredFilePath = new Path(getChunkRootPath(), taskId);

    if (fs.exists(acquiredFilePath)) {
      LOG.info("Acquiring pre-assigned chunk: " + acquiredFilePath);
      return new DynamicInputChunk(acquiredFilePath, taskAttemptContext, this);
    }

    for (FileStatus chunkFile : getListOfChunkFiles()) {
      if (fs.rename(chunkFile.getPath(), acquiredFilePath)) {
        LOG.info(taskId + " acquired " + chunkFile.getPath());
        return new DynamicInputChunk(acquiredFilePath, taskAttemptContext,
            this);
      }
    }
    return null;
  }

  public DynamicInputChunk createChunkForWrite(String chunkId)
      throws IOException {
    return new DynamicInputChunk(chunkId, this);
  }

  public FileStatus [] getListOfChunkFiles() throws IOException {
    Path chunkFilePattern = new Path(chunkRootPath, chunkFilePrefix + "*");
    FileStatus chunkFiles[] = fs.globStatus(chunkFilePattern);
    numChunksLeft = chunkFiles.length;
    return chunkFiles;
  }
}
