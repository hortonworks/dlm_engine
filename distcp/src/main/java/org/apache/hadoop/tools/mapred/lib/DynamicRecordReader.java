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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The DynamicRecordReader is used in conjunction with the DynamicInputFormat
 * to implement the "Worker pattern" for DistCp.
 * The DynamicRecordReader is responsible for:
 * 1. Presenting the contents of each chunk to DistCp's mapper.
 * 2. Acquiring a new chunk when the current chunk has been completely consumed,
 *    transparently.
 */
public class DynamicRecordReader<K, V> extends RecordReader<K, V> {
  private static final Log LOG = LogFactory.getLog(DynamicRecordReader.class);
  private TaskAttemptContext taskAttemptContext;
  private Configuration configuration;
  private DynamicInputChunk<K, V> chunk;
  private TaskID taskId;

  // Data required for progress indication.
  private int numRecordsPerChunk; // Constant per job.
  private int totalNumRecords;    // Constant per job.
  private int numRecordsProcessedByThisMap = 0;
  private long timeOfLastChunkDirScan = 0;
  private boolean isChunkDirAlreadyScanned = false;
  private DynamicInputChunkContext<K, V> chunkContext;

  private static long TIME_THRESHOLD_FOR_DIR_SCANS = TimeUnit.MINUTES.toMillis(5);

  DynamicRecordReader(DynamicInputChunkContext<K, V> chunkContext) {
    this.chunkContext = chunkContext;
  }

  /**
   * Implementation for RecordReader::initialize(). Initializes the internal
   * RecordReader to read from chunks.
   * @param inputSplit The InputSplit for the map. Ignored entirely.
   * @param taskAttemptContext The AttemptContext.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext)
                         throws IOException, InterruptedException {
    numRecordsPerChunk = DynamicInputFormat.getNumEntriesPerChunk(
            taskAttemptContext.getConfiguration());
    this.taskAttemptContext = taskAttemptContext;
    configuration = taskAttemptContext.getConfiguration();
    taskId = taskAttemptContext.getTaskAttemptID().getTaskID();
    chunk = chunkContext.acquire(this.taskAttemptContext);
    timeOfLastChunkDirScan = System.currentTimeMillis();
    isChunkDirAlreadyScanned = false;

    totalNumRecords = getTotalNumRecords();

  }

  private int getTotalNumRecords() {
    return DistCpUtils.getInt(configuration,
                              DistCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS);
  }

  /**
   * Implementation of RecordReader::nextValue().
   * Reads the contents of the current chunk and returns them. When a chunk has
   * been completely exhausted, an new chunk is acquired and read,
   * transparently.
   * @return True, if the nextValue() could be traversed to. False, otherwise.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public boolean nextKeyValue()
      throws IOException, InterruptedException {

    if (chunk == null) {
      if (LOG.isDebugEnabled())
        LOG.debug(taskId + ": RecordReader is null. No records to be read.");
      return false;
    }

    if (chunk.getReader().nextKeyValue()) {
      ++numRecordsProcessedByThisMap;
      return true;
    }

    if (LOG.isDebugEnabled())
      LOG.debug(taskId + ": Current chunk exhausted. " +
                         " Attempting to pick up new one.");

    chunk.release();
    timeOfLastChunkDirScan = System.currentTimeMillis();
    isChunkDirAlreadyScanned = false;

    chunk = chunkContext.acquire(taskAttemptContext);

    if (chunk == null) return false;

    if (chunk.getReader().nextKeyValue()) {
      ++numRecordsProcessedByThisMap;
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * Implementation of RecordReader::getCurrentKey().
   * @return The key of the current record. (i.e. the source-path.)
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public K getCurrentKey()
      throws IOException, InterruptedException {
    return chunk.getReader().getCurrentKey();
  }

  /**
   * Implementation of RecordReader::getCurrentValue().
   * @return The value of the current record. (i.e. the target-path.)
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public V getCurrentValue()
      throws IOException, InterruptedException {
    return chunk.getReader().getCurrentValue();
  }

  /**
   * Implementation of RecordReader::getProgress().
   * @return A fraction [0.0,1.0] indicating the progress of a DistCp mapper.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public float getProgress()
      throws IOException, InterruptedException {
    final int numChunksLeft = getNumChunksLeft();
    if (numChunksLeft < 0) {// Un-initialized. i.e. Before 1st dir-scan.
      assert numRecordsProcessedByThisMap <= numRecordsPerChunk
              : "numRecordsProcessedByThisMap:" + numRecordsProcessedByThisMap +
                " exceeds numRecordsPerChunk:" + numRecordsPerChunk;
      return ((float) numRecordsProcessedByThisMap) / totalNumRecords;
      // Conservative estimate, till the first directory scan.
    }

    return ((float) numRecordsProcessedByThisMap)
            /(numRecordsProcessedByThisMap + numRecordsPerChunk*numChunksLeft);
  }

  private int getNumChunksLeft() throws IOException {
    long now = System.currentTimeMillis();
    boolean tooLongSinceLastDirScan
                  = now - timeOfLastChunkDirScan > TIME_THRESHOLD_FOR_DIR_SCANS;

    if (tooLongSinceLastDirScan
            || (!isChunkDirAlreadyScanned &&
                    numRecordsProcessedByThisMap%numRecordsPerChunk
                              > numRecordsPerChunk/2)) {
      chunkContext.getListOfChunkFiles();
      isChunkDirAlreadyScanned = true;
      timeOfLastChunkDirScan = now;
    }

    return chunkContext.getNumChunksLeft();
  }
  /**
   * Implementation of RecordReader::close().
   * Closes the RecordReader.
   * @throws IOException
   */
  @Override
  public void close()
      throws IOException {
    if (chunk != null)
        chunk.close();
  }
}
