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

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class StubContext {

  private StubStatusReporter reporter = new StubStatusReporter();
  private RecordReader<Text, CopyListingFileStatus> reader;
  private StubInMemoryWriter writer = new StubInMemoryWriter();
  private Mapper<Text, CopyListingFileStatus, Text, Text>.Context mapperContext;

  public StubContext(Configuration conf,
      RecordReader<Text, CopyListingFileStatus> reader, int taskId)
      throws IOException, InterruptedException {

    WrappedMapper<Text, CopyListingFileStatus, Text, Text> wrappedMapper
            = new WrappedMapper<Text, CopyListingFileStatus, Text, Text>();

    MapContextImpl<Text, CopyListingFileStatus, Text, Text> contextImpl
            = new MapContextImpl<Text, CopyListingFileStatus, Text, Text>(conf,
            getTaskAttemptID(taskId), reader, writer,
            null, reporter, null);

    this.reader = reader;
    this.mapperContext = wrappedMapper.getMapContext(contextImpl);
  }

  public Mapper<Text, CopyListingFileStatus, Text, Text>.Context getContext() {
    return mapperContext;
  }

  public StatusReporter getReporter() {
    return reporter;
  }

  public RecordReader<Text, CopyListingFileStatus> getReader() {
    return reader;
  }

  public void setReader(RecordReader<Text, CopyListingFileStatus> reader) {
    this.reader = reader;
  }

  public StubInMemoryWriter getWriter() {
    return writer;
  }

  public static class StubStatusReporter extends StatusReporter {

    private Counters counters = new Counters();

    public StubStatusReporter() {
	    /*
      final CounterGroup counterGroup
              = new CounterGroup("FileInputFormatCounters",
                                 "FileInputFormatCounters");
      counterGroup.addCounter(new Counter("BYTES_READ",
                                          "BYTES_READ",
                                          0));
      counters.addGroup(counterGroup);
      */
    }

    @Override
    public Counter getCounter(Enum<?> name) {
      return counters.findCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return counters.findCounter(group, name);
    }

    @Override
    public void progress() {}

    @Override
    public float getProgress() {
      return 0F;
    }

    @Override
    public void setStatus(String status) {}
  }


  public static class StubInMemoryWriter extends RecordWriter<Text, Text> {

    List<Text> keys = new ArrayList<Text>();

    List<Text> values = new ArrayList<Text>();

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
      keys.add(key);
      values.add(value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    }

    public List<Text> keys() {
      return keys;
    }

    public List<Text> values() {
      return values;
    }

  }

  public static TaskAttemptID getTaskAttemptID(int taskId) {
    return new TaskAttemptID("", 0, TaskType.MAP, taskId, 0);
  }
}
