/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package org.apache.hadoop.tools.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.tools.mapred.CopyMapper.FileAction;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class TestRetriableFileCopyCommand {
  @SuppressWarnings("rawtypes")
  @Test
  public void testFailOnCloseError() throws Exception {
    Mapper.Context context = mock(Mapper.Context.class);
    doReturn(new Configuration()).when(context).getConfiguration();

    Exception expectedEx = new IOException("boom");
    OutputStream out = mock(OutputStream.class);
    doThrow(expectedEx).when(out).close();

    File f = File.createTempFile(this.getClass().getSimpleName(), null);
    f.deleteOnExit();
    FileStatus stat =
        new FileStatus(1L, false, 1, 1024, 0, new Path(f.toURI()));

    Exception actualEx = null;
    try {
      new RetriableFileCopyCommand("testFailOnCloseError", FileAction.OVERWRITE)
        .copyBytes(stat, 0, out, 512, context);
    } catch (Exception e) {
      actualEx = e;
    }
    assertNotNull("close didn't fail", actualEx);
    assertEquals(expectedEx, actualEx);
  }
}
