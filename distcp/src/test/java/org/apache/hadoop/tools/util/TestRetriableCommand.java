/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package org.apache.hadoop.tools.util;

import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestRetriableCommand {

  private static class MyRetriableCommand extends RetriableCommand {

    private int succeedAfter;
    private int retryCount = 0;

    public MyRetriableCommand(int succeedAfter) {
      super("MyRetriableCommand");
      this.succeedAfter = succeedAfter;
    }

    public MyRetriableCommand(int succeedAfter, RetryPolicy retryPolicy) {
      super("MyRetriableCommand", retryPolicy);
      this.succeedAfter = succeedAfter;
    }

    @Override
    protected Object doExecute(Object... arguments) throws Exception {
      if (++retryCount < succeedAfter)
        throw new Exception("Transient failure#" + retryCount);
      return 0;
    }
  }

  @Test
  public void testRetriableCommand() {
    try {
      new MyRetriableCommand(5).execute(0);
      Assert.assertTrue(false);
    }
    catch (Exception e) {
      Assert.assertTrue(true);
    }


    try {
      new MyRetriableCommand(3).execute(0);
      Assert.assertTrue(true);
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }

    try {
      new MyRetriableCommand(5, RetryPolicies.
          retryUpToMaximumCountWithFixedSleep(5, 0, TimeUnit.MILLISECONDS)).execute(0);
      Assert.assertTrue(true);
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
  }
}
