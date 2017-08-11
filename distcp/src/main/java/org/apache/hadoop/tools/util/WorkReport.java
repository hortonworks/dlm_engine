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

/**
 *  WorkReport{@literal <T>} is a simple container for items of class T and its
 *  corresponding retry counter that indicates how many times this item
 *  was previously attempted to be processed.
 */
public class WorkReport<T> {
  private T item;
  private final boolean success;
  private final int retry;
  private final Exception exception;

  /**
   *  @param  item       Object representing work report.
   *  @param  retry      Number of unsuccessful attempts to process work.
   *  @param  success    Indicates whether work was successfully completed.
   */
  public WorkReport(T item, int retry, boolean success) {
    this(item, retry, success, null);
  }

  /**
   *  @param  item       Object representing work report.
   *  @param  retry      Number of unsuccessful attempts to process work.
   *  @param  success    Indicates whether work was successfully completed.
   *  @param  exception  Exception thrown while processing work.
   */
  public WorkReport(T item, int retry, boolean success, Exception exception) {
    this.item = item;
    this.retry = retry;
    this.success = success;
    this.exception = exception;
  }

  public T getItem() {
    return item;
  }

  /**
   *  @return True if the work was processed successfully.
   */
  public boolean getSuccess() {
    return success;
  }

  /**
   *  @return  Number of unsuccessful attempts to process work.
   */
  public int getRetry() {
    return retry;
  }

  /**
   *  @return  Exception thrown while processing work.
   */
  public Exception getException() {
    return exception;
  }
}
