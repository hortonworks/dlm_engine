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
 *  WorkRequest{@literal <T>} is a simple container for items of class T and its
 *  corresponding retry counter that indicates how many times this item
 *  was previously attempted to be processed.
 */
public class WorkRequest<T> {
  private int retry;
  private T item;

  public WorkRequest(T item) {
    this(item, 0);
  }

  /**
   *  @param  item   Object representing WorkRequest input data.
   *  @param  retry  Number of previous attempts to process this work request.
   */
  public WorkRequest(T item, int retry) {
    this.item = item;
    this.retry = retry;
  }

  public T getItem() {
    return item;
  }

  /**
   *  @return  Number of previous attempts to process this work request.
   */
  public int getRetry() {
    return retry;
  }
}
