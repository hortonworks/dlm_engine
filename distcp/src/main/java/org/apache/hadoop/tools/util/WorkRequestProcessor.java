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
 *  Interface for ProducerConsumer worker loop.
 *
 */
public interface WorkRequestProcessor<T, R> {

  /**
   * Work processor.
   * The processor should be stateless: that is, it can be repeated after
   * being interrupted.
   *
   * @param   workRequest  Input work item.
   * @return  Outputs WorkReport after processing workRequest item.
   *
   */
  public WorkReport<R> processItem(WorkRequest<T> workRequest);
}
