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

/**
 * A CopyFilter which always returns true.
 *
 */
public class TrueCopyFilter extends CopyFilter {

  @Override
  public boolean shouldCopy(Path path) {
    return true;
  }
}
