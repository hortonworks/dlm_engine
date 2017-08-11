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
 * Test the case "-rdiff s2 s1 tgt tgt".
 */
public class TestDistCpSyncReverseFromTarget
  extends TestDistCpSyncReverseBase {
  /*
   * Initialize the source path to /target.
   * @see org.apache.hadoop.tools.TestDistCpSyncReverseBase#initSourcePath()
   */
  @Override
  void initSourcePath() {
    setSource(new Path("/target"));
    setSrcNotSameAsTgt(false);
  }
}
