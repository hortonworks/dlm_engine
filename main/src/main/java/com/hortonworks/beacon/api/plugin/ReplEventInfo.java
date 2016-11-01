/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.beacon.api.plugin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class contains information specific to a particular replication event.  Currently that
 * includes a group id (which is used to group related replication events together) and sequence
 * id (which tells Beacon what order to apply events in a group).
 */
public class ReplEventInfo {
  private final int version = 1; // non-static so Jackson can pick it up
  private long groupId;
  private int sequenceId;

  public ReplEventInfo(long groupId, int sequenceId) {
    this.groupId = groupId;
    this.sequenceId = sequenceId;
  }

  public int getVersion() {
    return version;
  }

  public long getGroupId() {
    return groupId;
  }

  public int getSequenceId() {
    return sequenceId;
  }
}
