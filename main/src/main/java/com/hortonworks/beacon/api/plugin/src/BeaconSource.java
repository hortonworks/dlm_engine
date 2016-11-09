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
package com.hortonworks.beacon.api.plugin.src;

import com.hortonworks.beacon.api.plugin.repltarget.ReplTarget;

/**
 * A plugin for the source side of replication.  Beacon will provide the implementation of this.
 * Data creators can then log any new data or data changes to this plugin.  Beacon does not
 * expect to be able to understand the contents of the message, except for the
 * {@link com.hortonworks.beacon.api.plugin.ReplType}.  The message will be decoded by the
 * appropriate {@link ReplTarget} on the target side.
 */
public abstract class BeaconSource {

  /**
   * Write a message to the queue.
   * @param message to replicate.
   * @throws BeaconException if something goes wrong.
   */
  public abstract void replicate(ReplMessage message) throws BeaconException;
}
