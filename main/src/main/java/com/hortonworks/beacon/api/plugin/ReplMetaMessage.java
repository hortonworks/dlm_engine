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

/**
 * A message sent between plugins of the same type, usually target to source.  Beacon does not
 * understand the content of the message.  This is provided so that target plugins have a way to
 * pass information to their source counterparts without needing to build their own communication
 * channels.
 *
 * Messages are versioned to handle passing messages between systems with different versions.  It
 * is expected that at a minimum messages of version x can be handled by systems of version x +- 1.
 *
 * The system uses Jackson to (de)serialize extensions of this class.  So all extensions must
 * have a public no-arguments constructor.  In most cases (de)serialization should just work with
 * no extra effort on the part of the extending class.  If special treatment is needed than
 * Jackson annotations can be used.
 */
public class ReplMetaMessage extends ReplMessage {

  /**
   * Intended for use only by Jackson.
   */
  public ReplMetaMessage() {
    super();
  }

  /**
   * Create a replication metadata message.
   * @param replicationType type of message, will determine what plugin on the other side
   *                        processes this message.
   * @param bodyVersion version of the body of this message.  This is used to figure out which
   *                    version of the subclass should be instantiated on the target side.
   */
  protected ReplMessage(ReplType replicationType, int bodyVersion) {
    super(replicationType, bodyVersion);
  }
}
