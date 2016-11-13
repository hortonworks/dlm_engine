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
 * A message sent through the replication system.  This has some basic pieces everyone will need,
 * but it is expected that plugins will extend this class with content of their own.
 *
 * Replication messages can have associated {@link ReplEventInfo} data to allow
 * grouping and sequencing on the target.  This data will have been passed to the
 * source system by the requester when it is relevant.
 *
 * Messages are versioned to handle passing messages between systems with different versions.
 * Messages of version n can decode versions n-1.  Version n+1 will be handled as if it were
 * version n (ie, changes should be forward compatible for at least one version).
 *
 * Replication messages contain the {@link ReplType} they are intended for.  This is used by
 * Beacon to determine which plugin to pass the message to.
 *
 * The system uses Jackson to (de)serialize extensions of this class.  So all extensions must
 * have a public no-argument constructor.
 */
public abstract class ReplMessage {

    // Non-static so that it's picked up by Jackson.
    private final int headerVersion = 1;  // Version of this header data
    private ReplEventInfo eventInfo;
    private ReplType type;
    protected int bodyVersion; // Version of the subclass extending this class

    /**
     * Intended for use only by Jackson when instantiating a new ReplMessage.
     */
    public ReplMessage() {
    }

    /**
     * Create a replication message that is not part of a larger event.
     * @param replicationType type of message, will determine what plugin on the target side
     *                        processes this message.
     * @param bodyVersion version of the body of this message.  This is used to figure out which
     *                    version of the subclass should be instantiated on the target side.
     */
    protected ReplMessage(ReplType replicationType, int bodyVersion) {
      this(replicationType, null, bodyVersion);
    }

    /**
     * Create a replication message that is part of a larger event, such as bootstrapping.
     * @param replicationType type of message, will determine what plugin on the target side
     *                        processes this message.
     * @param eventInfo information for the event this message is associated with
     * @param bodyVersion version of the body of this message.  This is used to figure out which
     *                    version of the subclass should be instantiated on the target side.
     */
    protected ReplMessage(ReplType replicationType, ReplEventInfo eventInfo, int bodyVersion) {
        this.eventInfo = eventInfo;
        this.type = replicationType;
        this.bodyVersion = bodyVersion;
    }

    public int getHeaderVersion() {
        return headerVersion;
    }

    public ReplEventInfo getEventInfo() {
        return eventInfo;
    }

    public ReplType getReplicationType() {
        return type;
    }
}
