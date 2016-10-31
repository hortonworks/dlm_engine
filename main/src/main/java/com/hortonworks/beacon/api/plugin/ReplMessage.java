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
 * A message sent through the replication system.  This has some basic pieces everyone will need,
 * but the payload of the message is opaque to Beacon.
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
 * The payload of the replication message is carried via an object contained in
 * this replication message.  While this class is Writable, there is no need for the contained
 * object to be Writable.  Rather it is assumed that the object can be serialized and
 * deserialized via a standard Jackson ObjectMapper.    This means that it must have
 * a no-argument constructor.  It is assumed that the class of the object is in the classpath on
 * the target side so that an instance of the object can be instantiated.
 */
public class ReplMessage<T> implements Writable {

    // Non-static so that it's picked up by Jackson.
    private final int version = 1;
    private ReplEventInfo eventInfo;
    private ReplType type;
    private T message;

    /**
     * Intended for use only by Jackson when instantiating a new ReplMessage.
     */
    public ReplMessage() {
    }

    /**
     * Create a replication message that is not part of a group.
     * @param replicationType type of message, will determine what plugin on the target side
     *                        processes this message.
     * @param message Message data to replicate.  This is opaque to Beacon and will be
     *                interpreted on the other side by the appropriate plugin.
     */
    public ReplMessage(ReplType replicationType, T message) {
      this(replicationType, null, message);
    }

    /**
     * Create a replication message that is part of a group.
     * @param replicationType type of message, will determine what plugin on the target side
     *                        processes this message.
     * @param eventInfo information for the event this message is associated with
     * @param message Message data to replicate.  This is opaque to Beacon and will be
     *                interpreted on the other side by the appropriate plugin.
     */
    public ReplMessage(ReplType replicationType, ReplEventInfo eventInfo, T message) {
        this.eventInfo = eventInfo;
        this.type = replicationType;
        this.message = message;
    }

    public int getVersion() {
        return version;
    }

    public ReplEventInfo getEventInfo() {
        return eventInfo;
    }

    public ReplType getReplicationType() {
        return type;
    }

    public T getMessage() {
        return message;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
