/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.api.plugin.repltarget;

import com.hortonworks.beacon.api.plugin.ReplMessage;
import com.hortonworks.beacon.api.plugin.ReplType;

import java.util.HashMap;
import java.util.Map;

/**
 * Information about the plugins needed to plugin to Beacon.
 */
public class PluginInfo {
    private final ReplType replType;
    private final Class<? extends ReplMessage> defaultMessageClass;
    private Map<Integer, Class<? extends ReplMessage>> messageClasses;

    /**
     * @param replType            Replication type this plugin will service.
     * @param defaultMessageClass Default class to use for deserializing messages of the above
     *                            ReplType.  This will be used if there are no matching entries for
     *                            the body version in the messageClasses map.  If all versions of
     *                            the message can be deserialized into the same class there is no
     *                            need to add any additional entries in the messageClasses map.
     */
    public PluginInfo(ReplType replType,
                      Class<? extends ReplMessage> defaultMessageClass) {
        this.replType = replType;
        this.defaultMessageClass = defaultMessageClass;
        messageClasses = new HashMap<>();
    }

    public ReplType getReplType() {
        return replType;
    }

    public Class<? extends ReplMessage> getDefaultMessageClass() {
        return defaultMessageClass;
    }

    /**
     * Add a class to deserialize messages that is version specific.  If the class can handle all
     * versions then it should be passed in the constructor and there's no need to add it here.
     *
     * @param version      version of the message to use this class for
     * @param messageClass class to deserialize this version of message with
     * @return this
     */
    public PluginInfo addMessageClass(int version, Class<? extends ReplMessage> messageClass) {
        messageClasses.put(version, messageClass);
        return this;
    }

    /**
     * Get any version specific classes to use in deserializing messages.
     *
     * @param version version of the message that needs deserialized
     * @return class to deserialize it with
     */
    public Class<? extends ReplMessage> getMessageClass(int version) {
        return messageClasses.get(version);
    }
}
