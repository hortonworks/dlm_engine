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

/**
 * Fetch a BeaconSource object that the source system can use to write data for replication.
 */
public class BeaconSourceFactory {

    private static BeaconSourceFactory self = null;

    public static synchronized BeaconSourceFactory get() {
        if (self == null) {
            self = new BeaconSourceFactory();
        }
        return self;
    }

    /**
     * Get a new BeacondSource instance
     * @return BeacondSource
     */
    public BeaconSource newBeacondSource() {
        // TODO - based on the configuration return the appropriate type of replication generator.
        return null;
    }
}
