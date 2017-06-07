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

package com.hortonworks.beacon.authorize;

/**
 * This class extends Exception class and shall be used for authorization module exception.
 */

public class BeaconAuthorizationException extends Exception {
    private static final long serialVersionUID = 1L;

    public BeaconAuthorizationException(String message) {
        super(message);
    }

    public BeaconAuthorizationException(String message, Throwable exception) {
        super(message, exception);
    }

    public BeaconAuthorizationException(String message, Throwable exception, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, exception, enableSuppression, writableStackTrace);
    }

    public BeaconAuthorizationException(BeaconAccessRequest request) {
        super("Unauthorized Request : " + request);
    }
}
