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

package com.hortonworks.beacon.exceptions;

import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.service.Services;

/**
 * Common Exception thrown.
 */
public class BeaconException extends Exception {

    /**
     * @param e Exception
     */
    public BeaconException(Throwable e) {
        super(e);
    }

    public BeaconException(String message, Throwable e, Object...objects) {
        super(((ResourceBundleService) Services.get().getService(ResourceBundleService.get().getName()))
                .getString(message, objects), e);
    }

    /**
     * @param message - custom exception message
     */
    public BeaconException(String message, Object...objects) {
        super(((ResourceBundleService) Services.get().getService(ResourceBundleService.get().getName()))
                .getString(message, objects));
    }

    /**
     *
     */
    private static final long serialVersionUID = -1475818869309247014L;

}
