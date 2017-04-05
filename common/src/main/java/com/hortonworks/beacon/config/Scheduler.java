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

package com.hortonworks.beacon.config;

/**
 * Configuration parameter related to beacon scheduler.
 */
public class Scheduler {
    private String quartzPrefix;
    private String quartzThreadPool;

    public void copy(Scheduler o) {
        setQuartzPrefix(o.getQuartzPrefix());
        setQuartzThreadPool(o.getQuartzThreadPool());
    }

    public String getQuartzPrefix() {
        return quartzPrefix;
    }

    public void setQuartzPrefix(String quartzPrefix) {
        this.quartzPrefix = quartzPrefix;
    }

    public String getQuartzThreadPool() {
        return quartzThreadPool;
    }

    public void setQuartzThreadPool(String quartzThreadPool) {
        this.quartzThreadPool = quartzThreadPool;
    }
}
