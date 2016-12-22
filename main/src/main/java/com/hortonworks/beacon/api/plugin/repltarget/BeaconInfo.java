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
package com.hortonworks.beacon.api.plugin.repltarget;

import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ExecutorService;

/**
 * Information about the Beacon engine, for use by Plugins.
 */
public class BeaconInfo {
  private final ExecutorService executorService;
  private final Configuration conf;
  private final Set<ReplicationPolicy> activePolicies;

  /**
   *
   * @param executorService thread pool the plugin should use to spawn new threads.
   * @param conf Configuration file for Beacon.
   * @param policies A set holding the current valid replication policies.  This will not be
   *                 copied.  It is assumed to be a reference to the master set, so that as
   *                 beacon adds and subtracts from the set this stays in sync.
   */
  public BeaconInfo(ExecutorService executorService, Configuration conf,
                    Set<ReplicationPolicy> policies) {
    this.executorService = executorService;
    this.conf = conf;
    this.policies = policies;
  }

  /**
   * Get the thread pool to use if the plugin needs to spawn threads.
   * @return thread pool
   */
  public ExecutorService getExecutorService() {
    return executorService;
  }

  /**
   * Get the configuration for this Beacon instance.
   * @return conf
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Get the replication policies currently in effect.
   * @return
   */
  public Set<ReplicationPolicy> getActivePolicies() {
    return activePolicies;
  }
}
