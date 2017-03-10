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


package com.hortonworks.beacon.plugin;

/**
 * This defines the plugin information that the plugin provides.
 */
public interface PluginInfo {

    /**
     * Return the plugin name.
     *
     * @return name
     */
    String getName();

    /**
     * Return the plugin version.
     *
     * @return version
     */
    String getVersion();

    /**
     * Return the plugin description.   This can be more descriptive as to the plugin operations etc
     *
     * @return description
     */
    String getDescription();

    /**
     * Dependencies of the plugin as a comma separated components in the format component-version.
     * Currently just logged by the plugin manager on discovery.
     *
     * @return dependencies.
     */
    String getDependencies();

    /**
     * Return the absolute path of the plugin staging directory.   This must be owned and managed by the
     * plugin.
     *
     * @return staging dir
     */
    String getStagingDir();

    /**
     * Return true if job should continue running other actions on plugin failure else return false.
     *
     * @return ignore failures or not
     */
    boolean ignoreFailures();
}
