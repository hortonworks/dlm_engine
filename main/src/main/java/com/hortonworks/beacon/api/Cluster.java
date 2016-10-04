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

package com.hortonworks.beacon.api;

import java.util.Properties;

/**
 * Created by vranganathan on 9/28/16.
 */
public class Cluster {
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNnUri() {
        return nnUri;
    }

    public void setNnUri(String nnUri) {
        this.nnUri = nnUri;
    }

    public String getHs2Uri() {
        return hs2Uri;
    }

    public void setHs2Uri(String hs2Uri) {
        this.hs2Uri = hs2Uri;
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    String name;
    String nnUri;
    String hs2Uri;
    String description;
    String version;

    Properties props;
    Properties tags;
}
