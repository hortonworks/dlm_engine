/*
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

package org.apache.ivory.entity.parser;

import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.log4j.Logger;

public class ClusterEntityParser extends EntityParser<Cluster> {

    private static final Logger LOG = Logger.getLogger(ProcessEntityParser.class);

    private static final String SCHEMA_FILE = "/schema/cluster/cluster-0.1.xsd";

    protected ClusterEntityParser() {
        super(EntityType.CLUSTER, SCHEMA_FILE);
    }

    @Override
    public void validate(Cluster entity) throws StoreAccessException, ValidationException {
        //TODO
    }
}
