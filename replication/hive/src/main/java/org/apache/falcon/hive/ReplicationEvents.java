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

package org.apache.falcon.hive;

import org.apache.hive.hcatalog.api.repl.Command;

import java.util.List;
import java.util.ListIterator;

/**
 * Replication events class.
 */
public class ReplicationEvents {
    private String dbName;
    private String tableName;
    private List<Command> exportCommands = null;
    private List<Command> importCommands = null;

    public ReplicationEvents(String dbName, String tableName, List<Command> exportCommands,
                             List<Command> importCommands) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.exportCommands = exportCommands;
        this.importCommands = importCommands;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public ListIterator<Command> getExportCommands() {
        return exportCommands.listIterator();
    }

    public ListIterator<Command> getImportCommands() {
        return importCommands.listIterator();
    }

}
