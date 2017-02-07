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

package com.hortonworks.beacon.replication.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility Class for Hive Repl Status.
 */
public final class HiveDRUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HiveDRUtils.class);

    private HiveDRUtils() {}

    public static String getReplDump(String database, String from, String to, String batch) {

        StringBuilder replDump = new StringBuilder();

        replDump.append("REPL DUMP ").append(database);

      /*  if (from != 0L) {
            replDump.append(from);
        }
        if (to != 0L) {
            replDump.append(to);
        }
        if (batch != 0L) {
            replDump.append(batch);
        } */

        LOG.info("Created Repl DUMP String : {}", replDump.toString());
        return replDump.toString();
    }


    public static String getReplLoad(String database, String dumpDirectory) {
        StringBuilder replLoad = new StringBuilder();

        replLoad.append("REPL LOAD ").append(database).append(" FROM ").
                append("'").append(dumpDirectory).append("'");
        LOG.info("Repl LOAD to String:"+replLoad.toString());
        return replLoad.toString();
    }

    public static String getReplStatus(String database) {
        StringBuilder replStatus = new StringBuilder();

        replStatus.append("REPL STATUS ").append(database);

        return replStatus.toString();
    }
}
