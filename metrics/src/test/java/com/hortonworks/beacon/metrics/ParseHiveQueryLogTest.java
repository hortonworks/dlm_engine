/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.metrics;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.HiveActionType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests hive query log parsing.
 */
public class ParseHiveQueryLogTest {
    @Test
    public void testParseQueryLog() throws BeaconException {

        List<String> dump = new ArrayList<>();
        dump.add("INFO  : REPL::START: {\"dbName\":\"default\",\"dumpType\":\"BOOTSTRAP\",\"estimatedNumTables\":4,"
                + "\"estimatedNumFunctions\":2,\"dumpStartTime\":1504271329}");
        dump.add("INFO  : REPL::FUNCTION_DUMP: {\"dbName\":\"default\",\"functionName\":\"f1\","
                + "\"functionsDumpProgress\":\"1/2\",\"dumpTime\":1504271329}");
        dump.add("INFO  : REPL::FUNCTION_DUMP: {\"dbName\":\"default\",\"functionName\":\"f2\","
                + "\"functionsDumpProgress\":\"2/2\",\"dumpTime\":1504271330}");
        dump.add("INFO  : REPL::TABLE_DUMP: {\"dbName\":\"default\",\"tableName\":\"t1\","
                + "\"tableType\":\"MANAGED_TABLE\",\"tablesDumpProgress\":\"1/4\",\"dumpTime\":1504271331}");
        dump.add("INFO  : REPL::TABLE_DUMP: {\"dbName\":\"default\",\"tableName\":\"t2\","
                + "\"tableType\":\"MANAGED_TABLE\",\"tablesDumpProgress\":\"2/4\",\"dumpTime\":1504271332}");
        dump.add("INFO  : REPL::TABLE_DUMP: {\"dbName\":\"default\",\"tableName\":\"v1\","
                + "\"tableType\":\"VIRTUAL_VIEW\",\"tablesDumpProgress\":\"3/4\",\"dumpTime\":1504271332}");
        ParseHiveQueryLogV2 hiveQueryLog = new ParseHiveQueryLogV2();
        hiveQueryLog.parseQueryLog(dump, HiveActionType.EXPORT);
        Assert.assertEquals(hiveQueryLog.getTotal(), 4);
        Assert.assertEquals(hiveQueryLog.getCompleted(), 3);
    }
}
