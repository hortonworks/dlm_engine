package com.hortonworks.beacon.metrics;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.HiveActionType;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

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
        ParseHiveQueryLog parseHiveQueryLog = new ParseHiveQueryLog();
        parseHiveQueryLog.parseQueryLog(dump, HiveActionType.EXPORT);
        Assert.assertEquals(parseHiveQueryLog.getCompleted(), 3);
        Assert.assertEquals(parseHiveQueryLog.getTotal(), 4);
    }

}