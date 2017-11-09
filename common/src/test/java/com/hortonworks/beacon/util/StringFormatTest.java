/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.util;

import org.junit.Assert;
import org.testng.annotations.Test;

/**
 * StringFormatUtilsTest Test class to test StringFormatUtil functionality.
 */
public class StringFormatTest {

    @Test
    public void testFormatException() throws Exception {
        String msg = StringFormat.format("{} string format.", "test");
        Assert.assertEquals(msg, "test string format.");
    }
}
