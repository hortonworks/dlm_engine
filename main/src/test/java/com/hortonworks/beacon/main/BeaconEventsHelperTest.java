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
package com.hortonworks.beacon.main;

import com.hortonworks.beacon.api.BeaconEventsHelper;
import junit.framework.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Test class for BeaconEventsHelper.
 */
public class BeaconEventsHelperTest {

    public static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
    }

    @Test
    public void testGetStartDate() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class[] cArg = new Class[4];
        cArg[0] = String.class;
        cArg[1] = Date.class;
        cArg[2] = int.class;
        cArg[3] = int.class;

        Method method = BeaconEventsHelper.class.getDeclaredMethod("getStartDate", cArg);
        method.setAccessible(true);
        Object[] argObjects = new Object[4];
        argObjects[0] = null;
        Date now = new Date();
        argObjects[1] = now;
        argObjects[2] = 300; // DEFAULT_FREQUENCY_IN_SECOND = 300
        argObjects[3] = 10;  // default number of results
        Date result = (Date)method.invoke(null, argObjects);
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        long hours = getDateDiff(result, now, TimeUnit.HOURS);
        //when start, end, numResults and offset parameters have default/empty values (null, null, 10, 0)
        //then please return results from at least the past 24 hours
        Assert.assertTrue(hours >= 24);


    }
}
