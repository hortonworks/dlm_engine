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
package org.apache.falcon.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Test for {@link StartupProperties}
 */
public class StartupPropertiesTest {
    @Test
    public void testWindowsUriConversion() {
        if (!StartupProperties.WINDOWS) { // test only on Windows
            return;
        }
        String[][] paths = new String[][]{
            // key name, input value, expected value
            {"normalpath", "file:/a", "file:/a"},
            {"windowspath1", "file://d:\\a", "file:///d:/a"},
            {"windowspath2", "file://d:\\a\\", "file:///d:/a/"},
            {"windowspath2", "file://d:\\a\\", "file:///d:/a/"},
            {"windowspath3", "file://d:\\a/b/c", "file:///d:/a/b/c"},
        };
        Properties properties = new Properties();
        for (String[] path : paths) {
            properties.setProperty(path[0], path[1]);
        }
        StartupProperties.fixWindowsUris(properties);

        // Check to ensure the URIs are fixed correctly
        for (String[] path : paths) {
          Assert.assertEquals(properties.get(path[0]), path[2]);
        }
    }
}
