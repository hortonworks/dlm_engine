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

import org.apache.falcon.FalconException;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Properties read during application startup.
 */
public final class StartupProperties extends ApplicationProperties {

    private static final Pattern WINDOWS_URI_PATTERN = Pattern.compile("^file://[a-zA-Z]:");

    static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

    private static final String FILE_SCHEME = "file://";

    private static final String PROPERTY_FILE = "startup.properties";

    private static final AtomicReference<StartupProperties> INSTANCE =
            new AtomicReference<StartupProperties>();

    private StartupProperties() throws FalconException {
        fixWindowsUris(this);
    }

    /**
     * Fix any malformed URIs in the configuration due to windows paths.
     */
    static void fixWindowsUris(Properties properties) {
        // startup.properties uses configuration values such as
        // file://${directory}/target. On windows platform this can
        // result in malformed URI. Change URIs like file://d:\path1/path2 to
        // to correct URI file:///d:/path1/path2
        if (!WINDOWS) {
            return;
        }
        // Run through the config values and fix the URIs
        Set<Map.Entry<Object, Object>> entrySet = properties.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            String value = (String) entry.getValue();
            if (WINDOWS_URI_PATTERN.matcher(value).find()) {
                String newValue = FILE_SCHEME + "/" + value.substring(FILE_SCHEME.length()).replace("\\", "/");
                properties.setProperty((String) entry.getKey(), newValue);
            }
        }
    }

    @Override
    protected String getPropertyFile() {
        return PROPERTY_FILE;
    }

    public static Properties get() {
        try {
            if (INSTANCE.get() == null) {
                INSTANCE.compareAndSet(null, new StartupProperties());
            }
            return INSTANCE.get();
        } catch (FalconException e) {
            throw new RuntimeException("Unable to read application " + "startup properties", e);
        }
    }
}
