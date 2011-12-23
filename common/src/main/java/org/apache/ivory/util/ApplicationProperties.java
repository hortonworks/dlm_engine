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

package org.apache.ivory.util;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class ApplicationProperties extends Properties {

  private static Logger LOG = Logger.getLogger(ApplicationProperties.class);

  protected abstract String getPropertyFile();

  protected ApplicationProperties() throws IOException {
    loadProperties();
  }

  protected void loadProperties() throws IOException {
    String propertyFile = getPropertyFile();

    InputStream resource = getClass().getResourceAsStream("/" + propertyFile);

    if (resource == null) {
      throw new FileNotFoundException(propertyFile +
          " not found in class path");
    } else {
      //TODO:: Should we clear and reload? In which case we need to lock
      //TODO:: down the object.
      LOG.info("Loading properties from " + propertyFile);
      load(resource);
    }
  }

}
