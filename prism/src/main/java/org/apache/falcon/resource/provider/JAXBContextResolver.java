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

package org.apache.falcon.resource.provider;

import org.codehaus.jackson.map.ObjectMapper;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * An implementation of Context Resolver for JAXB.
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JAXBContextResolver implements ContextResolver<ObjectMapper> {

    private final ObjectMapper defaultObjectMapper;


    public JAXBContextResolver() {
        defaultObjectMapper = createDefaultMapper();
    }

    private static ObjectMapper createDefaultMapper() {
        final ObjectMapper result = new ObjectMapper();
        // you can customize it here e.g. result.enable(SerializationFeature.INDENT_OUTPUT);
        return result;
    }

    public  ObjectMapper getContext(Class<?> objectType) {
        return defaultObjectMapper;
    }
}
