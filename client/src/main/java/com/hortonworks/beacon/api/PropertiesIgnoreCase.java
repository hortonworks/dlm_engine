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

package com.hortonworks.beacon.api;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Class to implement java util properties IgnoreCase.
 */
@Provider
@Produces({MediaType.TEXT_PLAIN})
@Consumes({MediaType.TEXT_PLAIN})
public class PropertiesIgnoreCase extends Properties implements MessageBodyReader<PropertiesIgnoreCase>,
        MessageBodyWriter<PropertiesIgnoreCase> {

    public PropertiesIgnoreCase(InputStream inputStream) throws IOException {
        super();
        load(inputStream);
    }

    public PropertiesIgnoreCase() {
        super();
    }

    public PropertiesIgnoreCase(String propertiesString) throws IOException {
        super();
        load(new StringReader(propertiesString));
    }

    @Override
    public synchronized Object put(Object key, Object value) {
        if (key != null && value != null) {
            return super.put(key.toString(), value.toString());
        }
        throw new IllegalStateException("Key/value = null is not supported, key: " + key + ", value: " + value);
    }

    public Object putIfNotNull(String key, Object value) {
        if (value != null) {
            return put(key, value);
        }
        return null;
    }

    @Override
    public String getProperty(String key) {
        if (key != null) {
            return super.getProperty(key);
        }
        throw new IllegalStateException("Key = null is not supported!");
    }


    public String getPropertyIgnoreCase(String key) {
        String value = getProperty(key);
        if (value == null) {
            value = getProperty(key.toLowerCase());
        }
        if (value != null) {
            return value;
        }

        // Not matching with the actual key/lower case then
        Set<Entry<Object, Object>> s = entrySet();
        Iterator<Entry<Object, Object>> it = s.iterator();
        while (it.hasNext()) {
            Entry<Object, Object> entry = it.next();
            if (key.equalsIgnoreCase((String) entry.getKey())) {
                return (String) entry.getValue();
            }
        }
        return null;
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return PropertiesIgnoreCase.class.isAssignableFrom(type);
    }

    @Override
    public PropertiesIgnoreCase readFrom(Class<PropertiesIgnoreCase> type, Type genericType, Annotation[] annotations,
                                         MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
                                         InputStream entityStream) throws IOException, WebApplicationException {
        return new PropertiesIgnoreCase(entityStream);
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return PropertiesIgnoreCase.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(PropertiesIgnoreCase propertiesIgnoreCase, Class<?> type, Type genericType,
                        Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(PropertiesIgnoreCase properties, Class<?> type, Type genericType,
                        Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
                        OutputStream entityStream) throws WebApplicationException, IOException {
        properties.store(entityStream, "");
    }
}
