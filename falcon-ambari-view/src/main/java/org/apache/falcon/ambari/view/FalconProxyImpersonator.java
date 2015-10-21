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
package org.apache.falcon.ambari.view;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.ambari.view.*;

/**
 * This is a class used to bridge the communication between the falcon-ui
 * and the falcon API executing inside ambari.
 */
public class FalconProxyImpersonator {

    private ViewContext viewContext;

    private static final String SERVICE_URI_PROP = "falcon.service.uri";
    private static final String DEFAULT_SERVICE_URI = "http://sandbox.hortonworks.com:15000";

    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";
    private static final String DELETE_METHOD = "DELETE";

    private static final String FALCON_ERROR = "<result><status>FAILED</status>";
    private static final String[] FORCE_JSON_RESPONSE = {"/entities/list/", "admin/version"};

    /**
     * Constructor to get the default viewcontext.
     * @param viewContext
     */
    @Inject
    public FalconProxyImpersonator(ViewContext viewContext) {
        this.viewContext = viewContext;
    }

    /**
     * Method to set the ambari user.
     * @param headers
     * @param ui
     * @return
     */
    @GET
    @Path("/")
    public Response setUser(@Context HttpHeaders headers, @Context UriInfo ui) {
        String result;
        try {
            result = viewContext.getUsername();
            return Response.ok(result).type(defineType(result)).build();
        } catch (Exception ex) {
            ex.printStackTrace();
            result = ex.toString();
            return Response.status(Response.Status.BAD_REQUEST).entity(result).build();
        }
    }

    /**
     * Method to attend all the GET calls.
     * @param headers
     * @param ui
     * @return
     */
    @GET
    @Path("/{path: .*}")
    public Response getUsage(@Context HttpHeaders headers, @Context UriInfo ui) {
        String result;
        try {
            String serviceURI = buildURI(ui);
            return consumeService(headers, serviceURI, GET_METHOD, null);
        } catch (Exception ex) {
            ex.printStackTrace();
            result = ex.toString();
            return Response.status(Response.Status.BAD_REQUEST).entity(result).build();
        }
    }

    /**
     * Method to attend all the POST calls.
     * @param xml
     * @param headers
     * @param ui
     * @return
     * @throws IOException
     */
    @POST
    @Path("/{path: .*}")
    public Response handlePost(String xml, @Context HttpHeaders headers, @Context UriInfo ui)
        throws IOException {
        String result;
        try {
            String serviceURI = buildURI(ui);
            return consumeService(headers, serviceURI, POST_METHOD, xml);
        } catch (Exception ex) {
            ex.printStackTrace();
            result = ex.toString();
            return Response.status(Response.Status.BAD_REQUEST).entity(result).build();
        }
    }

    /**
     * Method to attend all the DELETE calls.
     * @param headers
     * @param ui
     * @return
     * @throws IOException
     */
    @DELETE
    @Path("/{path: .*}")
    public Response handleDelete(@Context HttpHeaders headers, @Context UriInfo ui) throws IOException {
        String result;
        try {
            String serviceURI = buildURI(ui);
            return consumeService(headers, serviceURI, DELETE_METHOD, null);
        } catch (Exception ex) {
            ex.printStackTrace();
            result = ex.toString();
            return Response.status(Response.Status.BAD_REQUEST).entity(result).build();
        }
    }

    /**
     * Method set the parametters and cast them to a String.
     * @param ui
     * @return
     */
    private String buildURI(UriInfo ui) {

        String uiURI = ui.getAbsolutePath().getPath();
        int index = uiURI.indexOf("proxy/") + 5;
        uiURI = uiURI.substring(index);

        String serviceURI = viewContext.getProperties().get(SERVICE_URI_PROP) != null ? viewContext
                .getProperties().get(SERVICE_URI_PROP) : DEFAULT_SERVICE_URI;
        serviceURI += uiURI;

        MultivaluedMap<String, String> parameters = ui.getQueryParameters();
        Iterator<String> it = parameters.keySet().iterator();
        int i = 0;
        while (it.hasNext()) {
            String key = it.next();
            List<String> values = parameters.get(key);
            Iterator<String> it2 = values.iterator();
            if (i == 0) {
                serviceURI += "?" + key + "=" + it2.next();
            } else {
                serviceURI += "&" + key + "=" + it2.next();
            }
            i++;
        }
        return serviceURI;
    }

    /**
     * Method to consume the API from the URLStreamProvider.
     * @param headers
     * @param urlToRead
     * @param method
     * @param xml
     * @return
     * @throws Exception
     */
    public Response consumeService(HttpHeaders headers, String urlToRead, String method, String xml) throws Exception {

        Response response;

        URLStreamProvider streamProvider = viewContext.getURLStreamProvider();
        String name = viewContext.getUsername();
        Map<String, String> newHeaders = new HashMap();
        newHeaders.put("user.name", name);
        InputStream stream;

        if (method.equals(POST_METHOD)) {
            newHeaders.put("Accept", MediaType.APPLICATION_JSON);
            newHeaders.put("Content-type", "text/xml");
            stream = streamProvider.readFrom(urlToRead, method, xml, newHeaders);
        } else if (method.equals(DELETE_METHOD)) {
            newHeaders.put("Accept", MediaType.APPLICATION_JSON);
            stream = streamProvider.readFrom(urlToRead, method, (String)null, newHeaders);
        } else {
            newHeaders = checkIfDefinition(urlToRead, newHeaders);
            stream = streamProvider.readFrom(urlToRead, method, (String)null, newHeaders);
        }

        String sresponse = getStringFromInputStream(stream);

        if (sresponse.contains(FALCON_ERROR) || sresponse.contains(Response.Status.BAD_REQUEST.name())) {
            response = Response.status(Response.Status.BAD_REQUEST).
                    entity(sresponse).type(MediaType.TEXT_PLAIN).build();
        } else {
            return Response.status(Response.Status.OK).entity(sresponse).type(defineType(sresponse)).build();
        }

        return response;
    }

    /**
     * Method to read the response and send it to the front.
     * @param is
     * @return
     */
    private String getStringFromInputStream(InputStream is) {

        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();

    }

    /**
     * Method to cast the response type.
     * @param response
     * @return
     */
    private String defineType(String response) {
        if (response.startsWith("{")) {
            return MediaType.TEXT_PLAIN;
        } else if (response.startsWith("<")) {
            return MediaType.TEXT_XML;
        } else {
            return MediaType.TEXT_PLAIN;
        }
    }

    /**
     * Method to checks if the requested call needs to be forced to JSON
     * @param urlToRead
     * @param headers
     * @return
     * @throws Exception
     */
    private Map<String, String> checkIfDefinition(String urlToRead, Map<String, String> headers) throws Exception {
      boolean force = false;
      for(int i=0; i<FORCE_JSON_RESPONSE.length; i++){
        if (urlToRead.contains(FORCE_JSON_RESPONSE[i])) {
          force = true;
        }
      }
      if (force) {
        headers.put("Accept", MediaType.APPLICATION_JSON);
      }
      return headers;
    }

}
