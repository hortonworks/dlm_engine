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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.ambari.view.URLStreamProvider;
import org.apache.ambari.view.ViewContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a class used to bridge the communication between the falcon-ui
 * and the falcon API executing inside ambari.
 */
public class FalconProxyImpersonator {
	private static final Logger LOG = LoggerFactory.getLogger(FalconProxyImpersonator.class);
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
        boolean userNameExists=false;
        while (it.hasNext()) {
            String key = it.next();
            List<String> values = parameters.get(key);
            Iterator<String> it2 = values.iterator();
            if ("user.name".equals(key)){
            	userNameExists=true;
            }
            if (i == 0) {
                serviceURI += "?" + key + "=" + it2.next();
            } else {
                serviceURI += "&" + key + "=" + it2.next();
            }
            i++;
        }
        if (!userNameExists){
        	String queryChar=parameters.size()>1?"&":"?";
        	serviceURI=serviceURI +=queryChar+"user.name="+viewContext.getUsername();
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
    private Response consumeService(HttpHeaders headers, String urlToRead, String method, String xml) throws Exception {
  	  Response response;
        URLStreamProvider streamProvider = viewContext.getURLStreamProvider();
       
        InputStream stream;
        Map<String, String> newHeaders = getHeaders(headers);
        newHeaders.put("user.name", viewContext.getUsername());
       
        if (checkForceJsonRepsonse(urlToRead,newHeaders)){
      	  newHeaders.put("Accept", MediaType.APPLICATION_JSON);
        }
        LOG.error("Falcon urlllll="+urlToRead);
        stream = streamProvider.readFrom(urlToRead, method, xml, newHeaders);
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
            LOG.error(e.getMessage(),e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage(),e);
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
    private boolean checkForceJsonRepsonse(String urlToRead, Map<String, String> headers) throws Exception {
        for(int i=0; i<FORCE_JSON_RESPONSE.length; i++){
          if (urlToRead.contains(FORCE_JSON_RESPONSE[i])) {
            return true;
          }
        }
        return false;
    }
    public Map<String,String> getHeaders(HttpHeaders headers){
    	MultivaluedMap<String, String> requestHeaders = headers.getRequestHeaders();
    	Set<Entry<String, List<String>>> headerEntrySet = requestHeaders.entrySet();
    	HashMap<String,String> headersMap=new HashMap<String,String>();
    	for(Entry<String, List<String>> headerEntry:headerEntrySet){
    		String key = headerEntry.getKey();
    		List<String> values = headerEntry.getValue();
    		headersMap.put(key,strJoin(values,","));
    	}
    	return headersMap;
    }
    public String strJoin(List<String> strings, String separator) {//TODO use one of libraries.
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0, il = strings.size(); i < il; i++) {
            if (i > 0){
                stringBuilder.append(separator);
            }
            stringBuilder.append(strings.get(i));
        }
        return stringBuilder.toString();
    }
}
