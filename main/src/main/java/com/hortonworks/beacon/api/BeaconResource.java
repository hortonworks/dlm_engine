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

package com.hortonworks.beacon.api;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;

import javax.print.attribute.standard.MediaPrintableArea;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Properties;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("/api/beacon")
public class BeaconResource {

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */

    private HashMap<String, Cluster> map;

    @GET
    @Path("hello")
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "Hello Beacon";
    }

    @POST
    @Path("cluster/submit/{cluster-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public int submit(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request)
        throws IOException {
        Properties properties = new Properties();
        properties.load(request.getInputStream());
        YamlReader reader = new YamlReader(new InputStreamReader(request.getInputStream()));
        Object clusterInfo = reader.read(Cluster.class);
        System.out.println("Clusterinfo = " + clusterInfo);
        return 0;
    }

    @GET
    @Path("cluster/list/{cluster-name}")
    @Produces({MediaType.TEXT_PLAIN})
    public int list(@PathParam("cluster-name") String clusterName, @Context HttpServletRequest request)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        YamlWriter writer = new YamlWriter(new OutputStreamWriter(baos));
        writer.write(map.get(clusterName));
        return 0;
    }
}

