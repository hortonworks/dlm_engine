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

package com.hortonworks.beacon.client.resource;


import com.hortonworks.beacon.client.entity.Entity;
import org.apache.commons.lang3.StringUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * Entity list used for marshalling / unmarshalling with REST calls.
 */
@XmlRootElement(name = "clusters")
@XmlAccessorType(XmlAccessType.FIELD)
//@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ClusterList {
    public int getTotalResults() {
        return totalResults;
    }

    @XmlElement
    private int totalResults;

    @XmlElement(name = "cluster")
    private final ClusterElement[] elements;

    /**
     * List of fields returned by RestAPI.
     */
    public enum ClusterFieldList {
        NAME, DATACENTER, PEERS, TAGS
    }

    /**
     * Element within an entity.
     */
    public static class ClusterElement {
        //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
        @XmlElement
        public String name;
        @XmlElement
        public String dataCenter;
        @XmlElementWrapper(name = "peers")
        public List<String> peer;
        @XmlElementWrapper(name = "tags")
        public List<String> tag;


        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

        @Override
        public String toString() {
            String outString = name;
            if (StringUtils.isNotEmpty(dataCenter)) {
                outString += "(" + dataCenter + ")";
            }

            if (peer != null && !peer.isEmpty()) {
                outString += " - " + peer.toString();
            }

            if (tag != null && !tag.isEmpty()) {
                outString += " - " + tag.toString();
            }

            outString += "\n";
            return outString;
        }
    }

    //For JAXB
    public ClusterList() {
        this.elements = null;
        this.totalResults = 0;
    }

    public ClusterList(ClusterElement[] elements, int totalResults) {
        this.totalResults = totalResults;
        this.elements = elements;
    }

    public ClusterList(Entity[] elements, int totalResults) {
        this.totalResults = totalResults;
        int len = elements.length;
        ClusterElement[] items = new ClusterElement[len];
        for (int i = 0; i < len; i++) {
            items[i] = createClusterElement(elements[i]);
        }
        this.elements = items;
    }

    private ClusterElement createClusterElement(Entity e) {
        ClusterElement element = new ClusterElement();
        element.name = e.getName();
        element.dataCenter = null;
        element.peer = new ArrayList<String>();
        element.tag = new ArrayList<String>();
        return element;
    }

    public ClusterElement[] getElements() {
        return elements;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(totalResults + "\n");
        for (ClusterElement element : elements) {
            buffer.append(element.toString());
        }
        return buffer.toString();
    }
}
