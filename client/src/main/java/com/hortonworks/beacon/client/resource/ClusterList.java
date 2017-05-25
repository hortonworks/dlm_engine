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


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Arrays;
import java.util.List;

/**
 * Entity list used for marshalling / unmarshalling with REST calls.
 */
@XmlRootElement(name = "clusters")
@XmlAccessorType(XmlAccessType.FIELD)
//@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ClusterList {

    @XmlElement
    private long totalResults;

    @XmlElement
    private int results;

    @XmlElement(name = "cluster")
    private final ClusterElement[] elements;

    public long getTotalResults() {
        return totalResults;
    }

    public int getResults() {
        return results;
    }
    /**
     * List of fields returned by RestAPI.
     */
    public enum ClusterFieldList {
        NAME, PEERS, TAGS
    }

    /**
     * Element within an entity.
     */
    public static class ClusterElement {
        //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
        @XmlElement
        public String name;
        @XmlElementWrapper(name = "peers")
        public List<String> peer;
        @XmlElementWrapper(name = "tags")
        public List<String> tag;


        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

        @Override
        public String toString() {
            return "ClusterElement {"
                    + "name='" + name + '\''
                    + ", peer=" + peer
                    + ", tag=" + tag
                    + '}';
        }
    }

    //For JAXB
    public ClusterList() {
        this.elements = null;
        this.totalResults = 0;
        this.results = 0;
    }

    public ClusterList(ClusterElement[] elements, long totalResults) {
        this.totalResults = totalResults;
        this.elements = elements != null ? Arrays.copyOf(elements, elements.length) : null;
        this.results = elements != null ? elements.length : 0;
    }

    public ClusterElement[] getElements() {
        return elements != null ? Arrays.copyOf(elements, elements.length) : null;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(totalResults + "\n");
        buffer.append(results + "\n");
        for (ClusterElement element : elements) {
            buffer.append(element.toString());
        }
        return buffer.toString();
    }
}
