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

package com.hortonworks.beacon.client.resource;

import com.hortonworks.beacon.client.entity.PeerInfo;
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
        NAME, PEERS, PEERSINFO, TAGS
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
        @XmlElementWrapper(name = "peersInfo")
        public List<PeerInfo> peersInfo;
        @XmlElementWrapper(name = "tags")
        public List<String> tag;


        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

        @Override
        public String toString() {
            return "ClusterElement {"
                    + "name='" + name + '\''
                    + ", peer=" + peer
                    + ", tag=" + tag
                    + ", peersInfo=" + peersInfo
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
