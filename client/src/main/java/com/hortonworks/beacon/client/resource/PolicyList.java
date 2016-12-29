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
@XmlRootElement(name = "policies")
@XmlAccessorType(XmlAccessType.FIELD)
//@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class PolicyList {
    public int getTotalResults() {
        return totalResults;
    }

    @XmlElement
    private int totalResults;

    @XmlElement(name = "policy")
    private final PolicyElement[] elements;

    /**
     * List of fields returned by RestAPI.
     */
    public enum PolicyFieldList {
        TYPE, NAME, STATUS, TAGS, CLUSTERS, FREQUENCY, STARTTIME, ENDTIME
    }

    /**
     * Filter by these Fields is supported by RestAPI.
     */
    public enum PolicyFilterByFields {
        SOURCECLUSTER, TARGETCLUSTER
    }

    /**
     * Element within an entity.
     */
    public static class PolicyElement {
        //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
        @XmlElement
        public String type;
        @XmlElement
        public String name;
        @XmlElement
        public String status;
        @XmlElement
        public Long frequency;
        @XmlElement
        public String startTime;
        @XmlElement
        public String endTime;
        @XmlElementWrapper(name = "tags")
        public List<String> tag;
        @XmlElementWrapper(name = "sourceclusters")
        public List<String> sourceCluster;
        @XmlElementWrapper(name = "targetclusters")
        public List<String> targetCluster;

        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

        @Override
        public String toString() {
            String outString = "(" + type + ") " + name;
            if (StringUtils.isNotEmpty(status)) {
                outString += "(" + status + ")";
            }

            if (frequency != null) {
                outString += "(" + frequency + ")";
            }

            if (StringUtils.isNotEmpty(startTime)) {
                outString += "(" + startTime + ")";
            }

            if (StringUtils.isNotEmpty(endTime)) {
                outString += "(" + endTime + ")";
            }

            if (tag != null && !tag.isEmpty()) {
                outString += " - " + tag.toString();
            }

            if (sourceCluster != null && !sourceCluster.isEmpty()) {
                outString += " - " + sourceCluster.toString();
            }

            if (targetCluster != null && !targetCluster.isEmpty()) {
                outString += " - " + targetCluster.toString();
            }

            outString += "\n";
            return outString;
        }
    }

    //For JAXB
    public PolicyList() {
        this.elements = null;
        this.totalResults = 0;
    }

    public PolicyList(PolicyElement[] elements, int totalResults) {
        this.totalResults = totalResults;
        this.elements = elements;
    }

    public PolicyList(Entity[] elements, int totalResults) {
        this.totalResults = totalResults;
        int len = elements.length;
        PolicyElement[] items = new PolicyElement[len];
        for (int i = 0; i < len; i++) {
            items[i] = createPolicyElement(elements[i]);
        }
        this.elements = items;
    }

    private PolicyElement createPolicyElement(Entity e) {
        PolicyElement element = new PolicyElement();
        element.type = e.getEntityType().name().toLowerCase();
        element.name = e.getName();
        element.status = null;
        element.frequency = null;
        element.startTime = null;
        element.endTime = null;
        element.tag = new ArrayList<String>();
        element.sourceCluster = new ArrayList<String>();
        element.targetCluster = new ArrayList<String>();
        return element;
    }

    public PolicyElement[] getElements() {
        return elements;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(totalResults + "\n");
        for (PolicyElement element : elements) {
            buffer.append(element.toString());
        }
        return buffer.toString();
    }
}
