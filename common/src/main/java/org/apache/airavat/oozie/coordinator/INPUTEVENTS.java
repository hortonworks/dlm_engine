//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 in JDK 6 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2011.12.08 at 10:28:42 AM GMT+05:30 
//


package org.apache.airavat.oozie.coordinator;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for INPUTEVENTS complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="INPUTEVENTS">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="data-in" type="{uri:oozie:coordinator:0.1}DATAIN" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "INPUTEVENTS", propOrder = {
    "dataIn"
})
public class INPUTEVENTS {

    @XmlElement(name = "data-in", required = true)
    protected List<DATAIN> dataIn;

    /**
     * Gets the value of the dataIn property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the dataIn property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDataIn().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DATAIN }
     * 
     * 
     */
    public List<DATAIN> getDataIn() {
        if (dataIn == null) {
            dataIn = new ArrayList<DATAIN>();
        }
        return this.dataIn;
    }

}
