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

package com.hortonworks.beacon.config;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.BeaconCredentialProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Security Configuration class for Beacon.   Responsible for loading and maintaining the beacon
 * security configuration from the beacon-security-site.xml file.
 */
public final class PropertiesUtil {
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
    private static Map<String, String> propertiesMap = new HashMap<>();

    private static final String CONFIG_FILE = "beacon-security-site.xml";
    private static final String BASIC_AUTH_FILE = "user-credentials.properties";
    public static final String BASE_API = "api/beacon/";
    private boolean initialized;

    private PropertiesUtil() {
    }

    private static final class Holder {
        private static final PropertiesUtil INSTANCE = new PropertiesUtil();
    }

    public static PropertiesUtil getInstance() {
        if (!Holder.INSTANCE.initialized) {
            Holder.INSTANCE.init();
        }
        return Holder.INSTANCE;
    }

    public void init() {
        loadConfig(CONFIG_FILE);
    }

    private void loadConfig(String configFileName) {
        String path = getResourceFileName(configFileName);
        logger.info("Loading {} from {}", configFileName, path);
        try {
            DocumentBuilderFactory xmlDocumentBuilderFactory = DocumentBuilderFactory.newInstance();
            xmlDocumentBuilderFactory.setIgnoringComments(true);
            xmlDocumentBuilderFactory.setNamespaceAware(true);
            xmlDocumentBuilderFactory.setExpandEntityReferences(false);
            xmlDocumentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            DocumentBuilder xmlDocumentBuilder = xmlDocumentBuilderFactory.newDocumentBuilder();
            Document xmlDocument = xmlDocumentBuilder.parse(new File(path));
            xmlDocument.getDocumentElement().normalize();
            NodeList nList = xmlDocument.getElementsByTagName("property");
            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    String propertyName = "";
                    String propertyValue = "";
                    if (eElement.getElementsByTagName("name").item(0) != null) {
                        propertyName = eElement.getElementsByTagName("name").item(0).getTextContent().trim();
                    }
                    if (eElement.getElementsByTagName("value").item(0) != null) {
                        propertyValue = eElement.getElementsByTagName("value").item(0).getTextContent().trim();
                    }
                    propertiesMap.put(propertyName, propertyValue);
                }
            }
        } catch (Exception e) {
            logger.error("Load configuration fail. Reason: " + e.toString());
        }

        Class cl = PropertiesUtil.class;
        URL resource = cl.getResource("/" + BASIC_AUTH_FILE);
        InputStream resourceAsStream = null;
        Properties properties = new Properties();

        if (resource != null) {
            logger.info("Loading {} from {}", BASIC_AUTH_FILE, resource.getPath());
            resourceAsStream = cl.getResourceAsStream("/" + BASIC_AUTH_FILE);
        } else {
            resource = cl.getResource(BASIC_AUTH_FILE);
            if (resource != null) {
                logger.info("Loading {} from {}", BASIC_AUTH_FILE, resource.getPath());
                resourceAsStream = cl.getResourceAsStream(BASIC_AUTH_FILE);
            }
        }
        if (resourceAsStream != null) {
            try {
                properties.load(resourceAsStream);
                Enumeration<?> e = properties.propertyNames();
                while (e.hasMoreElements()) {
                    String key = (String) e.nextElement();
                    if (key!=null) {
                        String value = properties.getProperty(key);
                        propertiesMap.put(key, value);
                    }
                }
            } catch (Exception e) {
                logger.error("Unable to build property file " + BASIC_AUTH_FILE+"  Reason: "+ e.toString());
            } finally {
                try {
                    resourceAsStream.close();
                } catch (IOException e) {
                    logger.error("Unable to close property file " + BASIC_AUTH_FILE+" Reason: "+ e.toString());
                }
            }
        }
        initialized = true;
    }

    public String getResourceFileName(String aResourceName) {
        String ret = aResourceName;
        ClassLoader cl = getClass().getClassLoader();
        for (String path : new String[] { aResourceName, "/" + aResourceName }) {
            try {
                URL lurl = cl.getResource(path);
                if (lurl != null) {
                    ret = lurl.getFile();
                }
            } catch (Throwable t) {
                ret = null;
            }
            if (ret != null) {
                break;
            }
        }
        if (ret == null) {
            ret = aResourceName;
        }
        return ret;
    }
    public  String getProperty(String key, String defaultValue) {
        if (key == null) {
            return null;
        }
        String rtrnVal = propertiesMap.get(key);
        if (rtrnVal == null) {
            rtrnVal = defaultValue;
        }
        return rtrnVal;
    }

    public  String getProperty(String key) {
        if (key == null) {
            return null;
        }
        return propertiesMap.get(key);
    }

    public  boolean getBooleanProperty(String key, boolean defaultValue) {
        if (key == null) {
            return defaultValue;
        }
        String value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try{
            return Boolean.parseBoolean(value);
        }catch(Exception ex){
            return defaultValue;
        }
    }

    public static Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public static InputStream getFileAsInputStream(String fileName) {
        File fileToLoad = null;

        if (fileName != null) {
            // Look for configured filename
            fileToLoad = new File(fileName);
        }

        InputStream inStr = null;
        if (fileToLoad!=null && fileToLoad.exists()) {
            try {
                inStr = new FileInputStream(fileToLoad);
            } catch (FileNotFoundException e) {
                logger.error("Error loading file " + fileName+ " Reason: "+e.toString());
            }
        } else {
            // Look for file as class loader resource
            inStr = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            if (inStr == null) {
                String msg = fileName + " not found in file system or as class loader resource";
                logger.error(msg);
            }

        }
        return inStr;
    }

    public String resolvePassword(String passwordAlias) throws BeaconException {
        if (StringUtils.isNotBlank(passwordAlias)) {
            return new BeaconCredentialProvider(
                    propertiesMap.get(BeaconCredentialProvider.CREDENTIAL_PROVIDER_PATH)).resolveAlias(passwordAlias);
        }
        return null;
    }
}
