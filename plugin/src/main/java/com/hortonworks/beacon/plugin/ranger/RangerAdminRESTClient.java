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

package com.hortonworks.beacon.plugin.ranger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.util.DateUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.MultiPart;
import com.sun.jersey.multipart.file.FileDataBodyPart;
import com.sun.jersey.multipart.impl.MultiPartWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.hortonworks.beacon.util.KnoxTokenUtils;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * RangerAdminRESTClient to connect to Ranger and export policies.
 *
 */
public class RangerAdminRESTClient {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAdminRESTClient.class);
    private static final String RANGER_REST_URL_GET_POLICIES = "/service/plugins/policies/service/name/";
    private static final String RANGER_REST_URL_EXPORTJSONFILE = "/service/plugins/policies/exportJson";
    private static final String RANGER_REST_URL_IMPORTJSONFILE =
            "/service/plugins/policies/importPoliciesFromFile?updateIfExists=true";

    private static final String BEACON_KERBEROS_AUTH_ENABLED="beacon.kerberos.authentication.enabled";
    private static final String BEACON_AUTH_TYPE = "beacon.kerberos.authentication.type";
    private static final String BEACON_USER_PRINCIPAL = "beacon.kerberos.principal";
    private static final String BEACON_USER_KEYTAB = "beacon.kerberos.keytab";
    private static final String NAME_RULES = "beacon.kerberos.namerules.auth_to_local";
    private static final String DEFAULT_NAME_RULE = "DEFAULT";
    private static final String KERBEROS_TYPE = "kerberos";
    private String keyStoreFile = null;
    private String keyStoreFilepwd = null;
    private String trustStoreFile = null;
    private String trustStoreFilepwd = null;
    private String keyStoreType = null;
    private String trustStoreType = null;
    private HostnameVerifier hv = null;
    private static final PropertiesUtil AUTHCONFIG=PropertiesUtil.getInstance();
    private String principal = AUTHCONFIG.getProperty(BEACON_USER_PRINCIPAL);
    private String keytab = AUTHCONFIG.getProperty(BEACON_USER_KEYTAB);
    private String nameRules = AUTHCONFIG.getProperty(NAME_RULES, DEFAULT_NAME_RULE);
    private boolean createDenyPolicy = AUTHCONFIG.getBooleanProperty("beacon.ranger.plugin.create.denypolicy", true);
    private static final String BEACON_RANGER_USER = "beacon.ranger.user";
    private static final String BEACON_RANGER_PASSWORD = "beacon.ranger.password";
    private static final String HDFS_RANGER_POLICIES_FILE_NAME = "source_ranger_hdfs_exported_policies";
    private static final String HIVE_RANGER_POLICIES_FILE_NAME = "source_ranger_hive_exported_policies";

    public RangerExportPolicyList exportRangerPolicies(DataSet dataset) {
        RangerExportPolicyList rangerExportPolicyList = null;
        if (isSpnegoEnable() && !BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()
                && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
            try {
                principal = SecureClientLogin.getPrincipal(principal,
                        BeaconConfig.getInstance().getEngine().getHostName());
                Subject sub = null;
                try {
                    sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
                } catch (Exception ex) {
                    sub = null;
                }
                if (sub == null) {
                    principal = SecureClientLogin.getPrincipal(principal,
                            java.net.InetAddress.getLocalHost().getCanonicalHostName());
                    sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
                }
                final DataSet finaDataset = dataset;
                rangerExportPolicyList = Subject.doAs(sub, new PrivilegedAction<RangerExportPolicyList>() {
                    @Override
                    public RangerExportPolicyList run() {
                        try {
                            return getRangerPoliciesFromFile(finaDataset, true);
                        } catch (Exception e) {
                            LOG.error("Failed to export Ranger policies", e);
                        }
                        return null;
                    }
                });
                return rangerExportPolicyList;
            } catch (Exception e) {
                LOG.error("Failed to Authenticate Using given Principal and Keytab", e);
            }
            return null;
        } else {
            return getRangerPoliciesFromFile(dataset, true);
        }
    }

    public RangerExportPolicyList importRangerPolicies(DataSet dataset,
            RangerExportPolicyList rangerExportPolicyList) throws BeaconException {
        RangerExportPolicyList result = null;
        if (isSpnegoEnable() && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
            try {
                Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
                final DataSet finaDataset = dataset;
                final RangerExportPolicyList finalList=rangerExportPolicyList;
                result = Subject.doAs(sub, new PrivilegedAction<RangerExportPolicyList>() {
                    @Override
                    public RangerExportPolicyList run() {
                        try {
                            return importRangerPoliciesFromFile(finaDataset, finalList);
                        } catch (Exception e) {
                            LOG.error("Failed to export Ranger policies", e);
                        }
                        return null;
                    }
                });
                if (result==null) {
                    throw new BeaconException("Ranger policy import failed, Please refer target Ranger admin logs.");
                }
                return result;
            } catch (Exception e) {
                LOG.error("Failed to Authenticate Using given Principal and Keytab", e);
            }
            return result;
        } else {
            return importRangerPoliciesFromFile(dataset, rangerExportPolicyList);
        }
    }

    private RangerExportPolicyList getRangerPoliciesFromFile(DataSet dataset, boolean exportRestAPI) {
        RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();
        if (exportRestAPI) {
            rangerExportPolicyList=getRangerPoliciesFromExportREST(dataset);
        } else {
            Map<String, Object> metaDataInfo = new LinkedHashMap<String, Object>();
            metaDataInfo.put("Host name", "");
            metaDataInfo.put("Exported by", "");
            metaDataInfo.put("Export time", "");
            metaDataInfo.put("Ranger apache version", "");
            rangerExportPolicyList.setMetaDataInfo(metaDataInfo);
            rangerExportPolicyList.setPolicies(getRangerPolicies(dataset));
        }
        return rangerExportPolicyList;
    }

    private RangerExportPolicyList getRangerPoliciesFromExportREST(DataSet dataset) {
        String sourceRangerEndpoint = dataset.getSourceCluster().getRangerEndpoint();
        Properties clusterProperties = dataset.getSourceCluster().getCustomProperties();
        String rangerHIVEServiceName = null;
        String rangerHDFSServiceName = null;
        if (clusterProperties != null) {
            rangerHDFSServiceName = clusterProperties.getProperty("rangerHDFSServiceName");
            rangerHIVEServiceName = clusterProperties.getProperty("rangerHIVEServiceName");
        }
        Client rangerClient = getRangerClient(dataset.getSourceCluster(),
                BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled());
        ClientResponse clientResp = null;
        String uri = null;
        String sourceDataSet=dataset.getSourceDataSet();
        if (sourceDataSet.endsWith("/")) {
            sourceDataSet=StringUtils.removePattern(sourceDataSet, "/+$");
        }
        if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
            if (!StringUtils.isEmpty(rangerHDFSServiceName)) {
                uri = RANGER_REST_URL_EXPORTJSONFILE + "?serviceName=" + rangerHDFSServiceName + "&polResource="
                        + sourceDataSet + "&resource:path=" + sourceDataSet
                        + "&serviceType=hdfs&resourceMatchScope=self_or_ancestor&resourceMatch=full";
            }
        }
        if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
            if (!StringUtils.isEmpty(rangerHIVEServiceName)) {
                uri = RANGER_REST_URL_EXPORTJSONFILE + "?serviceName=" + rangerHIVEServiceName + "&polResource="
                        + sourceDataSet + "&resource:database=" + sourceDataSet
                        + "&serviceType=hive&resourceMatchScope=self_or_ancestor&resourceMatch=full";
            }
        }
        if (sourceRangerEndpoint.endsWith("/")) {
            sourceRangerEndpoint=StringUtils.removePattern(sourceRangerEndpoint, "/+$");
        }
        String url = sourceRangerEndpoint + (uri.startsWith("/") ? uri : ("/" + uri));
        LOG.debug("URL to export policies from source Ranger: {}", url);
        RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();
        try {
            WebResource webResource = rangerClient.resource(url);
            if (BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()) {
                webResource.header("hadoop-jwt", getSSOToken(dataset.getSourceCluster().getKnoxGatewayURL()));
            }
            clientResp = webResource.get(ClientResponse.class);
            Gson gson = new GsonBuilder().create();
            String response = clientResp.getEntity(String.class);
            if (StringUtils.isNotEmpty(response)) {
                rangerExportPolicyList = gson.fromJson(response, RangerExportPolicyList.class);
            } else {
                LOG.debug(
                    "Ranger policy export request returned empty list or failed, Please refer Ranger admin logs.");
            }
        } catch (Exception ex){
            LOG.warn("Ranger policy export request returned empty list or failed, Please refer Ranger admin logs.");
            LOG.warn("Exception occurred while exporting Ranger policies: {}", ex);
        }
        return rangerExportPolicyList;
    }

    private List<RangerPolicy> getRangerPolicies(DataSet dataset) {
        String sourceRangerEndpoint = dataset.getSourceCluster().getRangerEndpoint();
        Properties clusterProperties = dataset.getSourceCluster().getCustomProperties();
        String rangerHIVEServiceName = null;
        String rangerHDFSServiceName = null;
        if (clusterProperties != null) {
            rangerHDFSServiceName = clusterProperties.getProperty("rangerHDFSServiceName");
            rangerHIVEServiceName = clusterProperties.getProperty("rangerHIVEServiceName");
        }
        Client rangerClient = getRangerClient(dataset.getSourceCluster(),
                BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled());
        ClientResponse clientResp = null;
        String uri = null;
        String sourceDataSet=dataset.getSourceDataSet();
        if (sourceDataSet.endsWith("/")) {
            sourceDataSet=StringUtils.removePattern(sourceDataSet, "/+$");
        }
        if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
            if (!StringUtils.isEmpty(rangerHDFSServiceName)) {
                uri = RANGER_REST_URL_GET_POLICIES + rangerHDFSServiceName + "?resource:path="
                        + sourceDataSet
                        + "&serviceType=hdfs&policyType=0&resourceMatchScope=self_or_ancestor";
            }
        }
        if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
            if (!StringUtils.isEmpty(rangerHIVEServiceName)) {
                uri = RANGER_REST_URL_GET_POLICIES + rangerHIVEServiceName + "?resource:database="
                        + sourceDataSet
                        + "&serviceType=hive&policyType=0&resourceMatchScope=self_or_ancestor";
            }
        }
        if (sourceRangerEndpoint.endsWith("/")) {
            sourceRangerEndpoint=StringUtils.removePattern(sourceRangerEndpoint, "/+$");
        }
        String url = sourceRangerEndpoint + (uri.startsWith("/") ? uri : ("/" + uri));
        LOG.debug("URL to export policies from source Ranger: {}", url);
        RangerPolicyList rangerPolicies = new RangerPolicyList();
        try {
            WebResource webResource = rangerClient.resource(url);
            webResource.header("hadoop-jwt", getSSOToken(dataset.getSourceCluster().getKnoxGatewayURL()));
            clientResp = webResource.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);
            Gson gson = new GsonBuilder().create();
            String response = clientResp.getEntity(String.class);
            rangerPolicies=(RangerPolicyList) gson.fromJson(response, RangerPolicyList.class);
        } catch (Exception ex){
            LOG.info("Ranger policy export request returned empty list or failed, Please refer Ranger admin logs.");
            LOG.error("Exception occurred while exporting Ranger policies: {}", ex);
        }
        if (!CollectionUtils.isEmpty(rangerPolicies.getPolicies())) {
            return rangerPolicies.getPolicies();
        } else {
            return new ArrayList<RangerPolicy>();
        }
    }

    public List<RangerPolicy> removeMutilResourcePolicies(DataSet dataset, List<RangerPolicy> rangerPolicies) {
        List<RangerPolicy> rangerPoliciesToImport = new ArrayList<RangerPolicy>();
        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            Map<String, RangerPolicy.RangerPolicyResource> rangerPolicyResourceMap=null;
            RangerPolicy.RangerPolicyResource rangerPolicyResource=null;
            List<String> resourceNameList=null;
            for (RangerPolicy rangerPolicy : rangerPolicies) {
                if (rangerPolicy!=null) {
                    rangerPolicyResourceMap=rangerPolicy.getResources();
                    if (rangerPolicyResourceMap!=null) {
                        if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
                            rangerPolicyResource=rangerPolicyResourceMap.get("path");
                        } else if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
                            rangerPolicyResource=rangerPolicyResourceMap.get("database");
                        }
                        if (rangerPolicyResource!=null) {
                            resourceNameList=rangerPolicyResource.getValues();
                            if (CollectionUtils.isNotEmpty(resourceNameList) && resourceNameList.size()==1) {
                                rangerPoliciesToImport.add(rangerPolicy);
                            }
                        }
                    }
                }
            }
        }
        return rangerPoliciesToImport;
    }

    public List<RangerPolicy> addSingleDenyPolicies(DataSet dataset, List<RangerPolicy> rangerPolicies) {
        String clusterName=dataset.getSourceCluster().getName();
        if (StringUtils.isEmpty(clusterName)) {
            clusterName="source";
        }
        List<RangerPolicy> rangerPoliciesToImport = new ArrayList<RangerPolicy>();
        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            for (RangerPolicy rangerPolicy : rangerPolicies) {
                rangerPolicy.setDescription(rangerPolicy.getName() + " created by beacon while importing from "
                        + clusterName + " on " + DateUtil.formatDate(new Date()));
                rangerPoliciesToImport.add(rangerPolicy);
            }
        }
        if (!createDenyPolicy) {
            return rangerPoliciesToImport;
        }
        RangerPolicy denyRangerPolicy = null;
        Properties clusterProperties = dataset.getSourceCluster().getCustomProperties();
        String rangerServiceName = null;
        if (clusterProperties != null) {
            if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
                rangerServiceName = clusterProperties.getProperty("rangerHDFSServiceName");
            }
            if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
                rangerServiceName = clusterProperties.getProperty("rangerHIVEServiceName");
            }
        }
        String sourceDataSet=dataset.getSourceDataSet();
        if (sourceDataSet.endsWith("/")) {
            sourceDataSet=StringUtils.removePattern(sourceDataSet, "/+$");
        }
        String targetDataSet=dataset.getTargetDataSet();
        if (targetDataSet.endsWith("/")) {
            targetDataSet=StringUtils.removePattern(targetDataSet, "/+$");
        }
        if (!StringUtils.isEmpty(rangerServiceName)) {
            denyRangerPolicy = new RangerPolicy();
            denyRangerPolicy.setService(rangerServiceName);
            denyRangerPolicy.setName(clusterName + "_beacon deny policy for " + targetDataSet);
            denyRangerPolicy.setDescription("Deny policy created by beacon while importing from " + clusterName + " on "
                    + DateUtil.formatDate(new Date()));
        }
        if (denyRangerPolicy!=null) {
            Map<String, RangerPolicy.RangerPolicyResource> rangerPolicyResourceMap =
                    new HashMap<String, RangerPolicy.RangerPolicyResource>();
            RangerPolicy.RangerPolicyResource rangerPolicyResource = new RangerPolicy.RangerPolicyResource();
            List<String> resourceNameList = new ArrayList<String>();

            List<RangerPolicy.RangerPolicyItem> denyPolicyItemsForPublicGroup = denyRangerPolicy.getDenyPolicyItems();
            RangerPolicy.RangerPolicyItem denyPolicyItem = new RangerPolicy.RangerPolicyItem();
            List<RangerPolicy.RangerPolicyItemAccess> denyPolicyItemAccesses =
                    new ArrayList<RangerPolicy.RangerPolicyItemAccess>();

            List<RangerPolicy.RangerPolicyItem> denyExceptionsItemsForBeaconUser =
                    denyRangerPolicy.getDenyExceptions();
            RangerPolicy.RangerPolicyItem denyExceptionsPolicyItem = new RangerPolicy.RangerPolicyItem();
            List<RangerPolicy.RangerPolicyItemAccess> denyExceptionsPolicyItemAccesses =
                    new ArrayList<RangerPolicy.RangerPolicyItemAccess>();

            if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
                resourceNameList.add(sourceDataSet);
                resourceNameList.add("/dummy");
                rangerPolicyResource.setIsRecursive(true);
                rangerPolicyResource.setValues(resourceNameList);
                rangerPolicyResourceMap.put("path", rangerPolicyResource);
                denyRangerPolicy.setResources(rangerPolicyResourceMap);

                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("write", true));
                denyPolicyItem.setAccesses(denyPolicyItemAccesses);
                denyPolicyItemsForPublicGroup.add(denyPolicyItem);
                List<String> denyPolicyItemsGroups = new ArrayList<String>();
                denyPolicyItemsGroups.add("public");
                denyPolicyItem.setGroups(denyPolicyItemsGroups);
                denyRangerPolicy.setDenyPolicyItems(denyPolicyItemsForPublicGroup);

                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("read", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("write", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("execute", true));
                denyExceptionsPolicyItem.setAccesses(denyExceptionsPolicyItemAccesses);
                denyExceptionsItemsForBeaconUser.add(denyExceptionsPolicyItem);
                List<String> denyExceptionsPolicyItemsUsers = new ArrayList<String>();
                denyExceptionsPolicyItemsUsers.add("beacon");
                denyExceptionsPolicyItem.setUsers(denyExceptionsPolicyItemsUsers);
                denyRangerPolicy.setDenyExceptions(denyExceptionsItemsForBeaconUser);
            }
            if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
                resourceNameList.add(sourceDataSet);
                resourceNameList.add("dummy");
                rangerPolicyResource.setValues(resourceNameList);
                RangerPolicy.RangerPolicyResource rangerPolicyResourceColumn =new RangerPolicy.RangerPolicyResource();
                rangerPolicyResourceColumn.setValues(new ArrayList<String>(){{add("*"); }});
                RangerPolicy.RangerPolicyResource rangerPolicyResourceTable =new RangerPolicy.RangerPolicyResource();
                rangerPolicyResourceTable.setValues(new ArrayList<String>(){{add("*"); }});
                rangerPolicyResourceMap.put("database", rangerPolicyResource);
                rangerPolicyResourceMap.put("table", rangerPolicyResourceTable);
                rangerPolicyResourceMap.put("column", rangerPolicyResourceColumn);
                denyRangerPolicy.setResources(rangerPolicyResourceMap);

                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("create", true));
                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("update", true));
                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("drop", true));
                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("alter", true));
                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("index", true));
                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("lock", true));
                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("write", true));
                denyPolicyItem.setAccesses(denyPolicyItemAccesses);
                denyPolicyItemsForPublicGroup.add(denyPolicyItem);
                List<String> denyPolicyItemsGroups = new ArrayList<String>();
                denyPolicyItemsGroups.add("public");
                denyPolicyItem.setGroups(denyPolicyItemsGroups);
                denyRangerPolicy.setDenyPolicyItems(denyPolicyItemsForPublicGroup);

                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("create", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("update", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("drop", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("alter", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("index", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("lock", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("write", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("select", true));
                denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("read", true));
                denyExceptionsPolicyItem.setAccesses(denyExceptionsPolicyItemAccesses);
                denyExceptionsItemsForBeaconUser.add(denyExceptionsPolicyItem);
                List<String> denyExceptionsPolicyItemsUsers = new ArrayList<String>();
                denyExceptionsPolicyItemsUsers.add("beacon");
                denyExceptionsPolicyItem.setUsers(denyExceptionsPolicyItemsUsers);
                denyRangerPolicy.setDenyExceptions(denyExceptionsItemsForBeaconUser);
            }
            rangerPoliciesToImport.add(denyRangerPolicy);
        }
        return rangerPoliciesToImport;
    }

    public RangerExportPolicyList importRangerPoliciesFromFile(DataSet dataset,
            RangerExportPolicyList rangerExportPolicyList) throws BeaconException{
        String targetRangerEndpoint = dataset.getTargetCluster().getRangerEndpoint();
        Properties sourceClusterProperties = dataset.getSourceCluster().getCustomProperties();
        Properties targetClusterProperties = dataset.getTargetCluster().getCustomProperties();
        String sourceClusterServiceName = null;
        String targetClusterServiceName = null;
        String serviceMapJsonFileName = "servicemap.json";
        String rangerPoliciesJsonFileName = "replicationPolicies.json";
        String sourceDataSet=dataset.getSourceDataSet();
        if (sourceDataSet.endsWith("/")) {
            sourceDataSet=StringUtils.removePattern(sourceDataSet, "/+$");
        }
        String uri = RANGER_REST_URL_IMPORTJSONFILE+"&polResource="+sourceDataSet;
        if (sourceClusterProperties != null && targetClusterProperties != null) {
            if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
                sourceClusterServiceName = sourceClusterProperties.getProperty("rangerHDFSServiceName");
                targetClusterServiceName = targetClusterProperties.getProperty("rangerHDFSServiceName");
                serviceMapJsonFileName = "hdfs_servicemap.json";
                rangerPoliciesJsonFileName = "hdfs_replicationPolicies.json";
            } else if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
                sourceClusterServiceName = sourceClusterProperties.getProperty("rangerHIVEServiceName");
                targetClusterServiceName = targetClusterProperties.getProperty("rangerHIVEServiceName");
                serviceMapJsonFileName = "hive_servicemap.json";
                rangerPoliciesJsonFileName = "hive_replicationPolicies.json";
            }
        }
        Map<String, String> serviceMap = new LinkedHashMap<String, String>();
        if (!StringUtils.isEmpty(sourceClusterServiceName) && !StringUtils.isEmpty(targetClusterServiceName)) {
            serviceMap.put(sourceClusterServiceName, targetClusterServiceName);
        }

        Gson gson = new GsonBuilder().create();
        String jsonServiceMap = gson.toJson(serviceMap);
        serviceMapJsonFileName=writeJsonStringToFile(jsonServiceMap, serviceMapJsonFileName);

        String jsonRangerExportPolicyList = gson.toJson(rangerExportPolicyList);
        rangerPoliciesJsonFileName=writeJsonStringToFile(jsonRangerExportPolicyList, rangerPoliciesJsonFileName);

        if (targetRangerEndpoint.endsWith("/")) {
            targetRangerEndpoint=StringUtils.removePattern(targetRangerEndpoint, "/+$");
        }
        String url = targetRangerEndpoint + (uri.startsWith("/") ? uri : ("/" + uri));
        LOG.debug("URL to import policies on target Ranger: {}", url);
        Client rangerClient = getRangerClient(dataset.getTargetCluster(), false);
        ClientResponse clientResp = null;
        WebResource webResource = rangerClient.resource(url);
        FileDataBodyPart filePartPolicies = new FileDataBodyPart("file", new File(rangerPoliciesJsonFileName));
        FileDataBodyPart filePartServiceMap = new FileDataBodyPart("servicesMapJson",
                new File(serviceMapJsonFileName));
        FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
        MultiPart multipartEntity=null;
        try {
            multipartEntity = formDataMultiPart.bodyPart(filePartPolicies).bodyPart(filePartServiceMap);
            try {
                clientResp = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.MULTIPART_FORM_DATA)
                    .post(ClientResponse.class, multipartEntity);
            } catch (Throwable t) {
                if (clientResp==null) {
                    throw new BeaconException("Ranger policy import failed, Please refer target Ranger admin logs.");
                }
            }
            if (clientResp!=null) {
                if (clientResp.getStatus()==HttpServletResponse.SC_NO_CONTENT) {
                    LOG.debug("Ranger policy import finished successfully");
                } else if (clientResp.getStatus()==HttpServletResponse.SC_UNAUTHORIZED) {
                    throw new BeaconException("Authentication Failure while communicating to Ranger admin");
                } else {
                    throw new BeaconException("Ranger policy import failed, Please refer target Ranger admin logs.");
                }
            }
        } finally {
            try {
                if (filePartPolicies!=null) {
                    filePartPolicies.cleanup();
                }
                if (filePartServiceMap!=null) {
                    filePartServiceMap.cleanup();
                }
                if (formDataMultiPart!=null) {
                    formDataMultiPart.close();
                }
                if (multipartEntity!=null) {
                    multipartEntity.close();
                }
            } catch (IOException e) {
                LOG.error("Exception occurred while closing resources: {}", e);
            }
        }
        return rangerExportPolicyList;
    }

    private String writeJsonStringToFile(String jsonString, String fileName) {
        Charset encoding = StandardCharsets.UTF_8;
        String filePath= "";
        try {
            String parentPath=System.getProperty("beacon.home");
            if (StringUtils.isEmpty(parentPath)) {
                parentPath=System.getProperty("user.dir");
            }
            filePath= parentPath + File.separator + fileName;
            java.nio.file.Path path = Paths.get(filePath);
            List<String> fileContents = new ArrayList<String>();
            fileContents.add(jsonString);
            Files.write(path, fileContents, encoding);
        } catch (IOException ex) {
            LOG.error("Failed to write json string to file: {}, {}", filePath, ex);
        } catch (Exception ex) {
            LOG.error("Failed to write json string to file: {}, {}", filePath, ex);
        }
        return filePath;
    }


    private synchronized Client getRangerClient(Cluster cluster, boolean shouldProxy) {
        Client ret = null;
        String rangerEndpoint = shouldProxy
                ? KnoxTokenUtils.getKnoxProxiedURL(cluster.getKnoxGatewayURL(), "ranger")
                :  cluster.getRangerEndpoint();
        Properties clusterProperties = cluster.getCustomProperties();
        ClientConfig config = new DefaultClientConfig();
        config.getClasses().add(MultiPartWriter.class);
        if (StringUtils.startsWith(rangerEndpoint, "https://")) {
            SSLContext sslContext = null;
            if (sslContext == null) {
                try {
                    KeyManager[] kmList = null;
                    TrustManager[] tmList = null;
                    keyStoreFile = clusterProperties.getProperty("SSLKeyStoreFile");
                    keyStoreFilepwd = clusterProperties.getProperty("SSLKeyStoreFilePassword");
                    keyStoreType = KeyStore.getDefaultType();
                    LOG.debug("SSLKeyStoreFile: {}", keyStoreFile);
                    if (keyStoreFile != null && keyStoreFilepwd != null) {
                        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                        InputStream in = null;
                        try {
                            in = getFileInputStream(keyStoreFile);
                            if (in == null) {
                                LOG.error("SSLKeyStoreFile: {}", keyStoreFile);
                                return ret;
                            }
                            keyStore.load(in, keyStoreFilepwd.toCharArray());
                            KeyManagerFactory keyManagerFactory = KeyManagerFactory
                                    .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                            keyManagerFactory.init(keyStore, keyStoreFilepwd.toCharArray());
                            kmList = keyManagerFactory.getKeyManagers();
                        } finally {
                            if (in != null) {
                                in.close();
                            }
                        }
                    }

                    trustStoreFile = clusterProperties.getProperty("SSLTrustStoreFile");
                    trustStoreFilepwd = clusterProperties.getProperty("SSLTrustStoreFilePassword");
                    LOG.debug("SSLTrustStoreFile: {}", trustStoreFile);
                    trustStoreType = KeyStore.getDefaultType();
                    if (trustStoreFile != null && trustStoreFilepwd != null) {
                        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
                        InputStream in = null;
                        try {
                            in = getFileInputStream(trustStoreFile);
                            if (in == null) {
                                LOG.error("Unable to obtain keystore from file: {}", keyStoreFile);
                                return ret;
                            }
                            trustStore.load(in, trustStoreFilepwd.toCharArray());
                            TrustManagerFactory trustManagerFactory = TrustManagerFactory
                                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                            trustManagerFactory.init(trustStore);
                            tmList = trustManagerFactory.getTrustManagers();
                        } finally {
                            if (in != null) {
                                in.close();
                            }
                        }
                    }
                    sslContext = SSLContext.getInstance("SSL");
                    sslContext.init(kmList, tmList, new SecureRandom());
                    hv = new HostnameVerifier() {
                        public boolean verify(String urlHostName, SSLSession session) {
                            return session.getPeerHost().equals(urlHostName);
                        }
                    };
                } catch (Throwable t) {
                    LOG.error("Unable to create SSLConext for communication to Ranger admin", t);
                }
            }
            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hv, sslContext));
            ret = Client.create(config);
        } else {
            config.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
            ret = Client.create(config);
        }
        if (!(isSpnegoEnable() && SecureClientLogin.isKerberosCredentialExists(principal, keytab))) {
            if (ret != null) {
                String remoteRangerAdmin = AUTHCONFIG.getProperty(BEACON_RANGER_USER);
                LOG.debug("Beacon Ranger User: {}", remoteRangerAdmin);
                String remoteRangerPassword=null;
                try {
                    remoteRangerPassword = AUTHCONFIG.resolvePassword(BEACON_RANGER_PASSWORD);
                } catch (BeaconException e) {
                    remoteRangerPassword=null;
                }
                if (StringUtils.isEmpty(remoteRangerPassword)) {
                    remoteRangerPassword=AUTHCONFIG.getProperty(BEACON_RANGER_PASSWORD);
                }
                if (!StringUtils.isEmpty(remoteRangerAdmin) && !StringUtils.isEmpty(remoteRangerPassword)) {
                    ret.addFilter(new HTTPBasicAuthFilter(remoteRangerAdmin, remoteRangerPassword));
                }
            }
        }
        return ret;
    }

    private InputStream getFileInputStream(String path) throws FileNotFoundException {
        InputStream ret = null;
        File f = new File(path);
        if (f.exists()) {
            ret = new FileInputStream(f);
        } else {
            ret = RangerAdminRESTClient.class.getResourceAsStream(path);
            if (ret == null) {
                if (!path.startsWith("/")) {
                    ret = getClass().getResourceAsStream("/" + path);
                }
            }
            if (ret == null) {
                ret = ClassLoader.getSystemClassLoader().getResourceAsStream(path);
                if (ret == null) {
                    if (!path.startsWith("/")) {
                        ret = ClassLoader.getSystemResourceAsStream("/" + path);
                    }
                }
            }
        }
        return ret;
    }

    private static boolean isSpnegoEnable() {
        boolean isKerberos = AUTHCONFIG.getBooleanProperty(BEACON_KERBEROS_AUTH_ENABLED, false);
        if (isKerberos && KERBEROS_TYPE.equalsIgnoreCase(AUTHCONFIG.getProperty(BEACON_AUTH_TYPE))) {
            return isKerberos;
        }
        if (isKerberos) {
            isKerberos = false;
            String keytab = AUTHCONFIG.getProperty(BEACON_USER_KEYTAB);
            String principal="*";
            try {
                principal = SecureClientLogin.getPrincipal(AUTHCONFIG.getProperty(BEACON_USER_PRINCIPAL),
                        BeaconConfig.getInstance().getEngine().getHostName());
            } catch (IOException e) {
                LOG.error("Unable to read principal: {}", e.toString());
            }
            String hostname = BeaconConfig.getInstance().getEngine().getHostName();
            if (StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(principal)
                    && StringUtils.isNotEmpty(hostname)) {
                isKerberos = true;
            }
        }
        return isKerberos;
    }

    public List<RangerPolicy> changeDataSet(DataSet dataset, List<RangerPolicy> rangerPolicies) {
        String targetDataSet=dataset.getTargetDataSet();
        String sourceDataSet=dataset.getSourceDataSet();
        if (sourceDataSet.endsWith("/")) {
            sourceDataSet=StringUtils.removePattern(sourceDataSet, "/+$");
        }
        if (targetDataSet.endsWith("/")) {
            targetDataSet=StringUtils.removePattern(targetDataSet, "/+$");
        }
        if (targetDataSet.equals(sourceDataSet)) {
            return rangerPolicies;
        }
        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            Map<String, RangerPolicy.RangerPolicyResource> rangerPolicyResourceMap=null;
            RangerPolicy.RangerPolicyResource rangerPolicyResource=null;
            List<String> resourceNameList=null;
            for (RangerPolicy rangerPolicy : rangerPolicies) {
                if (rangerPolicy!=null) {
                    rangerPolicyResourceMap=rangerPolicy.getResources();
                    if (rangerPolicyResourceMap!=null) {
                        if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
                            rangerPolicyResource=rangerPolicyResourceMap.get("path");
                            if (rangerPolicyResource!=null) {
                                resourceNameList=rangerPolicyResource.getValues();
                                if (CollectionUtils.isNotEmpty(resourceNameList)) {
                                    for (int i=0; i< resourceNameList.size(); i++) {
                                        String resourceName=resourceNameList.get(i);
                                        if (resourceName.startsWith(sourceDataSet)) {
                                            String temp=resourceName.substring(sourceDataSet.length());
                                            String newResourceName=targetDataSet.concat(temp);
                                            resourceNameList.set(i, newResourceName);
                                        }
                                    }
                                }
                            }
                        } else if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
                            rangerPolicyResource=rangerPolicyResourceMap.get("database");
                            if (rangerPolicyResource!=null) {
                                resourceNameList=rangerPolicyResource.getValues();
                                if (CollectionUtils.isNotEmpty(resourceNameList)) {
                                    for (int i=0; i< resourceNameList.size(); i++) {
                                        String resourceName=resourceNameList.get(i);
                                        if (resourceName.equals(sourceDataSet)) {
                                            resourceNameList.set(i, targetDataSet);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return rangerPolicies;
    }

    private String getSSOToken(String knoxBaseURL) {
        if (BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled()) {
            try {
                return KnoxTokenUtils.getKnoxSSOToken(knoxBaseURL);
            } catch (Exception e) {
                LOG.error("Unable to get knox sso token from {} : {} . Cause: {}", knoxBaseURL, e.getMessage(), e);
                return null;
            }
        }
        return null;
    }

    private Path writeExportedRangerPoliciesToJsonFile(String jsonString, String fileName, Path stagingDirPath) {
        String filePath= "";
        Path newPath=null;
        FSDataOutputStream outStream=null;
        OutputStreamWriter writer=null;
        try {
            FileSystem fileSystem=getFileSystem(stagingDirPath);
            if (fileSystem!=null && fileSystem.exists(stagingDirPath)) {
                newPath=stagingDirPath.suffix(File.separator + fileName);
                outStream = fileSystem.create(newPath, true);
                writer = new OutputStreamWriter(outStream);
                writer.write(jsonString);
            }
        } catch (IOException ex) {
            if (newPath!=null) {
                filePath=newPath.toString();
            }
            LOG.error("Failed to write json string to file: {}, {}", filePath, ex);
        } catch (Exception ex) {
            if (newPath!=null) {
                filePath=newPath.toString();
            }
            LOG.error("Failed to write json string to file: {}, {}", filePath, ex);
        } finally {
            try{
                if (writer!=null) {
                    writer.close();
                }
                if (outStream!=null) {
                    outStream.close();
                }
            }catch(Exception ex) {
                LOG.error("Unable to close writer/outStream.", ex);
            }
        }
        return newPath;
    }

    private FileSystem getFileSystem(Path path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = path.getFileSystem(conf);
        return fileSystem;
    }

    public Path saveRangerPoliciesToFile(DataSet dataset,
        RangerExportPolicyList rangerExportPolicyList, Path stagingDirPath) throws BeaconException{
        String rangerPoliciesJsonFileName = null;
        String timeStampStr=DateUtil.formatDate(new Date(), "yyyyMMddHHmmss");
        if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
            rangerPoliciesJsonFileName = HDFS_RANGER_POLICIES_FILE_NAME+"_"+timeStampStr+".json";
        } else if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
            rangerPoliciesJsonFileName = HIVE_RANGER_POLICIES_FILE_NAME+"_"+timeStampStr+".json";
        }
        Gson gson = new GsonBuilder().create();
        String jsonRangerExportPolicyList = gson.toJson(rangerExportPolicyList);
        return writeExportedRangerPoliciesToJsonFile(jsonRangerExportPolicyList, rangerPoliciesJsonFileName,
                stagingDirPath);
    }

    public RangerExportPolicyList readRangerPoliciesFromJsonFile(Path filePath) throws BeaconException {
        RangerExportPolicyList rangerExportPolicyList =null;
        Gson gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
        try {
            FileSystem fs=getFileSystem(filePath);
            InputStream inputStream = fs.open(filePath);
            Reader reader = new InputStreamReader(inputStream, Charset.forName("UTF-8"));
            rangerExportPolicyList = gsonBuilder.fromJson(reader, RangerExportPolicyList.class);
        } catch(Exception ex){
            LOG.error("Error reading file :"+filePath, ex);
        }
        return rangerExportPolicyList;
    }
}
