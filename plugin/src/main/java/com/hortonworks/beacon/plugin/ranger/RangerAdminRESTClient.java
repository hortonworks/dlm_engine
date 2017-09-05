/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.plugin.ranger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.rb.MessageCode;
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

/**
 * RangerAdminRESTClient to connect to Ranger and export policies.
 *
 */
public class RangerAdminRESTClient {
    private static final BeaconLog LOG = BeaconLog.getLog(RangerAdminRESTClient.class);
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

    public RangerExportPolicyList exportRangerPolicies(DataSet dataset) {
        RangerExportPolicyList rangerExportPolicyList = null;
        if (isSpnegoEnable() && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
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
                            return getRangerPoliciesFromFile(finaDataset);
                        } catch (Exception e) {
                            LOG.error(MessageCode.PLUG_000039.name(), e);
                        }
                        return null;
                    }
                });
                return rangerExportPolicyList;
            } catch (Exception e) {
                LOG.error(MessageCode.PLUG_000040.name(), e);
            }
            return null;
        } else {
            return getRangerPoliciesFromFile(dataset);
        }
    }

    public RangerExportPolicyList importRangerPolicies(DataSet dataset,
            RangerExportPolicyList rangerExportPolicyList) {
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
                            LOG.error(MessageCode.PLUG_000039.name(), e);
                        }
                        return null;
                    }
                });
                return result;
            } catch (Exception e) {
                LOG.error(MessageCode.PLUG_000040.name(), e);
            }
            return null;
        } else {
            return importRangerPoliciesFromFile(dataset, rangerExportPolicyList);
        }
    }

    private RangerExportPolicyList getRangerPoliciesFromFile(DataSet dataset) {
        String sourceRangerEndpoint = dataset.getSourceCluster().getRangerEndpoint();
        Properties clusterProperties = dataset.getSourceCluster().getCustomProperties();
        String rangerHIVEServiceName = null;
        String rangerHDFSServiceName = null;
        if (clusterProperties != null) {
            rangerHDFSServiceName = clusterProperties.getProperty("rangerHDFSServiceName");
            rangerHIVEServiceName = clusterProperties.getProperty("rangerHIVEServiceName");
        }
        Client rangerClient = getRangerClient(dataset.getSourceCluster());
        ClientResponse clientResp = null;
        String uri = null;
        if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
            if (!StringUtils.isEmpty(rangerHDFSServiceName)) {
                uri = RANGER_REST_URL_EXPORTJSONFILE + "?serviceName=" + rangerHDFSServiceName + "&polResource="
                        + dataset.getDataSet() + "&resource:path=" + dataset.getDataSet() + "&serviceType=hdfs";
            }
        }
        if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
            if (!StringUtils.isEmpty(rangerHIVEServiceName)) {
                uri = RANGER_REST_URL_EXPORTJSONFILE + "?serviceName=" + rangerHIVEServiceName + "&polResource="
                        + dataset.getDataSet() + "&resource:database=" + dataset.getDataSet() + "&serviceType=hive";
            }
        }
        String url = sourceRangerEndpoint + (uri.startsWith("/") ? uri : ("/" + uri));
        LOG.info(MessageCode.PLUG_000024.name(), url);
        RangerExportPolicyList rangerExportPolicyList = null;
        try {
            WebResource webResource = rangerClient.resource(url);
            clientResp = webResource.get(ClientResponse.class);
            Gson gson = new GsonBuilder().create();
            String response = clientResp.getEntity(String.class);
            rangerExportPolicyList = gson.fromJson(response, RangerExportPolicyList.class);
        } catch (Exception ex){
            rangerExportPolicyList = null;
            LOG.info(MessageCode.PLUG_000026.name());
            LOG.error(MessageCode.PLUG_000029.name(), ex);
        }
        if (rangerExportPolicyList == null || CollectionUtils.isEmpty(rangerExportPolicyList.getPolicies())) {
            rangerExportPolicyList = new RangerExportPolicyList();
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

    private List<RangerPolicy> getRangerPolicies(DataSet dataset) {
        String sourceRangerEndpoint = dataset.getSourceCluster().getRangerEndpoint();
        Properties clusterProperties = dataset.getSourceCluster().getCustomProperties();
        String rangerHIVEServiceName = null;
        String rangerHDFSServiceName = null;
        if (clusterProperties != null) {
            rangerHDFSServiceName = clusterProperties.getProperty("rangerHDFSServiceName");
            rangerHIVEServiceName = clusterProperties.getProperty("rangerHIVEServiceName");
        }
        Client rangerClient = getRangerClient(dataset.getSourceCluster());
        ClientResponse clientResp = null;
        String uri = null;
        if (dataset.getType().equals(DataSet.DataSetType.HDFS)) {
            if (!StringUtils.isEmpty(rangerHDFSServiceName)) {
                uri = RANGER_REST_URL_GET_POLICIES + rangerHDFSServiceName + "?resource:path=" + dataset.getDataSet()
                        + "&serviceType=hdfs&policyType=0&resourceMatchScope=self_or_ancestor";
            }
        }
        if (dataset.getType().equals(DataSet.DataSetType.HIVE)) {
            if (!StringUtils.isEmpty(rangerHIVEServiceName)) {
                uri = RANGER_REST_URL_GET_POLICIES + rangerHIVEServiceName + "?resource:database="
                        + dataset.getDataSet() + "&serviceType=hive&policyType=0&resourceMatchScope=self_or_ancestor";
            }
        }
        String url = sourceRangerEndpoint + (uri.startsWith("/") ? uri : ("/" + uri));
        LOG.info(MessageCode.PLUG_000024.name(), url);
        RangerPolicyList rangerPolicies = new RangerPolicyList();
        try {
            WebResource webResource = rangerClient.resource(url);
            clientResp = webResource.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);
            Gson gson = new GsonBuilder().create();
            String response = clientResp.getEntity(String.class);
            rangerPolicies=(RangerPolicyList) gson.fromJson(response, RangerPolicyList.class);
        } catch (Exception ex){
            LOG.error(MessageCode.PLUG_000026.name(), ex);
        }
        if (!CollectionUtils.isEmpty(rangerPolicies.getPolicies())) {
            return rangerPolicies.getPolicies();
        } else {
            return new ArrayList<RangerPolicy>();
        }
    }

    public List<RangerPolicy> addSingleDenyPolicies(DataSet dataset, List<RangerPolicy> rangerPolicies) {
        if (!createDenyPolicy) {
            return rangerPolicies;
        }
        List<RangerPolicy> rangerPoliciesToImport = new ArrayList<RangerPolicy>();
        RangerPolicy denyRangerPolicy = new RangerPolicy();
        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            for (RangerPolicy rangerPolicy : rangerPolicies) {
                rangerPoliciesToImport.add(rangerPolicy);
                denyRangerPolicy.setService(rangerPolicy.getService());
            }
            denyRangerPolicy.setName("Deny policy of " + dataset.getDataSet());
            denyRangerPolicy.setDescription("Deny policy of dataset " + dataset.getDataSet());
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
                resourceNameList.add(dataset.getDataSet());
                resourceNameList.add("/dummy");
                rangerPolicyResource.setValues(resourceNameList);
                rangerPolicyResourceMap.put("path", rangerPolicyResource);
                denyRangerPolicy.setResources(rangerPolicyResourceMap);

                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("write", true));
                denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess("execute", true));
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
                resourceNameList.add(dataset.getDataSet());
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
            RangerExportPolicyList rangerExportPolicyList) {
        String targetRangerEndpoint = dataset.getTargetCluster().getRangerEndpoint();
        Properties sourceClusterProperties = dataset.getSourceCluster().getCustomProperties();
        Properties targetClusterProperties = dataset.getTargetCluster().getCustomProperties();
        String sourceClusterServiceName = null;
        String targetClusterServiceName = null;
        String serviceMapJsonFileName = "servicemap.json";
        String rangerPoliciesJsonFileName = "replicationPolicies.json";
        String uri = RANGER_REST_URL_IMPORTJSONFILE;
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

        String url = targetRangerEndpoint + (uri.startsWith("/") ? uri : ("/" + uri));
        LOG.info(MessageCode.PLUG_000025.name(), url);
        Client rangerClient = getRangerClient(dataset.getTargetCluster());
        ClientResponse clientResp = null;
        WebResource webResource = rangerClient.resource(url);
        FileDataBodyPart filePartPolicies = new FileDataBodyPart("file", new File(rangerPoliciesJsonFileName));
        FileDataBodyPart filePartServiceMap = new FileDataBodyPart("servicesMapJson",
                new File(serviceMapJsonFileName));
        FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
        MultiPart multipartEntity=null;
        try {
            multipartEntity = formDataMultiPart.bodyPart(filePartPolicies).bodyPart(filePartServiceMap);
            clientResp = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.MULTIPART_FORM_DATA)
                    .post(ClientResponse.class, multipartEntity);
            if (clientResp.getStatus()==204) {
                LOG.info(MessageCode.PLUG_000022.name());
            }else{
                LOG.info(MessageCode.PLUG_000023.name());
                LOG.info(MessageCode.PLUG_000027.name(), clientResp.getStatus());
            }
        } catch (Exception e) {
            LOG.error(MessageCode.PLUG_000030.name(), e);
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
                LOG.error(MessageCode.PLUG_000031.name(), e);
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
            Path path = Paths.get(filePath);
            List<String> fileContents = new ArrayList<String>();
            fileContents.add(jsonString);
            Files.write(path, fileContents, encoding);
        } catch (IOException ex) {
            LOG.error(MessageCode.PLUG_000032.name(), filePath, ex);
        } catch (Exception ex) {
            LOG.error(MessageCode.PLUG_000032.name(), filePath, ex);
        }
        return filePath;
    }

    private synchronized Client getRangerClient(Cluster cluster) {
        Client ret = null;
        String rangerEndpoint = cluster.getRangerEndpoint();
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
                    LOG.info(MessageCode.PLUG_000036.name(), keyStoreFile);
                    if (keyStoreFile != null && keyStoreFilepwd != null) {
                        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                        InputStream in = null;
                        try {
                            in = getFileInputStream(keyStoreFile);
                            if (in == null) {
                                LOG.error(MessageCode.PLUG_000036.name(), keyStoreFile);
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
                    LOG.info(MessageCode.PLUG_000037.name(), trustStoreFile);
                    trustStoreType = KeyStore.getDefaultType();
                    if (trustStoreFile != null && trustStoreFilepwd != null) {
                        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
                        InputStream in = null;
                        try {
                            in = getFileInputStream(trustStoreFile);
                            if (in == null) {
                                LOG.error(MessageCode.PLUG_000033.name(), keyStoreFile);
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
                    LOG.error(MessageCode.PLUG_000035.name(), t);
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
                LOG.info(MessageCode.PLUG_000042.name(), remoteRangerAdmin);
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
                LOG.error(MessageCode.PLUG_000038.name(), e.toString());
            }
            String hostname = BeaconConfig.getInstance().getEngine().getHostName();
            if (StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(principal)
                    && StringUtils.isNotEmpty(hostname)) {
                isKerberos = true;
            }
        }
        return isKerberos;
    }
}
