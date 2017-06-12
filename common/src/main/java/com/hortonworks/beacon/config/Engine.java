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

package com.hortonworks.beacon.config;


import com.hortonworks.beacon.log.BeaconLog;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Configuration parameters related to Beacon core engine.
 */
public class Engine {
    private BeaconLog logger = BeaconLog.getLog(Engine.class);

    private static final String BUILD_PROPS = "beacon-buildinfo.properties";
    private static final String DEF_VERSION = "1.0.0.2.6.0.0-SNAPSHOT";

    private String hostName;
    private int tlsPort;
    private int port;
    private String principal;
    private boolean tlsEnabled;
    private String credentialProviderPath;
    private String appPath;
    private String pluginStagingPath;
    private String localClusterName;

    private int loadNumThreads;
    private int loadTimeout;
    private int resultsPerPage;
    private int maxResultsPerPage;
    private int maxInstanceCount;

    private int socketBufferSize;
    private String services;

    private String version;

    public Engine() {
        Class cl = BeaconConfig.class;
        URL resource = cl.getResource("/" + BUILD_PROPS);
        InputStream resourceAsStream = null;
        Properties buildInfo = new Properties();

        if (resource != null) {
            resourceAsStream = cl.getResourceAsStream("/" + BUILD_PROPS);
        } else {
            resource = cl.getResource(BUILD_PROPS);
            if (resource != null) {
                resourceAsStream = cl.getResourceAsStream(BUILD_PROPS);
            }
        }
        if (resourceAsStream != null) {
            try {
                buildInfo.load(resourceAsStream);
            } catch (Exception e) {
                logger.warn("Unable to build property file " + BUILD_PROPS, e);
            } finally {
                try {
                    resourceAsStream.close();
                } catch (IOException e) {
                    // Ignore.
                }
            }
        }
        version = (String) buildInfo.get("build.version");
        if (version == null) {
            version = DEF_VERSION;
        }
        setAppPath("webapp/target/beacon-webapp-" + version);
    }

    public void copy(Engine o) {
        setHostName(o.getHostName());
        setPort(o.getPort());
        setTlsPort(o.getTlsPort());
        setPrincipal(o.getPrincipal());
        setTlsEnabled(o.getTlsEnabled());
        setCredentialProviderPath(o.getCredentialProviderPath());
        setAppPath(o.getAppPath());
        setPluginStagingPath(o.getPluginStagingPath());
        setLoadNumThreads(o.getLoadNumThreads());
        setLoadTimeout(o.getLoadTimeout());
        setResultsPerPage(o.getResultsPerPage());
        setSocketBufferSize(o.getSocketBufferSize());
        setLocalClusterName(o.getLocalClusterName());
        setServices(o.getServices());
        setMaxResultsPerPage(o.getMaxResultsPerPage());
        setMaxInstanceCount(o.getMaxInstanceCount());
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getTlsPort() {
        return tlsPort;
    }

    public void setTlsPort(int tlsPort) {
        if (tlsPort < 0 || tlsPort > 65535) {
            throw new IllegalArgumentException("tlsPort must be between 0 and 65535");
        }
        this.tlsPort = tlsPort;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        this.port = port;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public boolean getTlsEnabled() {
        return tlsEnabled;
    }

    public void setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public String getAppPath() {
        return appPath;
    }

    public void setAppPath(String appPath) {
        this.appPath = appPath;
    }

    public String getPluginStagingPath() {
        return pluginStagingPath;
    }

    public void setPluginStagingPath(String stagingPath) {
        this.pluginStagingPath = stagingPath;
    }

    public int getLoadNumThreads() {
        return loadNumThreads;
    }

    public void setLoadNumThreads(int loadNumThreads) {
        this.loadNumThreads = loadNumThreads;
    }

    public int getLoadTimeout() {
        return loadTimeout;
    }

    public void setLoadTimeout(int loadTimeout) {
        this.loadTimeout = loadTimeout;
    }

    public int getResultsPerPage() {
        return resultsPerPage;
    }

    public void setResultsPerPage(int resultsPerPage) {
        this.resultsPerPage = resultsPerPage;
    }

    public int getMaxResultsPerPage() {
        return maxResultsPerPage;
    }

    public void setMaxResultsPerPage(int maxResultsPerPage) {
        this.maxResultsPerPage = maxResultsPerPage;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }


    public String getLocalClusterName() {
        return localClusterName;
    }

    public void setLocalClusterName(String localClusterName) {
        this.localClusterName = localClusterName;
    }

    public String getServices() {
        return services;
    }

    public void setServices(String services) {
        this.services = services;
    }

    public String getCredentialProviderPath() {
        return credentialProviderPath;
    }

    public void setCredentialProviderPath(String credentialProviderPath) {
        this.credentialProviderPath = credentialProviderPath;
    }

    public int getMaxInstanceCount() {
        return maxInstanceCount;
    }

    public void setMaxInstanceCount(int maxInstanceCount) {
        this.maxInstanceCount = maxInstanceCount;
    }

    public String getVersion() {
        return version;
    }
}
