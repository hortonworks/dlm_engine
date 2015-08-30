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
package org.apache.oozie.client;

import org.apache.oozie.BundleEngine;
import org.apache.oozie.LocalOozieClient;
import org.apache.oozie.LocalOozieClientCoord;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.service.Services;

import java.io.PrintStream;
import java.util.List;
import java.util.Properties;

/**
 * Oozie Client for Local Oozie.
 */
public class LocalProxyOozieClient extends OozieClient {

    private static LocalOozieClientBundle localOozieClientBundle;
    private static LocalOozieClientCoord localOozieClientCoord;
    private static LocalOozieClient localOozieClient;
    private static final BundleEngine BUNDLE_ENGINE = Services.get().
            get(BundleEngineService.class).getBundleEngine(System.getProperty("user.name"));


    private LocalOozieClientBundle getLocalOozieClientBundle() {
        if (localOozieClientBundle == null) {
            localOozieClientBundle = new LocalOozieClientBundle(BUNDLE_ENGINE);
        }
        return localOozieClientBundle;
    }

    private LocalOozieClient getLocalOozieClient() {
        if (localOozieClient == null) {
            localOozieClient = (LocalOozieClient) LocalOozie.getClient();
        }
        return localOozieClient;
    }

    private LocalOozieClientCoord getLocalOozieClientCoord() {
        if (localOozieClientCoord == null) {
            localOozieClientCoord = (LocalOozieClientCoord) LocalOozie.getCoordClient();
        }
        return localOozieClientCoord;
    }

    @Override
    public BundleJob getBundleJobInfo(String jobId) throws OozieClientException {
        return getLocalOozieClientBundle().getBundleJobInfo(jobId);
    }

    @Override
    public List<BundleJob> getBundleJobsInfo(String filter, int start, int len) throws OozieClientException {
        return getLocalOozieClientBundle().getBundleJobsInfo(filter, start, len);
    }

    public String run(Properties conf) throws OozieClientException {
        return getLocalOozieClientBundle().run(conf);
    }

    @Override
    public Void reRunBundle(final String jobId, final String coordScope, final String dateScope,
                            final boolean refresh, final boolean noCleanup) throws OozieClientException {
        return getLocalOozieClientBundle().reRunBundle(jobId, coordScope, dateScope, refresh, noCleanup);
    }

    @Override
    public String dryrun(Properties conf) {
        return null;
    }

    @Override
    public CoordinatorAction getCoordActionInfo(String actionId) throws OozieClientException {
        return getLocalOozieClientCoord().getCoordActionInfo(actionId);
    }


    @Override
    public CoordinatorJob getCoordJobInfo(final String jobId) throws OozieClientException {
        return getLocalOozieClientCoord().getCoordJobInfo(jobId);
    }

    @Override
    public List<CoordinatorJob> getCoordJobsInfo(final String filter, final int start,
                                                 final int len) throws OozieClientException {
        return getLocalOozieClientCoord().getCoordJobsInfo(filter, start, len);
    }

    @Override
    public CoordinatorJob getCoordJobInfo(final String jobId, final String filter,
                                          final int start, final int len) throws OozieClientException {
        return getLocalOozieClientCoord().getCoordJobInfo(jobId, filter, start, len);
    }

    @Override
    public List<CoordinatorAction> reRunCoord(final String jobId, final String rerunType,
                                              final String scope, final boolean refresh,
                                              final boolean noCleanup) throws OozieClientException {
        return getLocalOozieClientCoord().reRunCoord(jobId, rerunType, scope, refresh, noCleanup);
    }

    @Override
    public List<WorkflowJob> getJobsInfo(final String filter) throws OozieClientException {
        return getLocalOozieClientCoord().getJobsInfo(filter);
    }

    @Override
    public List<WorkflowJob> getJobsInfo(final String filter, final int start,
                                         final int len) throws OozieClientException {
        return getLocalOozieClientCoord().getJobsInfo(filter, start, len);
    }

    @Override
    public WorkflowJob getJobInfo(final String jobId) throws OozieClientException {
        return getLocalOozieClient().getJobInfo(jobId);
    }


    @Override
    public WorkflowAction getWorkflowActionInfo(final String actionId) throws OozieClientException {
        return getLocalOozieClient().getWorkflowActionInfo(actionId);
    }

    @Override
    public WorkflowJob getJobInfo(final String jobId, final int start, final int len) throws OozieClientException {
        return getLocalOozieClient().getJobInfo(jobId, start, len);
    }

    @Override
    public String getJobId(final String externalId) throws OozieClientException {
        return getLocalOozieClient().getJobId(externalId);
    }

    @Override
    public void reRun(String jobId, Properties conf) throws OozieClientException {
        throw new IllegalStateException("Rerun not supported ");
    }

    @Override
    public void suspend(String jobId) throws OozieClientException {
        throw new IllegalStateException("Suspend not supported ");
    }

    @Override
    public void resume(String jobId) throws OozieClientException {
        throw new IllegalStateException("Resume not supported ");
    }

    @Override
    public void kill(String jobId) throws OozieClientException {
        throw new IllegalStateException("Kill not supported");
    }

    @Override
    public void change(final String jobId, final String changeValue) throws OozieClientException {
        throw new IllegalStateException("Change not supported");
    }

    @Override
    public void getJobLog(final String jobId, final String logRetrievalType,
                          final String logRetrievalScope, final PrintStream ps) throws OozieClientException {
        throw new IllegalStateException("Job logs not supported");
    }

    @Override
    public String getJobLog(final String jobId) throws OozieClientException {
        throw new IllegalStateException("Job logs not supported");
    }

}
