/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Config;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.CloudCredProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.CloudCredBuilder;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.CredentialProviderHelper;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.hadoop.conf.Configuration;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Beacon cloud cred resource management operations as REST API.
 */
@Path("/api/beacon/cloudcred")
public class CloudCredResource extends AbstractResourceManager {

    @POST
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult submit(@Context HttpServletRequest request) {
        try {
            PropertiesIgnoreCase properties = new PropertiesIgnoreCase();
            properties.load(request.getInputStream());
            String entityId = submitInternal(properties);
            return new APIResult(entityId, APIResult.Status.SUCCEEDED, "Cloud credential entity submitted.");
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @PUT
    @Path("{cloud-cred-id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult update(@PathParam("cloud-cred-id") String cloudCredId,
                            @Context HttpServletRequest request) {
        try {
            PropertiesIgnoreCase properties = new PropertiesIgnoreCase();
            properties.load(request.getInputStream());
            properties.setProperty(CloudCredProperties.ID.getName(), cloudCredId);
            updateInternal(cloudCredId, properties);
            return new APIResult(APIResult.Status.SUCCEEDED, "Cloud credential entity updated.");
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @DELETE
    @Path("{cloud-cred-id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult delete(@PathParam("cloud-cred-id") String cloudCredId) {
        try {
            deleteInternal(cloudCredId);
            return new APIResult(APIResult.Status.SUCCEEDED, "Cloud credential entity deleted.");
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("{cloud-cred-id}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public CloudCred get(@PathParam("cloud-cred-id") String cloudCredId) {
        try {
            return cloudCredDao.cloudCredResults(cloudCredId);
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (Exception e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public CloudCredList list(@DefaultValue("") @QueryParam("filterBy") String filterBy,
                              @DefaultValue("name") @QueryParam("orderBy") String orderBy,
                              @DefaultValue("ASC") @QueryParam("sortOrder") String sortOrder,
                              @DefaultValue("0") @QueryParam("offset") Integer offset,
                              @QueryParam("numResults") Integer resultsPerPage) {
        try {
            return listInternal(filterBy, orderBy, sortOrder, offset, resultsPerPage);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private String submitInternal(PropertiesIgnoreCase properties) throws BeaconException {
        try {
            CloudCred cloudCred = CloudCredBuilder.buildCloudCred(properties);
            validate(cloudCred);
            createCredential(cloudCred);
            RequestContext.get().startTransaction();
            cloudCredDao.submit(cloudCred);
            RequestContext.get().commitTransaction();
            return cloudCred.getId();
        } catch (ValidationException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void deleteInternal(String cloudCredId) {
        try {
            CloudCred cloudCred = cloudCredDao.getCloudCred(cloudCredId);
            deleteCredential(cloudCred);
            RequestContext.get().startTransaction();
            cloudCredDao.delete(cloudCredId);
            RequestContext.get().commitTransaction();
        } catch(Throwable t) {
            throw BeaconWebException.newAPIException(t, Response.Status.BAD_REQUEST);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void updateInternal(String cloudCredId, PropertiesIgnoreCase properties) throws BeaconException {
        try {
            CloudCred oldCloudCred = cloudCredDao.getCloudCred(cloudCredId);
            CloudCred newCloudCred = CloudCredBuilder.buildCloudCred(properties);
            newCloudCred.setProvider(oldCloudCred.getProvider());
            updateCredential(oldCloudCred, newCloudCred);
            Set<Map.Entry<Config, String>> entries = newCloudCred.getConfigs().entrySet();
            for (Map.Entry<Config, String> entry : entries) {
                oldCloudCred.getConfigs().put(entry.getKey(), entry.getValue());
            }
            validate(oldCloudCred);
            RequestContext.get().startTransaction();
            cloudCredDao.update(oldCloudCred);
            RequestContext.get().commitTransaction();
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (ValidationException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private CloudCredList listInternal(String filterBy, String orderBy, String sortOrder,
                                       Integer offset, Integer resultsPerPage) throws BeaconException {
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        resultsPerPage = resultsPerPage <= getMaxResultsPerPage() ? resultsPerPage : getMaxResultsPerPage();
        offset = checkAndSetOffset(offset);
        return cloudCredDao.listCloudCred(filterBy, orderBy, sortOrder, offset, resultsPerPage);
    }

    private void createCredential(CloudCred cloudCred) throws BeaconException {
        CloudCred.Provider provider = cloudCred.getProvider();
        switch (provider) {
            case S3:
                S3CredentialManager credentialManager = new S3CredentialManager(cloudCred.getId());
                credentialManager.create(cloudCred, Config.S3_ACCESS_KEY);
                credentialManager.create(cloudCred, Config.S3_SECRET_KEY);
                break;
            default:
                throw new IllegalArgumentException(
                        StringFormat.format("Invalid provider parameter passed: {}", provider.name()));
        }
    }

    private void deleteCredential(CloudCred cloudCred) throws BeaconException {
        CloudCred.Provider provider = cloudCred.getProvider();
        switch (provider) {
            case S3:
                S3CredentialManager credentialManager = new S3CredentialManager(cloudCred.getId());
                credentialManager.delete(Config.S3_ACCESS_KEY);
                credentialManager.delete(Config.S3_SECRET_KEY);
                break;
            default:
                throw new IllegalArgumentException(
                        StringFormat.format("Invalid provider parameter passed: {}", provider.name()));
        }
    }

    private void updateCredential(CloudCred oldCloudCred, CloudCred newCloudCred) throws BeaconException {
        CloudCred.Provider provider = oldCloudCred.getProvider();
        switch (provider) {
            case S3:
                S3CredentialManager credentialManager = new S3CredentialManager(oldCloudCred.getId());
                credentialManager.update(oldCloudCred, newCloudCred);
                break;
            default:
                throw new IllegalArgumentException(
                        StringFormat.format("Invalid provider parameter passed: {}", provider.name()));
        }
    }

    private class S3CredentialManager {

        private Configuration conf = new Configuration();

        S3CredentialManager(String id) {
            conf = new Configuration();
            String credProviderPath = BeaconConfig.getInstance().getEngine().getCloudCredProviderPath();
            credProviderPath = credProviderPath + id + ".jceks";
            conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PATH, credProviderPath);
        }

        void create(CloudCred cloudCred, Config credentialKey) throws BeaconException {
            String credential = getKey(cloudCred, credentialKey);
            CredentialProviderHelper.createCredentialEntry(conf, credentialKey.getConfigName(), credential);
            updateWithAlias(cloudCred, credentialKey.getConfigName(), credentialKey);
        }

        void delete(Config credentialKey) throws BeaconException {
            CredentialProviderHelper.deleteCredentialEntry(conf, credentialKey.getConfigName());
        }

        void update(CloudCred oldCloudCred, CloudCred newCloudCred) throws BeaconException {
            Map<Config, String> configs = newCloudCred.getConfigs();

            if (configs.containsKey(Config.S3_ACCESS_KEY)) {
                update(oldCloudCred, newCloudCred, Config.S3_ACCESS_KEY);
            }

            if (configs.containsKey(Config.S3_SECRET_KEY)) {
                update(oldCloudCred, newCloudCred, Config.S3_SECRET_KEY);
            }
        }

        private void update(CloudCred oldCloudCred, CloudCred newCloudCred, Config key) throws BeaconException {
            String alias = getKey(oldCloudCred, key);
            String credential = getKey(newCloudCred, key);
            CredentialProviderHelper.updateCredentialEntry(conf, alias, credential);
            updateWithAlias(newCloudCred, alias, key);
        }
    }

    private void updateWithAlias(CloudCred cloudCred, String alias, Config key) {
        cloudCred.getConfigs().put(key, alias);
    }

    private String getKey(CloudCred cloudCred, Config key) {
        return cloudCred.getConfigs().get(key);
    }
}
