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
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Config;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.client.CloudCredProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.client.util.CloudCredBuilder;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.CredentialProviderHelper;
import com.hortonworks.beacon.util.FSUtils;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Beacon cloud cred resource management operations as REST API.
 */
@Path("/api/beacon/cloudcred")
public class CloudCredResource extends AbstractResourceManager {

    private static final Logger LOG = LoggerFactory.getLogger(CloudCredResource.class);

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

    @GET
    @Path("{cloud-cred-id}/validate")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public APIResult validatePath(@PathParam("cloud-cred-id") String cloudCredId,
                           @QueryParam("path") String path) {
        try {
            if (StringUtils.isBlank(path)) {
                throw BeaconWebException.newAPIException("Query parameter [path] is empty.", Status.BAD_REQUEST);
            }
            validatePathInternal(cloudCredId, path);
            return new APIResult(APIResult.Status.SUCCEEDED,
                    "Credential [{}] has access to the path: [{}].", cloudCredId, path);
        } catch (BeaconWebException e) {
            throw e;
        } catch (Throwable e) {
            throw BeaconWebException.newAPIException(e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    private String submitInternal(PropertiesIgnoreCase properties) throws BeaconException {
        try {
            CloudCred cloudCred = CloudCredBuilder.buildCloudCred(properties);
            validate(cloudCred);
            createCredential(cloudCred);
            removeHiddenConfigs(cloudCred);
            RequestContext.get().startTransaction();
            cloudCredDao.submit(cloudCred);
            RequestContext.get().commitTransaction();
            setOwnerForCloudCredFile(cloudCred, "hive", "hdfs");
            return cloudCred.getId();
        } catch (ValidationException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void setOwnerForCloudCredFile(CloudCred cloudCred, String userName, String groupName) throws
            BeaconException {
        String credProviderPath = "/user/beacon/credential/" + cloudCred.getId() + BeaconConstants.JCEKS_EXT;
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(credProviderPath);
        try {
            FileSystemClientFactory.get().createProxiedFileSystem(new Configuration()).setOwner(path,
                    userName, groupName);
        } catch (IOException e) {
            throw new BeaconException(e, "Error while setting the owner of cloud credential file: {}",
                    credProviderPath);
        }
    }

    private void deleteInternal(String cloudCredId) {
        try {
            CloudCred cloudCred = cloudCredDao.getCloudCred(cloudCredId);
            checkActivePolicies(cloudCredId);
            RequestContext.get().startTransaction();
            cloudCredDao.delete(cloudCredId);
            deleteCredential(cloudCred);
            String credProviderPath = BeaconConfig.getInstance().getEngine().getCloudCredProviderPath();
            credProviderPath = credProviderPath + cloudCredId + BeaconConstants.JCEKS_EXT;
            String[] credPath = credProviderPath.split(BeaconConstants.JCEKS_HDFS_FILE_REGEX);
            Configuration configuration = new Configuration();
            FileSystem fileSystem = FSUtils.getFileSystem(configuration.get(BeaconConstants.FS_DEFAULT_NAME_KEY),
                    configuration, false);
            fileSystem.delete(new org.apache.hadoop.fs.Path(credPath[1]), false);
            RequestContext.get().commitTransaction();
        } catch (ValidationException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch(Throwable t) {
            throw BeaconWebException.newAPIException(t, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            RequestContext.get().rollbackTransaction();
        }
    }

    private void updateInternal(String cloudCredId, PropertiesIgnoreCase properties) throws BeaconException {
        try {
            CloudCred oldCloudCred = cloudCredDao.getCloudCred(cloudCredId);
            CloudCred newCloudCred = CloudCredBuilder.buildCloudCred(properties);
            newCloudCred.setProvider(oldCloudCred.getProvider());
            updateCredential(newCloudCred);
            Set<Map.Entry<Config, String>> entries = newCloudCred.getConfigs().entrySet();
            for (Map.Entry<Config, String> entry : entries) {
                oldCloudCred.getConfigs().put(entry.getKey(), entry.getValue());
            }
            validate(oldCloudCred);
            removeHiddenConfigs(oldCloudCred);
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

    private void validatePathInternal(String cloudCredId, String path) throws BeaconException {
        ValidationUtil.validateCloudPath(cloudCredId, path);
    }

    private void createCredential(CloudCred cloudCred) throws BeaconException {
        CloudCred.Provider provider = cloudCred.getProvider();
        switch (provider) {
            case AWS:
                S3CredentialManager credentialManager = new S3CredentialManager(cloudCred.getId());
                credentialManager.create(cloudCred, Config.AWS_ACCESS_KEY);
                credentialManager.create(cloudCred, Config.AWS_SECRET_KEY);
                break;
            default:
                throw new IllegalArgumentException(
                        StringFormat.format("Invalid provider parameter passed: {}", provider.name()));
        }
    }

    private void deleteCredential(CloudCred cloudCred) throws BeaconException {
        CloudCred.Provider provider = cloudCred.getProvider();
        switch (provider) {
            case AWS:
                S3CredentialManager credentialManager = new S3CredentialManager(cloudCred.getId());
                credentialManager.delete(Config.AWS_ACCESS_KEY);
                credentialManager.delete(Config.AWS_SECRET_KEY);
                break;
            default:
                throw new IllegalArgumentException(
                        StringFormat.format("Invalid provider parameter passed: {}", provider.name()));
        }
    }

    private void updateCredential(CloudCred newCloudCred) throws BeaconException {
        CloudCred.Provider provider = newCloudCred.getProvider();
        switch (provider) {
            case AWS:
                S3CredentialManager credentialManager = new S3CredentialManager(newCloudCred.getId());
                credentialManager.update(newCloudCred);
                break;
            default:
                throw new IllegalArgumentException(
                        StringFormat.format("Invalid provider parameter passed: {}", provider.name()));
        }
    }

    private class S3CredentialManager {

        private Configuration conf = new Configuration();

        S3CredentialManager(String id) {
            conf = cloudConf(id);
        }

        void create(CloudCred cloudCred, Config credentialKey) throws BeaconException {
            String credential = getKey(cloudCred, credentialKey);
            CredentialProviderHelper.createCredentialEntry(conf, credentialKey.getConfigName(), credential);
            updateWithAlias(cloudCred, credentialKey.getConfigName(), credentialKey);
        }

        void delete(Config credentialKey) throws BeaconException {
            CredentialProviderHelper.deleteCredentialEntry(conf, credentialKey.getConfigName());
        }

        void update(CloudCred newCloudCred) throws BeaconException {
            Map<Config, String> configs = newCloudCred.getConfigs();

            if (configs.containsKey(Config.AWS_ACCESS_KEY)) {
                update(newCloudCred, Config.AWS_ACCESS_KEY);
            }

            if (configs.containsKey(Config.AWS_SECRET_KEY)) {
                update(newCloudCred, Config.AWS_SECRET_KEY);
            }
        }

        private void update(CloudCred newCloudCred, Config key) throws BeaconException {
            String alias = key.getConfigName();
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

    private void removeHiddenConfigs(CloudCred cloudCred) {
        Map<Config, String> configs = cloudCred.getConfigs();
        for (Config config : Config.values()) {
            if (config.isHidden() && configs.containsKey(config)) {
                configs.remove(config);
            }
        }
    }
}
