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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.CloudCredProperties;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.client.util.CloudCredBuilder;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.commons.lang3.StringUtils;
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
import java.util.NoSuchElementException;


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
                           @QueryParam("filePath") String filePath) {
        try {
            if (StringUtils.isBlank(filePath)) {
                throw BeaconWebException.newAPIException("Query parameter [path] is empty.", Status.BAD_REQUEST);
            }
            validatePathInternal(cloudCredId, filePath);
            return new APIResult(APIResult.Status.SUCCEEDED,
                    "Credential [{}] has access to the path: [{}].", cloudCredId, filePath);
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

            BeaconCloudCred beaconCloudCred = new BeaconCloudCred(cloudCred);
            beaconCloudCred.createCredential();
            beaconCloudCred.removeHiddenConfigs();
            LOG.debug("BeaconCloudCred Configs: {}", beaconCloudCred.getConfigs());
            LOG.debug("CloudCred Configs: {}", cloudCred.getConfigs());

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
            checkActivePolicies(cloudCredId);
            RequestContext.get().startTransaction();
            cloudCredDao.delete(cloudCredId);

            BeaconCloudCred beaconCloudCred = new BeaconCloudCred(cloudCred);
            beaconCloudCred.deleteCredential();
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
            validate(newCloudCred);

            BeaconCloudCred beaconCloudCred = new BeaconCloudCred(newCloudCred);
            beaconCloudCred.updateCredential();
            beaconCloudCred.removeHiddenConfigs();

            RequestContext.get().startTransaction();
            cloudCredDao.update(newCloudCred);
            RequestContext.get().commitTransaction();
        } catch (NoSuchElementException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.NOT_FOUND);
        } catch (ValidationException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch(Throwable t) {
            throw BeaconWebException.newAPIException(t, Response.Status.INTERNAL_SERVER_ERROR);
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

    private void validatePathInternal(String cloudCredId, String path) {
        CloudCred cloudCred = cloudCredDao.getCloudCred(cloudCredId);
        ValidationUtil.validateCloudPath(cloudCred, path);
    }
}
