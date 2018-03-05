/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api.filter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.RequestContext;

/**
 * Initializing API context and logging request parameters.
 */
public class APIFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(APIFilter.class);

    private static final Set<String> MASKING_KEYWORDS = new HashSet<String>() {
        {
            add("password");
            add("access");
            add("secret");
            add("encryption");
        }
    };

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        try {
            HttpServletRequest request = (HttpServletRequest) servletRequest;
            MultiReadHttpServletRequest multiReadRequest = new MultiReadHttpServletRequest(request);

            RequestContext requestContext = RequestContext.get();
            NDC.push(requestContext.getRequestId());
            String queryString = request.getQueryString();
            String apiPath = request.getPathInfo();
            LOG.info("ThreadId: {}, HTTP method: {}, Query Parameters: {}, APIPath: {}",
                    Thread.currentThread().getName(), request.getMethod(), queryString, apiPath);
            String body = multiReadRequest.getRequestBody();
            if (StringUtils.isNotBlank(body)) {
                logRequestBody(body);
            }

            filterChain.doFilter(multiReadRequest, servletResponse);
        } catch (ValidationException e) {
            throw new IOException(e);
        } finally {
            // Clear the thread level request context
            RequestContext.get().clear();
            NDC.remove();
            NDC.clear();
        }
    }

    private void logRequestBody(String body) throws ValidationException {
        StringBuilder builder = new StringBuilder();
        for (String line : body.split(System.lineSeparator())) {
            String[] pair = line.split(BeaconConstants.EQUAL_SEPARATOR, 2);
            if (pair.length != 2) {
                throw new ValidationException("Failed to parse [key=value] pair: " + line);
            }
            for (String s : MASKING_KEYWORDS) {
                if (pair[0].toLowerCase().contains(s)) {
                    pair[1] = BeaconConstants.MASK;
                    break;
                }
            }
            builder.append(pair[0])
                    .append(BeaconConstants.EQUAL_SEPARATOR)
                    .append(pair[1])
                    .append(BeaconConstants.SEMICOLON_SEPARATOR);
        }
        LOG.info("Request body: {}", builder.toString());
    }

    @Override
    public void destroy() {
    }
}
