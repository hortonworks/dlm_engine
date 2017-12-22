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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

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

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        try {
            HttpServletRequest request = (HttpServletRequest) servletRequest;
            MultiReadHttpServletRequest multiReadRequest = new MultiReadHttpServletRequest(request);

            RequestContext.get();
            NDC.push(RequestContext.get().getRequestId());
            String queryString = request.getQueryString();
            String apiPath = request.getPathInfo();
            LOG.info("ThreadId: {}, HTTP method: {}, Query Parameters: {}, APIPath: {}",
                    Thread.currentThread().getName(), request.getMethod(), queryString, apiPath);
            String body = multiReadRequest.getRequestBody();
            if (StringUtils.isNotBlank(body)) {
                LOG.info("Request body: {}", body.replaceAll(System.lineSeparator(), ";"));
            }

            filterChain.doFilter(multiReadRequest, servletResponse);
        } finally {
            // Clear the thread level request context
            RequestContext.get().clear();
            NDC.remove();
            NDC.clear();
        }
    }

    @Override
    public void destroy() {
    }
}
